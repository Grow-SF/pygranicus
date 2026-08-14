"""A local stand-in for Granicus's CloudFront-fronted video host.

Every test in this suite runs against this server rather than the network:
the real host is slow, rate-limited, and serves multi-gigabyte files.
"""
import http.server
import os
import selectors
import threading
import time

import pytest

ERROR_PAGE = b"<HTML><H1>403 ERROR</H1>Request blocked.</HTML>"


class WakeableHTTPServer(http.server.ThreadingHTTPServer):
    """A server that stops the moment it is asked, rather than polling to ask.

    socketserver cannot simply block on the listening socket, because its
    shutdown() only sets a flag: an idle server would never wake to read it.
    It settles for waking on a timer instead, which costs a wait on every
    shutdown and a wakeup many times a second the rest of the time -- the
    standard library says as much in a comment above the loop.

    Watching a pipe alongside the socket lets shutdown() speak up directly, so
    the wait can block indefinitely and still end at once.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._wake_reader, self._wake_writer = os.pipe()
        self._closed = False
        self._stopped = threading.Event()
        # Set until serving begins, so a shutdown() that arrives first is not
        # left waiting on a loop that never ran.
        self._stopped.set()

    def serve_forever(self, poll_interval=None):
        """Serve until shutdown(). poll_interval is accepted and ignored."""
        self._stopped.clear()
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(self, selectors.EVENT_READ)
                selector.register(self._wake_reader, selectors.EVENT_READ)
                while True:
                    for key, _ in selector.select():
                        if key.fileobj is not self:
                            return
                        # The same private call socketserver's own loop makes.
                        self._handle_request_noblock()
        finally:
            self._stopped.set()

    def shutdown(self):
        # Stopping an already-closed server is not an error: the pipe is gone,
        # and so is the loop that was listening to it.
        if self._closed:
            return
        os.write(self._wake_writer, b"\0")
        self._stopped.wait()

    def server_close(self):
        super().server_close()
        if not self._closed:
            self._closed = True
            os.close(self._wake_reader)
            os.close(self._wake_writer)


class FakeGranicus:
    """Serves bytes over HTTP with Range support, and records every request.

    Args:
        payload: the bytes to serve.
        blocked_ua_prefixes: User-Agent prefixes to reject with a 403, the way
            CloudFront rejects `python-requests/x.y.z`.
        blocked_methods: which HTTP methods the block applies to.
        delay_ranges: {start_offset: seconds} — sleep before serving the chunk
            beginning at that offset, to force out-of-order completion.
    """

    def __init__(self, payload, blocked_ua_prefixes=(),
                 blocked_methods=("HEAD", "GET"), delay_ranges=None,
                 segment_bodies=None, delay_paths=None):
        self.payload = payload
        self.blocked_ua_prefixes = blocked_ua_prefixes
        self.blocked_methods = blocked_methods
        self.delay_ranges = delay_ranges or {}
        # Paths served whole rather than as ranges of `payload`,
        # for the stream segment tests, plus per-path delays to
        # force out-of-order completion.
        self.segment_bodies = dict(segment_bodies or {})
        self.delay_paths = dict(delay_paths or {})
        self.requests = []
        self._lock = threading.Lock()
        self._server = WakeableHTTPServer(
            ("127.0.0.1", 0), self._make_handler())
        self._thread = threading.Thread(
            target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def origin(self):
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    @property
    def url(self):
        return f"http://127.0.0.1:{self._server.server_address[1]}/video.mp4"

    def shutdown(self):
        self._server.shutdown()
        self._server.server_close()

    def _record(self, method, headers):
        with self._lock:
            self.requests.append((method, dict(headers)))

    def user_agents(self, method):
        """Every User-Agent seen for the given method, in request order."""
        return [h.get("User-Agent") for m, h in self.requests if m == method]

    @property
    def ranges(self):
        """(start, end) for every ranged GET, in request order."""
        found = []
        for method, headers in self.requests:
            if method == "GET" and "Range" in headers:
                start, end = headers["Range"].replace("bytes=", "").split("-")
                found.append((int(start), int(end)))
        return found

    def _make_handler(self):
        server = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _is_blocked(self, method):
                if method not in server.blocked_methods:
                    return False
                ua = self.headers.get("User-Agent", "")
                return any(ua.startswith(p)
                           for p in server.blocked_ua_prefixes)

            def _send_403(self):
                self.send_response(403)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(ERROR_PAGE)))
                self.end_headers()

            def do_HEAD(self):
                server._record("HEAD", self.headers)
                if self._is_blocked("HEAD"):
                    self._send_403()
                    return
                self.send_response(200)
                self.send_header("Content-Type", "video/mp4")
                self.send_header("Content-Length", str(len(server.payload)))
                self.send_header("Accept-Ranges", "bytes")
                self.end_headers()

            def do_GET(self):
                server._record("GET", self.headers)
                path = self.path.split("?")[0]
                if path in server.segment_bodies:
                    delay = server.delay_paths.get(path, 0)
                    if delay:
                        time.sleep(delay)
                    body = server.segment_bodies[path]
                    self.send_response(200)
                    self.send_header("Content-Type", "video/mp2t")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                if self._is_blocked("GET"):
                    self._send_403()
                    self.wfile.write(ERROR_PAGE)
                    return
                header_range = self.headers.get("Range")
                if header_range:
                    start, end = header_range.replace(
                        "bytes=", "").split("-")
                    start, end = int(start), int(end)
                    delay = server.delay_ranges.get(start, 0)
                    if delay:
                        time.sleep(delay)
                    body = server.payload[start:end + 1]
                    self.send_response(206)
                    self.send_header(
                        "Content-Range",
                        f"bytes {start}-{end}/{len(server.payload)}")
                else:
                    body = server.payload
                    self.send_response(200)
                self.send_header("Content-Type", "video/mp4")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        return Handler


@pytest.fixture
def granicus():
    """Factory for FakeGranicus servers, each shut down when the test ends."""
    servers = []

    def make(payload, **kwargs):
        server = FakeGranicus(payload, **kwargs)
        servers.append(server)
        return server

    yield make
    for server in servers:
        server.shutdown()


@pytest.fixture
def payload():
    """Factory for deterministic pseudo-video bytes of a given size."""
    def make(size):
        return bytes((i * 7 + 11) % 256 for i in range(size))
    return make
