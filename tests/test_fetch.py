import io
import sys

import pytest
import requests

from pygranicus import fetch

from conftest import ERROR_PAGE

# Small enough to produce several chunks from a small payload, so tests stay
# fast; the production default of 32 MiB is exercised only by the size math.
CHUNK = 8192
REQUESTS_DEFAULT_UA = "python-requests"
BLOCKED_UA_PREFIX = fetch.USER_AGENT.split("/")[0]


def test_head_request_sends_the_configured_user_agent(
        tmp_path, granicus, payload):
    server = granicus(payload(50_000))

    fetch.download_video(server.url, CHUNK, 4, str(tmp_path / "out.mp4"))

    assert server.user_agents("HEAD") == [fetch.USER_AGENT]


def test_every_chunk_request_sends_the_configured_user_agent(
        tmp_path, granicus, payload):
    server = granicus(payload(50_000))

    fetch.download_video(server.url, CHUNK, 4, str(tmp_path / "out.mp4"))

    chunk_agents = server.user_agents("GET")
    assert chunk_agents, "no chunk requests were made"
    assert set(chunk_agents) == {fetch.USER_AGENT}


def test_user_agent_is_not_the_requests_default():
    # CloudFront 403s the `python-requests/x.y.z` default, which is the whole
    # reason USER_AGENT exists.
    assert not fetch.USER_AGENT.startswith(REQUESTS_DEFAULT_UA)


def test_rejected_head_raises_http_error_not_index_error(
        tmp_path, granicus, payload):
    # A 403 error page carries its own Content-Length. Read as a video size it
    # yields zero chunks and an IndexError from the chunk math, which is what
    # this download used to fail with.
    server = granicus(payload(50_000), blocked_ua_prefixes=(BLOCKED_UA_PREFIX,),
                      blocked_methods=("HEAD",))

    with pytest.raises(requests.HTTPError):
        fetch.download_video(server.url, CHUNK, 4, str(tmp_path / "out.mp4"))


def test_rejected_chunk_never_writes_the_error_page_to_the_output(
        tmp_path, granicus, payload):
    # HEAD succeeds, so the size is right, but every chunk is rejected.
    server = granicus(payload(50_000), blocked_ua_prefixes=(BLOCKED_UA_PREFIX,),
                      blocked_methods=("GET",))
    out = tmp_path / "out.mp4"

    with pytest.raises(requests.HTTPError):
        fetch.download_video(server.url, CHUNK, 4, str(out))

    written = out.read_bytes() if out.exists() else b""
    assert ERROR_PAGE not in written


def test_downloads_file_contents_exactly(tmp_path, granicus, payload):
    data = payload(50_000)
    server = granicus(data)
    out = tmp_path / "out.mp4"

    fetch.download_video(server.url, CHUNK, 4, str(out))

    assert out.read_bytes() == data


def test_reassembles_chunks_in_order_when_completion_is_out_of_order(
        tmp_path, granicus, payload):
    # The first chunk finishes last. The linked list of futures exists exactly
    # so the output order still follows the file, not the completion order.
    data = payload(50_000)
    server = granicus(data, delay_ranges={0: 0.5})
    out = tmp_path / "out.mp4"

    fetch.download_video(server.url, CHUNK, 4, str(out))

    assert out.read_bytes() == data


def test_requested_ranges_tile_the_file_exactly(
        tmp_path, granicus, payload):
    size = 50_000
    server = granicus(payload(size))

    fetch.download_video(server.url, CHUNK, 4, str(tmp_path / "out.mp4"))

    ordered = sorted(server.ranges)
    assert ordered[0][0] == 0
    assert ordered[-1][1] == size - 1
    for (_, previous_end), (next_start, _) in zip(ordered, ordered[1:]):
        assert next_start == previous_end + 1


def test_handles_size_that_is_not_a_multiple_of_chunk_size(
        tmp_path, granicus, payload):
    data = payload(CHUNK * 3 + 123)
    server = granicus(data)
    out = tmp_path / "out.mp4"

    fetch.download_video(server.url, CHUNK, 4, str(out))

    assert out.read_bytes() == data


def test_output_filename_defaults_to_the_url_basename(
        tmp_path, granicus, payload, monkeypatch):
    data = payload(50_000)
    server = granicus(data)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys, "argv", ["pygranicus", server.url, "-c", str(CHUNK)])

    fetch.main()

    assert (tmp_path / "video.mp4").read_bytes() == data


def test_output_file_flag_overrides_the_basename(
        tmp_path, granicus, payload, monkeypatch):
    data = payload(50_000)
    server = granicus(data)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys, "argv",
        ["pygranicus", server.url, "-c", str(CHUNK), "-o", "custom.mp4"])

    fetch.main()

    assert (tmp_path / "custom.mp4").read_bytes() == data
    assert not (tmp_path / "video.mp4").exists()


def test_downloads_file_smaller_than_one_chunk(tmp_path, granicus, payload):
    # range(file_size // chunk_size) is range(0) for a file below one chunk,
    # leaving no chunks for the final-chunk fixup to extend.
    data = payload(5_000)
    server = granicus(data)
    out = tmp_path / "out.mp4"

    fetch.download_video(server.url, CHUNK * 10, 4, str(out))

    assert out.read_bytes() == data


class FakeTTY(io.StringIO):
    """A StringIO that claims to be a terminal, so tqdm will render to it."""

    def isatty(self):
        return True


def test_progress_bar_renders_to_a_terminal_sink(
        tmp_path, granicus, payload):
    server = granicus(payload(50_000))
    sink = FakeTTY()

    fetch.download_video(server.url, CHUNK, 4, str(tmp_path / "out.mp4"),
                         progress_sink=sink)

    assert "100%" in sink.getvalue()


def test_contents_are_byte_exact_with_progress_enabled(
        tmp_path, granicus, payload):
    # Guards the switch from response.content to iter_content.
    data = payload(50_000)
    server = granicus(data)
    out = tmp_path / "out.mp4"

    fetch.download_video(server.url, CHUNK, 4, str(out),
                         progress_sink=FakeTTY())

    assert out.read_bytes() == data


def test_verbose_lines_still_appear_while_a_bar_is_active(
        tmp_path, granicus, payload):
    # Verbose output must survive being routed through _log rather than
    # print() when a bar exists.
    server = granicus(payload(50_000))
    sink = FakeTTY()

    fetch.download_video(server.url, CHUNK, 4, str(tmp_path / "out.mp4"),
                         verbose=True, progress_sink=sink)

    assert "Downloading chunk" in sink.getvalue()


def test_cli_shows_the_bar_by_default_on_a_terminal(
        tmp_path, granicus, payload, monkeypatch):
    server = granicus(payload(50_000))
    stderr = FakeTTY()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "stderr", stderr)
    monkeypatch.setattr(
        sys, "argv", ["pygranicus", server.url, "-c", str(CHUNK)])

    fetch.main()

    assert "100%" in stderr.getvalue()


def test_cli_no_progress_flag_disables_the_bar(
        tmp_path, granicus, payload, monkeypatch):
    server = granicus(payload(50_000))
    stderr = FakeTTY()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "stderr", stderr)
    monkeypatch.setattr(
        sys, "argv",
        ["pygranicus", server.url, "-c", str(CHUNK), "--no-progress"])

    fetch.main()

    assert stderr.getvalue() == ""
