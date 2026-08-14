import argparse
import requests
import datetime
import html
import re
import time
import os
import sys
import urllib.parse
import concurrent.futures
import threading

from tqdm import tqdm


CHUNK_SIZE = 32 * 1024 * 1024  # 8 MiB
OUTPUT_FILE = None
NUM_THREADS = os.cpu_count()

# Granicus fronts its videos with CloudFront, which blocks the default
# `python-requests/x.y.z` User-Agent with a 403. Identify as a browser.
USER_AGENT = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
              'AppleWebKit/537.36 (KHTML, like Gecko) '
              'Chrome/120.0.0.0 Safari/537.36')


# Bytes pulled from the socket per read. Small enough that the bar moves
# smoothly, large enough not to add measurable overhead.
PROGRESS_BLOCK_SIZE = 64 * 1024

# Granicus player pages link the downloadable file directly. There is exactly
# one per page, alongside an .mp3 of the same recording that we ignore.
VIDEO_URL_RE = re.compile(
    r'https://archive-video\.granicus\.com/[^\s"\'<>]+\.mp4')
# Both the modern /player/clip/<id> and the legacy MediaPlayer.php?clip_id=<id>
# forms are in circulation.
CLIP_ID_RE = re.compile(r'/clip/(\d+)|[?&]clip_id=(\d+)')
TITLE_RE = re.compile(r'<title>(.*?)</title>', re.IGNORECASE | re.DOTALL)
# A clip's meeting date is not on the player page. It lives on the listing
# page for the body that met, which is keyed by view_id -- so the date can
# only be found when the pasted URL carries one.
VIEW_ID_RE = re.compile(r'[?&]view_id=(\d+)')
EPOCH_RE = re.compile(r'\b(1[0-9]{9})\b')
# How far back from a clip's link to look for its row's timestamp.
LISTING_ROW_SPAN = 1500
LISTING_TIMEOUT = 30
UNSAFE_FILENAME_RE = re.compile(r'[/\\:*?"<>|\x00-\x1f]')
# Leaves room for the clip id and extension inside the usual 255-byte limit.
MAX_TITLE_LENGTH = 150


def _safe_filename(text):
    """Turn a page title into something a filesystem will accept."""
    text = UNSAFE_FILENAME_RE.sub('', html.unescape(text))
    return ' '.join(text.split())[:MAX_TITLE_LENGTH].strip()


def _recording_date(page_url, view_id, clip_id):
    """Return the clip's meeting date as YYYY-MM-DD, or None.

    The date is a nicety, so every failure here returns None rather than
    raising: not knowing it must never stop a download.
    """
    parts = urllib.parse.urlparse(page_url)
    listing = (f'{parts.scheme}://{parts.netloc}'
               f'/ViewPublisher.php?view_id={view_id}')
    try:
        response = requests.get(listing, headers={"User-Agent": USER_AGENT},
                                timeout=LISTING_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException:
        return None

    page = response.text
    for match in re.finditer(rf'clip_id={clip_id}\b', page):
        row = page[max(0, match.start() - LISTING_ROW_SPAN):match.start()]
        stamps = EPOCH_RE.findall(row)
        if stamps:
            # Granicus records the meeting at local midnight, which lands on
            # the same calendar day in UTC for every US timezone.
            moment = datetime.datetime.fromtimestamp(
                int(stamps[-1]), datetime.timezone.utc)
            return moment.strftime('%Y-%m-%d')
    return None


def resolve_video_url(url):
    """Resolve a Granicus player URL to (video url, default filename).

    A URL that already points at a media file is returned untouched, so
    pasting a direct .mp4 link costs no extra request.
    """
    if urllib.parse.urlparse(url).path.endswith('.mp4'):
        return url, os.path.basename(urllib.parse.urlparse(url).path)

    response = requests.get(url, headers={"User-Agent": USER_AGENT})
    # A clip id that does not exist is a clean 404, which says so far better
    # than anything we could infer from the page body.
    response.raise_for_status()
    page = response.text

    match = VIDEO_URL_RE.search(page)
    if match is None:
        raise ValueError(f"no downloadable video found at {url}")
    video_url = match.group(0)

    title_match = TITLE_RE.search(page)
    title = _safe_filename(title_match.group(1)) if title_match else ''
    clip_match = CLIP_ID_RE.search(url)
    clip_id = next((g for g in clip_match.groups() if g), None) \
        if clip_match else None

    # Prefer the meeting date; fall back to the clip id, which is always
    # available from the URL even when the date is not.
    suffix = None
    view_match = VIEW_ID_RE.search(url)
    if view_match and clip_id:
        suffix = _recording_date(url, view_match.group(1), clip_id)
    suffix = suffix or clip_id

    if title and suffix:
        return video_url, f'{title}-{suffix}.mp4'
    # Nothing better to go on; fall back to the name the file already has.
    return video_url, os.path.basename(
        urllib.parse.urlparse(video_url).path)


def _log(message, progress=None):
    """Print a line without corrupting an active progress bar."""
    if progress is None:
        print(message)
    else:
        tqdm.write(message, file=progress.fp)


class Node:
    def __init__(self, chunk_id, data=None, next=None):
        self.data = data
        self.chunk_id = chunk_id
        self.next = next


def download_chunk(url, start, end, i, num_chunks, verbose, progress=None,
                   cancelled=None):
    """Download a chunk of the video.

    Note that i and num_chunks are only needed for verbose output
    """
    headers = {"Range": f"bytes={start}-{end}", "User-Agent": USER_AGENT}
    response = requests.get(url, headers=headers, stream=True)
    # Without this a rejected chunk writes the error page into the video file
    response.raise_for_status()
    if verbose:
        _log(f'Downloading chunk {i} of {num_chunks}', progress)
        start_time = time.time()
    blocks = []
    for block in response.iter_content(chunk_size=PROGRESS_BLOCK_SIZE):
        if cancelled is not None and cancelled.is_set():
            # Raise rather than return: these bytes are a partial chunk and
            # must never reach the output file.
            raise concurrent.futures.CancelledError()
        blocks.append(block)
        if progress is not None:
            progress.update(len(block))
    content = b"".join(blocks)
    if verbose:
        end_time = time.time()
        download_speed = len(content) / (end_time - start_time)
        _log(
            f'chunk {i} download speed: {download_speed / (1024 * 1024):.2f} MiB/s',
            progress)
    return content


def download_video(url, chunk_size, num_threads, output_file, verbose=False,
                   progress_sink=None):
    """
    Downloads the video by creating a linked list of concurrent.futures jobs and writes to output_file.
    """
    head = None
    current = None
    progress = None
    cancelled = threading.Event()
    try:
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=num_threads) as executor:
            response = requests.head(url, headers={"User-Agent": USER_AGENT})
            # A rejected HEAD still carries a Content-Length (of the error
            # page), which would otherwise be mistaken for the size of the
            # video.
            response.raise_for_status()
            file_size = int(response.headers["Content-Length"])
            # At least one chunk, so a file smaller than chunk_size still gets
            # downloaded rather than leaving `chunks` empty. The final chunk is
            # then stretched to the end of the file.
            num_whole_chunks = max(1, file_size // chunk_size)
            chunks = [(i * chunk_size, (i + 1) * chunk_size - 1)
                      for i in range(num_whole_chunks)]
            chunks[-1] = (chunks[-1][0], file_size - 1)

            # disable=None makes tqdm silence itself when the sink is not a
            # terminal, which covers pipes, redirects and CI logs.
            progress = (tqdm(total=file_size, unit="B", unit_scale=True,
                             unit_divisor=1024, file=progress_sink,
                             disable=None,
                             desc=os.path.basename(output_file))
                        if progress_sink is not None else None)

            i = 0
            num_chunks = len(chunks) + 1
            for (start, end) in chunks:
                i += 1
                if head is None:
                    head = Node(i)
                    current = head
                else:
                    current.next = Node(i)
                    current = current.next
                current.data = executor.submit(
                    download_chunk, url, start, end, i, num_chunks, verbose,
                    progress, cancelled)

            # Use `head` instead of `current` so we can free up memory as we write to file
            try:
                with open(output_file, "wb") as f:
                    while head is not None:
                        result = head.data.result()
                        if verbose:
                            _log(
                                f'Writing chunk {head.chunk_id} of {num_chunks}',
                                progress)
                        f.write(result)
                        head = head.next
            except BaseException:
                # Drop the chunks still queued, and tell the ones already
                # streaming to stop reading. Without both, the interrupt
                # downloads the rest of the file before it exits.
                cancelled.set()
                executor.shutdown(wait=False, cancel_futures=True)
                raise
    finally:
        # Closing after the executor's context manager has exited means the
        # pool has drained, so nothing renders after the bar's final line.
        if progress is not None:
            progress.close()


def main():
    parser = argparse.ArgumentParser(
        description='Download video file from url in parallel')
    parser.add_argument(
        'url', type=str, help='URL of the video file to download')
    parser.add_argument('-c', '--chunk_size', type=int, default=CHUNK_SIZE,
                        help='Size of each chunk to download, in bytes')
    parser.add_argument('-o', '--output_file', type=str, default=OUTPUT_FILE,
                        help='Output filename. Defaults to the last part of the url path')
    parser.add_argument('-t', '--num_threads', type=int, default=NUM_THREADS,
                        help='Number of threads to use for downloading')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Print the current chunk number, total number of chunks and download speed')
    parser.add_argument('--no-progress', action='store_true',
                        help='Do not display the progress bar')
    args = parser.parse_args()

    chunk_size = args.chunk_size
    url, default_output_file = resolve_video_url(args.url)
    output_file = args.output_file or default_output_file
    num_threads = args.num_threads
    verbose = args.verbose
    try:
        download_video(url, chunk_size, num_threads, output_file, verbose,
                       progress_sink=None if args.no_progress else sys.stderr)
    except KeyboardInterrupt:
        # 130 is the conventional exit code for SIGINT. The partial file is
        # left alone: it is the user's data, not ours to delete.
        print(f"\nInterrupted. Partial download left at {output_file}",
              file=sys.stderr)
        raise SystemExit(130)


if __name__ == '__main__':
    main()
