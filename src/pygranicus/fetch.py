import argparse
import requests
import html
import re
import shutil
import subprocess
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
# Listing rows print the meeting date as mm/dd/yy.
LISTING_DATE_RE = re.compile(r'\b(\d{2})/(\d{2})/(\d{2})\b')
# How far back from a clip's link to look for its row's timestamp.
LISTING_ROW_SPAN = 1500
LISTING_TIMEOUT = 30

# The stream is published alongside every archived video, cut into segments of
# a couple of seconds each. That granularity is what makes a time range cheap:
# only the segments covering it are fetched.
STREAM_HOST = 'https://archive-stream.granicus.com'
SEGMENT_DURATION_RE = re.compile(r'#EXTINF:([\d.]+)')
PLAYLIST_TIMEOUT = 60

# A download is thousands of small requests. Without a shared session each one
# opens a connection and resolves the host again, which is enough to make a
# resolver fail intermittently; and without retries one such blip ends the
# download. Both are configured once here.
RETRY_ATTEMPTS = 5
RETRY_BACKOFF = 0.5
RETRY_ON_STATUS = (429, 500, 502, 503, 504)
CONNECTION_POOL = 32

# Player pages mark each agenda item with an "index point" carrying the second
# it starts at. They are the chapters offered by --chapters.
INDEX_POINT_RE = re.compile(r'<div([^>]*index-point[^>]*)>(.*?)</div>', re.S)
INDEX_TIME_RE = re.compile(r'time="(\d+)"')
TAG_RE = re.compile(r'<[^>]+>')
# Leaves room for the meeting name a chapter file hangs off.
MAX_CHAPTER_TITLE = 120

# Room a progress bar needs for the bar itself, the rate, and the timings.
# Whatever is left over is what the description may use.
PROGRESS_BAR_RESERVE = 46
MIN_DESCRIPTION = 20
# Furniture questionary puts before each row: pointer, checkbox, spacing.
CHOICE_FURNITURE = 6
UNSAFE_FILENAME_RE = re.compile(r'[/\\:*?"<>|\x00-\x1f]')
# Leaves room for the clip id and extension inside the usual 255-byte limit.
MAX_TITLE_LENGTH = 150


def _safe_filename(text):
    """Turn a page title into something a filesystem will accept."""
    text = UNSAFE_FILENAME_RE.sub('', html.unescape(text))
    return ' '.join(text.split())[:MAX_TITLE_LENGTH].strip()


def is_media_url(url):
    """True when the URL already points at the file, not at a page about it."""
    return urllib.parse.urlparse(url).path.endswith('.mp4')


def fetch_page(url):
    """Read a page once, so its callers can share the one copy."""
    response = SESSION.get(url, headers={"User-Agent": USER_AGENT},
                           timeout=LISTING_TIMEOUT)
    response.raise_for_status()
    return response.text


def fetch_playlist(video_url):
    """Read the stream's segment playlist."""
    response = SESSION.get(chunklist_url(video_url),
                           headers={"User-Agent": USER_AGENT},
                           timeout=PLAYLIST_TIMEOUT)
    response.raise_for_status()
    return response.text


def terminal_width(fallback=80):
    return shutil.get_terminal_size(fallback=(fallback, 24)).columns


def fit(text, width):
    """Shorten text to width, showing that it was cut."""
    if width <= 0:
        return ''
    if len(text) <= width:
        return text
    if width <= 3:
        return text[:width]
    return text[:width - 3] + '...'


def progress_description(label, width=None):
    """Trim a label so the progress bar it labels still has room to draw.

    A long filename otherwise fills the line and the bar disappears
    altogether.
    """
    width = width if width is not None else terminal_width()
    return fit(label, max(MIN_DESCRIPTION, width - PROGRESS_BAR_RESERVE))


def chapter_choice_label(row, width=None):
    """One line describing a chapter, sized to fit on one line.

    Rows that wrap cost the picker several lines each, which is how it came
    to take over the whole window.
    """
    width = width if width is not None else terminal_width()
    start, seconds, size, title = row
    estimate = f'~{tqdm.format_sizeof(size, "B", 1024)}' if size else '?'
    prefix = (f'{range_suffix(start, None)[:-4]}  '
              f'{format_span(seconds):>8}  {estimate:>9}  ')
    return prefix + fit(title, width - len(prefix) - CHOICE_FURNITURE)


def parse_chapters(page):
    """Return [(start second, title)] for the agenda items a page lists."""
    chapters = []
    for attributes, body in INDEX_POINT_RE.findall(page):
        moment = INDEX_TIME_RE.search(attributes)
        if moment is None:
            continue
        title = html.unescape(TAG_RE.sub(' ', body))
        title = ' '.join(title.split())
        if title:
            chapters.append((int(moment.group(1)), title))
    return sorted(chapters)


def format_span(seconds):
    """A compact duration: 1m35s, 1h02m05s, or ? when it is not known."""
    if seconds is None:
        return '?'
    hours, rest = divmod(int(seconds), 3600)
    minutes, rest = divmod(rest, 60)
    if hours:
        return f'{hours}h{minutes:02d}m{rest:02d}s'
    return f'{minutes}m{rest:02d}s'


def chapter_rows(chapters, total_seconds, total_bytes):
    """Measure each chapter: (start, duration, estimated bytes, title).

    The size is the meeting's average rate over the chapter's length. Rate
    varies with what is on screen, so it is an estimate, not a promise. When
    the meeting's length is unknown the last chapter cannot be measured.
    """
    rate = (total_bytes / total_seconds
            if total_bytes and total_seconds else None)
    rows = []
    for i, (start, title) in enumerate(chapters):
        if i + 1 < len(chapters):
            end = chapters[i + 1][0]
        else:
            end = total_seconds
        seconds = None if end is None else max(0, end - start)
        size = None if (seconds is None or rate is None) else seconds * rate
        rows.append((start, seconds, size, title))
    return rows


def chapter_ranges(chapters):
    """Pair each chapter with where it ends: the next one's start.

    The last chapter has no successor, so it runs to the end of the video.
    """
    ranges = []
    for i, (start, title) in enumerate(chapters):
        end = chapters[i + 1][0] if i + 1 < len(chapters) else None
        ranges.append((start, end, title))
    return ranges


def chapter_filename(output_file, title):
    """Name a chapter's file after the meeting it came from."""
    stem, extension = os.path.splitext(output_file)
    safe = _safe_filename(title)[:MAX_CHAPTER_TITLE].strip()
    return f'{stem} {safe}{extension}'


def meeting_size(video_url, playlist=None):
    """Return (seconds, bytes) for the whole meeting, or (None, None).

    Used only to measure chapters, so a failure costs the estimate rather
    than the download.
    """
    try:
        head = SESSION.head(video_url, headers={"User-Agent": USER_AGENT},
                            timeout=LISTING_TIMEOUT)
        head.raise_for_status()
        total_bytes = int(head.headers["Content-Length"])
        if playlist is None:
            playlist = fetch_playlist(video_url)
        seconds = sum(float(x) for x in SEGMENT_DURATION_RE.findall(playlist))
    except (requests.RequestException, KeyError, ValueError):
        return None, None
    return (seconds or None), total_bytes


def choose_chapters(rows):
    """Ask which chapters to download. Nothing is selected to begin with."""
    # prompt_toolkit places an inline prompt by asking the terminal where the
    # cursor is. Warp never answers (warpdotdev/warp#7739), and the prompt
    # then renders part-way down the screen or takes the window over. Telling
    # prompt_toolkit not to ask makes the placement deterministic. Read when
    # the prompt is built, so setting it here is in time.
    os.environ.setdefault('PROMPT_TOOLKIT_NO_CPR', '1')
    import questionary
    choices = [questionary.Choice(title=chapter_choice_label(row), value=index)
               for index, row in enumerate(rows)]
    picked = questionary.checkbox(
        'Select chapters to download', choices=choices).ask()
    return picked or []


def parse_timecode(text):
    """Read 'HH:MM:SS', 'MM:SS' or plain seconds into a number of seconds."""
    parts = str(text).strip().split(':')
    if len(parts) > 3 or not all(part.isdigit() for part in parts):
        raise ValueError(
            f"{text!r} is not a time; use HH:MM:SS, MM:SS or seconds")
    seconds = 0
    for part in parts:
        seconds = seconds * 60 + int(part)
    return seconds


def range_suffix(start, end):
    """Describe a time range in a form a filesystem will accept."""
    def clock(seconds):
        hours, seconds = divmod(int(seconds), 3600)
        minutes, seconds = divmod(seconds, 60)
        return f'{hours}h{minutes:02d}m{seconds:02d}s'
    return f'{clock(start)}-{clock(end) if end is not None else "end"}'


def chunklist_url(video_url):
    """Derive the stream's segment playlist from an archived video URL."""
    path = urllib.parse.urlparse(video_url).path.strip('/')
    if path.count('/') != 1:
        raise ValueError(f"cannot find the stream for {video_url}")
    client, name = path.split('/')
    return (f'{STREAM_HOST}/OnDemand/_definst_/'
            f'mp4:archive/{client}/{name}/chunklist.m3u8')


def select_segments(playlist, playlist_url, start, end):
    """Return the URLs of the segments overlapping [start, end).

    A segment is kept whole, so the result covers at least the requested
    range and at most one segment more at each end.
    """
    urls = []
    elapsed = 0.0
    duration = None
    for line in playlist.splitlines():
        line = line.strip()
        if not line:
            continue
        match = SEGMENT_DURATION_RE.match(line)
        if match:
            duration = float(match.group(1))
            continue
        if line.startswith('#') or duration is None:
            continue
        segment_start, segment_end = elapsed, elapsed + duration
        elapsed = segment_end
        duration = None
        if segment_end > start and (end is None or segment_start < end):
            urls.append(urllib.parse.urljoin(playlist_url, line))
    return urls


def download_segment(url, progress=None, cancelled=None):
    """Download one stream segment.

    Read in blocks rather than whole so that an interrupt can stop it partway,
    as it already can for a byte-range chunk.
    """
    if cancelled is not None and cancelled.is_set():
        # Checked before the request as well as during it: a request that has
        # begun holds its worker until it finishes or gives up retrying, and
        # stopping waits on every worker.
        raise concurrent.futures.CancelledError()
    response = SESSION.get(url, headers={"User-Agent": USER_AGENT},
                           stream=True)
    response.raise_for_status()
    blocks = []
    for block in response.iter_content(chunk_size=PROGRESS_BLOCK_SIZE):
        if cancelled is not None and cancelled.is_set():
            # Raise rather than return: these bytes are half a segment and
            # must never reach the output file.
            raise concurrent.futures.CancelledError()
        blocks.append(block)
    if progress is not None:
        progress.update(1)
    return b"".join(blocks)


def download_segments(urls, num_threads, output_file, progress_sink=None):
    """Fetch stream segments in parallel and write them in playlist order.

    Segments have no length until they arrive, so unlike a byte-range
    download the bar counts segments rather than bytes.
    """
    progress = None
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_threads)
    try:
        if progress_sink is not None:
            progress = tqdm(total=len(urls), unit='seg', file=progress_sink,
                            disable=None,
                            desc=progress_description(
                                f'Downloading '
                                f'{os.path.basename(output_file)}'))
        cancelled = threading.Event()
        futures = [executor.submit(download_segment, url, progress, cancelled)
                   for url in urls]
        try:
            with open(output_file, "wb") as f:
                for future in futures:
                    f.write(future.result())
        except BaseException as stopped:
            # Say so before the waiting starts: shutdown still has to let go
            # of whatever is mid-flight, and a bar left ticking through that
            # reads as though the interrupt was missed.
            _announce_stop(stopped, progress)
            progress = None
            cancelled.set()
            executor.shutdown(wait=False, cancel_futures=True)
            raise
    finally:
        executor.shutdown(wait=True)
        if progress is not None:
            progress.close()


def remux_to_mp4(source, destination):
    """Rewrap a .ts as .mp4 without re-encoding. False if ffmpeg is absent."""
    if shutil.which('ffmpeg') is None:
        return False
    result = subprocess.run(
        ['ffmpeg', '-y', '-v', 'error', '-i', source, '-c', 'copy',
         destination],
        capture_output=True)
    return result.returncode == 0


def _recording_date(page_url, view_id, clip_id):
    """Return the clip's meeting date as YYYY-MM-DD, or None.

    The date is a nicety, so every failure here returns None rather than
    raising: not knowing it must never stop a download.
    """
    parts = urllib.parse.urlparse(page_url)
    listing = (f'{parts.scheme}://{parts.netloc}'
               f'/ViewPublisher.php?view_id={view_id}')
    try:
        response = SESSION.get(listing, headers={"User-Agent": USER_AGENT},
                               timeout=LISTING_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException:
        return None

    page = response.text
    for match in re.finditer(rf'clip_id={clip_id}\b', page):
        row = page[max(0, match.start() - LISTING_ROW_SPAN):match.start()]
        dates = LISTING_DATE_RE.findall(row)
        if dates:
            month, day, year = dates[-1]
            return f'20{year}-{month}-{day}'
    return None


def resolve_video_url(url, page=None):
    """Resolve a Granicus player URL to (video url, default filename).

    A URL that already points at a media file is returned untouched, so
    pasting a direct .mp4 link costs no extra request. `page` lets a caller
    that has already read the page hand it over rather than have it read
    twice.
    """
    if is_media_url(url):
        return url, os.path.basename(urllib.parse.urlparse(url).path)

    if page is None:
        # A clip id that does not exist is a clean 404, which says so far
        # better than anything we could infer from the page body.
        page = fetch_page(url)

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


def _announce_stop(reason, progress=None):
    """Acknowledge an interrupt at once, and still the progress bar.

    What follows is a wait on whatever is already downloading. A bar left
    ticking through that looks like the interrupt was missed, which is how it
    earns a second one.
    """
    if not isinstance(reason, KeyboardInterrupt):
        return
    if progress is not None:
        progress.close()
    print('\nStopping...', file=sys.stderr)


def build_session(pool_size=CONNECTION_POOL):
    """A session that reuses connections and rides out transient failures."""
    session = requests.Session()
    session.headers['User-Agent'] = USER_AGENT
    retries = requests.adapters.Retry(
        total=RETRY_ATTEMPTS, backoff_factor=RETRY_BACKOFF,
        status_forcelist=RETRY_ON_STATUS,
        allowed_methods=('GET', 'HEAD'))
    adapter = requests.adapters.HTTPAdapter(
        max_retries=retries, pool_connections=pool_size,
        pool_maxsize=pool_size)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session


SESSION = build_session()


class Node:
    def __init__(self, data=None, next=None):
        self.data = data
        self.next = next


def download_chunk(url, start, end, progress=None, cancelled=None):
    """Download a chunk of the video."""
    headers = {"Range": f"bytes={start}-{end}", "User-Agent": USER_AGENT}
    response = SESSION.get(url, headers=headers, stream=True)
    # Without this a rejected chunk writes the error page into the video file
    response.raise_for_status()
    blocks = []
    for block in response.iter_content(chunk_size=PROGRESS_BLOCK_SIZE):
        if cancelled is not None and cancelled.is_set():
            # Raise rather than return: these bytes are a partial chunk and
            # must never reach the output file.
            raise concurrent.futures.CancelledError()
        blocks.append(block)
        if progress is not None:
            progress.update(len(block))
    return b"".join(blocks)


def download_video(url, chunk_size, num_threads, output_file,
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
            response = SESSION.head(url, headers={"User-Agent": USER_AGENT})
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

            for (start, end) in chunks:
                if head is None:
                    head = Node()
                    current = head
                else:
                    current.next = Node()
                    current = current.next
                current.data = executor.submit(
                    download_chunk, url, start, end, progress, cancelled)

            # Use `head` instead of `current` so we can free up memory as we write to file
            try:
                with open(output_file, "wb") as f:
                    while head is not None:
                        f.write(head.data.result())
                        head = head.next
            except BaseException as stopped:
                # Drop the chunks still queued, and tell the ones already
                # streaming to stop reading. Without both, the interrupt
                # downloads the rest of the file before it exits.
                _announce_stop(stopped, progress)
                progress = None
                cancelled.set()
                executor.shutdown(wait=False, cancel_futures=True)
                raise
    finally:
        # Closing after the executor's context manager has exited means the
        # pool has drained, so nothing renders after the bar's final line.
        if progress is not None:
            progress.close()


def download_range(url, start, end, num_threads, output_file,
                   progress_sink=None, playlist=None):
    """Download only the part of a video between two times.

    Works off the segmented stream rather than the mp4, because a range of an
    mp4 cannot be cut out by byte offsets alone. Returns the file written,
    which is the .ts rather than the requested .mp4 if ffmpeg is unavailable.
    """
    playlist_url = chunklist_url(url)
    if playlist is None:
        playlist = fetch_playlist(url)
    segments = select_segments(playlist, playlist_url, start, end)
    if not segments:
        raise SystemExit(f"no video found in {range_suffix(start, end)}")

    stem, extension = os.path.splitext(output_file)
    stream_file = f'{stem}.ts'
    download_segments(segments, num_threads, stream_file,
                      progress_sink=progress_sink)
    if extension != '.mp4':
        return stream_file

    mp4_file = f'{stem}.mp4'
    if remux_to_mp4(stream_file, mp4_file):
        os.remove(stream_file)
        return mp4_file
    print(f'ffmpeg was not found, so the download is left as {stream_file}, '
          f'which plays as it is.', file=sys.stderr)
    return stream_file


def _network_reason(error):
    """Say what went wrong in a line, rather than in a stack of causes."""
    if isinstance(error, requests.exceptions.Timeout):
        return 'the server did not respond in time'
    if isinstance(error, requests.exceptions.ConnectionError):
        return 'could not reach the server'
    response = getattr(error, 'response', None)
    if response is not None:
        return f'the server answered {response.status_code}'
    return str(error) or error.__class__.__name__


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
    parser.add_argument('--no-progress', action='store_true',
                        help='Do not display the progress bar')
    parser.add_argument('--chapters', action='store_true',
                        help='List the agenda items and pick which to download')
    parser.add_argument('--start', type=str, default=None,
                        help='Start of the range to download, as HH:MM:SS, MM:SS or seconds')
    parser.add_argument('--end', type=str, default=None,
                        help='End of the range to download, as HH:MM:SS, MM:SS or seconds')
    args = parser.parse_args()

    chunk_size = args.chunk_size
    # --chapters wants the page too, so read it once and share it.
    page = None
    if args.chapters and not is_media_url(args.url):
        page = fetch_page(args.url)
    url, default_output_file = resolve_video_url(args.url, page=page)
    output_file = args.output_file or default_output_file
    num_threads = args.num_threads
    progress_sink = None if args.no_progress else sys.stderr
    start = parse_timecode(args.start) if args.start else None
    end = parse_timecode(args.end) if args.end else None
    if end is not None and end <= (start or 0):
        raise SystemExit("--end must come after --start")
    if args.chapters and (start is not None or end is not None):
        raise SystemExit("--chapters and --start/--end are two ways to ask "
                         "for the same thing; use one")

    if args.chapters:
        if not sys.stdin.isatty():
            raise SystemExit("--chapters needs a terminal to ask you in")
        if page is None:
            raise SystemExit("--chapters needs the address of a player page, "
                             "not a link straight to the video")
        chapters = parse_chapters(page)
        if not chapters:
            raise SystemExit(f"{args.url} lists no agenda items")
        # Read once here and hand to every chapter downloaded below.
        playlist = fetch_playlist(url)
        total_seconds, total_bytes = meeting_size(url, playlist=playlist)
        rows = chapter_rows(chapters, total_seconds, total_bytes)
        picked = choose_chapters(rows)
        if not picked:
            raise SystemExit("Nothing selected; nothing downloaded")
        ranges = chapter_ranges(chapters)
        failed = []
        try:
            for index in picked:
                chapter_start, chapter_end, title = ranges[index]
                try:
                    written = download_range(
                        url, chapter_start, chapter_end, num_threads,
                        chapter_filename(output_file, title),
                        progress_sink=progress_sink, playlist=playlist)
                except requests.RequestException as error:
                    # One chapter failing is no reason to abandon the rest.
                    failed.append(title)
                    print(f'Could not download {title}: '
                          f'{_network_reason(error)}', file=sys.stderr)
                    continue
                print(f'Wrote {written}', file=sys.stderr)
        except KeyboardInterrupt:
            print("\nInterrupted.", file=sys.stderr)
            raise SystemExit(130)
        if failed:
            raise SystemExit(f'{len(failed)} of {len(picked)} could not be '
                             f'downloaded')
        return

    try:
        if start is None and end is None:
            download_video(url, chunk_size, num_threads, output_file,
                           progress_sink=progress_sink)
        else:
            if not args.output_file:
                # Keep a clip from overwriting the whole meeting.
                stem, extension = os.path.splitext(output_file)
                output_file = (f'{stem} {range_suffix(start or 0, end)}'
                               f'{extension}')
            output_file = download_range(
                url, start or 0, end, num_threads, output_file,
                progress_sink=progress_sink)
    except KeyboardInterrupt:
        # 130 is the conventional exit code for SIGINT. The partial file is
        # left alone: it is the user's data, not ours to delete.
        print(f"\nInterrupted. Partial download left at {output_file}",
              file=sys.stderr)
        raise SystemExit(130)
    except requests.RequestException as error:
        # A network fault is not a crash; say what happened in a line.
        raise SystemExit(f'Download failed: {_network_reason(error)}. '
                         f'Partial download left at {output_file}')


if __name__ == '__main__':
    main()
