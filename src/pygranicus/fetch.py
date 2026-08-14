import argparse
import requests
import time
import os
import sys
import concurrent.futures

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


def download_chunk(url, start, end, i, num_chunks, verbose, progress=None):
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
                    progress)

            # Use `head` instead of `current` so we can free up memory as we write to file
            with open(output_file, "wb") as f:
                while head is not None:
                    result = head.data.result()
                    if verbose:
                        _log(f'Writing chunk {head.chunk_id} of {num_chunks}',
                             progress)
                    f.write(result)
                    head = head.next
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

    url = args.url
    chunk_size = args.chunk_size
    if args.output_file:
        output_file = args.output_file
    else:
        output_file = os.path.basename(url)
    num_threads = args.num_threads
    verbose = args.verbose
    download_video(url, chunk_size, num_threads, output_file, verbose,
                   progress_sink=None if args.no_progress else sys.stderr)


if __name__ == '__main__':
    main()
