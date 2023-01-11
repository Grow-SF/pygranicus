import argparse
import requests
import time
import os
import concurrent.futures


CHUNK_SIZE = 32 * 1024 * 1024  # 8 MiB
OUTPUT_FILE = None
NUM_THREADS = os.cpu_count()


class Node:
    def __init__(self, chunk_id, data=None, next=None):
        self.data = data
        self.chunk_id = chunk_id
        self.next = next


def download_chunk(url, start, end, i, num_chunks, verbose):
    """Download a chunk of the video.

    Note that i and num_chunks are only needed for verbose output
    """
    headers = {"Range": f"bytes={start}-{end}"}
    response = requests.get(url, headers=headers, stream=True)
    if verbose:
        print(f'Downloading chunk {i} of {num_chunks}')
        start_time = time.time()
    content = response.content
    if verbose:
        end_time = time.time()
        download_speed = len(content) / (end_time - start_time)
        print(
            f'chunk {i} download speed: {download_speed / (1024 * 1024):.2f} MiB/s')
    return content


def download_video(url, chunk_size, num_threads, output_file, verbose=False):
    """
    Downloads the video by creating a linked list of concurrent.futures jobs and writes to output_file.
    """
    head = None
    current = None
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        response = requests.head(url)
        file_size = int(response.headers["Content-Length"])
        chunks = [(i * chunk_size, (i + 1) * chunk_size - 1)
                  for i in range(file_size//chunk_size)]
        chunks[-1] = (chunks[-1][0], file_size - 1)

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
                download_chunk, url, start, end, i, num_chunks, verbose)

        # Use `head` instead of `current` so we can free up memory as we write to file
        with open(output_file, "wb") as f:
            while head is not None:
                result = head.data.result()
                if verbose:
                    print(f'Writing chunk {head.chunk_id} of {num_chunks}')
                f.write(result)
                head = head.next


if __name__ == '__main__':
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
    args = parser.parse_args()

    url = args.url
    chunk_size = args.chunk_size
    if args.output_file:
        output_file = args.output_file
    else:
        output_file = os.path.basename(url)
    num_threads = args.num_threads
    verbose = args.verbose
    download_video(url, chunk_size, num_threads, output_file, verbose)
