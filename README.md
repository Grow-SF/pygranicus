# pygranicus

[![tests](https://github.com/Grow-SF/pygranicus/actions/workflows/tests.yml/badge.svg)](https://github.com/Grow-SF/pygranicus/actions/workflows/tests.yml)

Download granicus video files efficiently

# Usage

No clone or install needed — [uv](https://docs.astral.sh/uv/) fetches and runs it:

```sh
uvx --from git+https://github.com/Grow-SF/pygranicus pygranicus "https://archive-video.granicus.com/video/file/here.mp4" --verbose
```

From a local clone:

```sh
uv run pygranicus "https://archive-video.granicus.com/video/file/here.mp4" --verbose
```

To install it as a persistent command on your PATH:

```sh
uv tool install git+https://github.com/Grow-SF/pygranicus
pygranicus "https://archive-video.granicus.com/video/file/here.mp4" --verbose
```

## Options

| Flag | Description |
| --- | --- |
| `-c`, `--chunk_size` | Size of each chunk to download, in bytes. Defaults to 32 MiB. |
| `-o`, `--output_file` | Output filename. Defaults to the last part of the url path. |
| `-t`, `--num_threads` | Number of threads to use for downloading. Defaults to the CPU count. |
| `-v`, `--verbose` | Print chunk progress and per-chunk download speed. |

# Development

```sh
uv sync            # create the venv, including dev dependencies
uv run pygranicus --help
```

Dependencies are declared in `pyproject.toml` and pinned in `uv.lock`; both are
committed. This project used pipenv before v1.0.0.
