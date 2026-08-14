# pygranicus

[![tests](https://github.com/Grow-SF/pygranicus/actions/workflows/tests.yml/badge.svg)](https://github.com/Grow-SF/pygranicus/actions/workflows/tests.yml)

Download granicus video files efficiently

# Usage

Paste the URL of the player page you are watching, and the video is found for
you:

```sh
uvx --from git+https://github.com/Grow-SF/pygranicus pygranicus "https://sanfrancisco.granicus.com/player/clip/42000?view_id=13&redirect=true"
```

That saves `BOS Rules Committee-2022-09-12.mp4`, named after the meeting rather
than the underlying file's identifier. The date is only published on a body's
listing page, so it can be found when the URL you paste carries a `view_id`;
without one the clip id is used instead. The older `MediaPlayer.php?clip_id=…` links
work too, as does a direct media URL:

```sh
uv run pygranicus "https://archive-video.granicus.com/video/file/here.mp4" --verbose
```

Use `-o` to choose the filename yourself.

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
| `--no-progress` | Disable the progress bar. The bar is shown by default when stderr is a terminal, and silences itself automatically when output is piped or redirected. |

# Development

```sh
uv sync            # create the venv, including dev dependencies
uv run pygranicus --help
```

Dependencies are declared in `pyproject.toml` and pinned in `uv.lock`; both are
committed. This project used pipenv before v1.0.0.
