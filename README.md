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

## Picking agenda items

Meetings are indexed by agenda item. `--chapters` lists them and lets you tick
the ones you want:

```sh
uv run pygranicus "https://sanfrancisco.granicus.com/player/clip/42000?view_id=13" --chapters
```

```
? Select chapters to download  (space to toggle, enter to confirm)
  ○ 0:00:14  11 ROLL CALL AND ANNOUNCEMENTS
  ○ 0:06:15  220848 Appointment, Treasury Oversight Committee
❯ ◉ 0:07:50  220946 Appointment, Children, Youth and Their Families
  ○ 0:11:22  220427 Administrative Code - County Veterans Service Officer
```

Nothing is selected to begin with, so you opt in to each item and an accidental
return downloads nothing. Each item you tick is saved as its own file, named
after the agenda item, and only the video for that item is fetched.

This needs a terminal to ask you in, so it will not run with input redirected
or under CI.

## Downloading part of a meeting

Meetings run for hours. To take a slice of one, give `--start` and `--end` as
`HH:MM:SS`, `MM:SS`, or plain seconds:

```sh
uv run pygranicus "https://sanfrancisco.granicus.com/player/clip/42000?view_id=13" --start 1:00:00 --end 1:10:00
```

That fetches only the part you asked for — about 8 MB per 30 seconds, rather
than the 2.5 GB of a full meeting — and saves
`BOS Rules Committee-2022-09-12 1h00m00s-1h10m00s.mp4`. Either bound may be
omitted: `--start` alone runs to the end, `--end` alone from the beginning.

Ranges come from the segmented stream, which is cut into pieces of about two
seconds. A segment is kept whole, so you may get up to two seconds of extra
video at each end. Trimming to the exact frame would mean re-encoding, which is
slower and loses quality.

The pieces are joined into an `.mp4` using `ffmpeg` if it is installed. Without
it you get a `.ts` file instead, which plays in VLC and most players.

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
