import io
import os
import signal
import sys
import threading
import time

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


def test_interrupt_stops_the_download_promptly(tmp_path, granicus, payload):
    # A bare Future.result() parks in an untimed lock acquire that SIGINT
    # cannot break, so the download used to run to completion after Ctrl-C.
    # Signals are only delivered to the main thread, which is where pytest
    # runs this, so os.kill on ourselves is a faithful stand-in for Ctrl-C.
    size = CHUNK * 20
    server = granicus(payload(size),
                      delay_ranges={i * CHUNK: 0.5 for i in range(20)})
    interrupt = threading.Timer(0.3, os.kill, (os.getpid(), signal.SIGINT))
    interrupt.start()
    started = time.monotonic()
    try:
        with pytest.raises(KeyboardInterrupt):
            fetch.download_video(server.url, CHUNK, 2,
                                 str(tmp_path / "out.mp4"))
    finally:
        interrupt.cancel()

    assert time.monotonic() - started < 3, "interrupt was not acted on promptly"


PLAYER_PAGE = b"""<html><head><title>BOS Rules Committee</title></head><body>
<a href="https://archive-stream.granicus.com/OnDemand/_definst_/mp4:archive/sf/sf_abc.mp4/playlist.m3u8">stream</a>
<a href="https://archive-video.granicus.com/sanfrancisco/sanfrancisco_0fa97861.mp4">Download</a>
<a href="https://archive-video.granicus.com/sanfrancisco/sanfrancisco_0fa97861.mp3">Audio</a>
</body></html>"""


def test_resolves_a_player_page_to_the_video_url(granicus):
    server = granicus(PLAYER_PAGE)

    video_url, _ = fetch.resolve_video_url(f"{server.url}/player/clip/42000")

    assert video_url == (
        "https://archive-video.granicus.com/sanfrancisco/"
        "sanfrancisco_0fa97861.mp4")


def test_falls_back_to_the_clip_id_when_no_view_is_named(granicus):
    server = granicus(PLAYER_PAGE)

    _, name = fetch.resolve_video_url(f"{server.url}/player/clip/42000")

    assert name == "BOS Rules Committee-42000.mp4"


def test_legacy_media_player_urls_also_resolve(granicus):
    server = granicus(PLAYER_PAGE)

    _, name = fetch.resolve_video_url(
        f"{server.url}/MediaPlayer.php?view_id=13&clip_id=42000")

    assert name == "BOS Rules Committee-42000.mp4"


def test_a_direct_media_url_is_returned_without_fetching_anything(granicus):
    server = granicus(PLAYER_PAGE)
    direct = f"{server.url}"          # the fixture URL ends in /video.mp4

    video_url, name = fetch.resolve_video_url(direct)

    assert video_url == direct
    assert name == "video.mp4"
    assert server.requests == [], "a direct .mp4 URL must not be fetched"


def test_a_page_with_no_video_reports_clearly(granicus):
    server = granicus(b"<html><head><title>Nothing</title></head></html>")

    with pytest.raises(ValueError, match="no downloadable video"):
        fetch.resolve_video_url(f"{server.url}/player/clip/1")


def test_titles_are_made_safe_for_the_filesystem(granicus):
    page = PLAYER_PAGE.replace(
        b"<title>BOS Rules Committee</title>",
        b"<title>Budget &amp; Finance: 3/4  Committee</title>")
    server = granicus(page)

    _, name = fetch.resolve_video_url(f"{server.url}/player/clip/7")

    assert name == "Budget & Finance 34 Committee-7.mp4"
    assert "/" not in name.replace(".mp4", "")


# The fixture serves one body for every path, so this doubles as the player
# page and as the view listing that the date is looked up in.
PAGE_WITH_LISTING_ROW = PLAYER_PAGE.replace(
    b"</body>",
    b"""<tr><td>09/12/22</td>
<td><a href="MediaPlayer.php?view_id=13&clip_id=42000">Video</a></td></tr>
</body>""")


def test_uses_the_meeting_date_when_the_url_names_a_view(granicus):
    server = granicus(PAGE_WITH_LISTING_ROW)

    _, name = fetch.resolve_video_url(
        f"{server.url}/player/clip/42000?view_id=13")

    assert name == "BOS Rules Committee-2022-09-12.mp4"


def test_falls_back_to_the_clip_id_when_the_view_has_no_date_for_it(granicus):
    # A clip belongs to one body's view; asking a different view yields
    # nothing, which must not cost us a usable filename.
    server = granicus(PLAYER_PAGE)

    _, name = fetch.resolve_video_url(
        f"{server.url}/player/clip/42000?view_id=13")

    assert name == "BOS Rules Committee-42000.mp4"


def test_an_unreachable_listing_yields_no_date_rather_than_raising():
    # The date is a nicety. Failing to find it must never stop a download.
    assert fetch._recording_date(
        "http://127.0.0.1:9/player/clip/42000", "13", "42000") is None


CHUNKLIST = b"""#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:4
#EXTINF:2.0,
media_0.ts
#EXTINF:2.0,
media_1.ts
#EXTINF:2.0,
media_2.ts
#EXTINF:2.0,
media_3.ts
#EXTINF:2.0,
media_4.ts
#EXT-X-ENDLIST
"""


def test_parses_the_timecode_forms_people_type():
    assert fetch.parse_timecode("90") == 90
    assert fetch.parse_timecode("1:30") == 90
    assert fetch.parse_timecode("01:00:00") == 3600
    assert fetch.parse_timecode("2:03:04") == 7384


def test_rejects_a_timecode_it_cannot_read():
    with pytest.raises(ValueError, match="not a time"):
        fetch.parse_timecode("half past four")


def test_derives_the_chunklist_url_from_the_media_url():
    assert fetch.chunklist_url(
        "https://archive-video.granicus.com/sanfrancisco/sf_abc.mp4") == (
        "https://archive-stream.granicus.com/OnDemand/_definst_/"
        "mp4:archive/sanfrancisco/sf_abc.mp4/chunklist.m3u8")


def test_selects_only_the_segments_covering_the_range():
    base = "https://host/path/chunklist.m3u8"

    urls = fetch.select_segments(CHUNKLIST.decode(), base, 4, 8)

    assert urls == ["https://host/path/media_2.ts",
                    "https://host/path/media_3.ts"]


def test_a_range_boundary_inside_a_segment_keeps_that_segment():
    base = "https://host/path/chunklist.m3u8"

    urls = fetch.select_segments(CHUNKLIST.decode(), base, 3, 5)

    assert urls == ["https://host/path/media_1.ts",
                    "https://host/path/media_2.ts"]


def test_an_open_ended_range_runs_to_the_end():
    base = "https://host/path/chunklist.m3u8"

    urls = fetch.select_segments(CHUNKLIST.decode(), base, 6, None)

    assert urls == ["https://host/path/media_3.ts",
                    "https://host/path/media_4.ts"]


def test_range_suffix_is_safe_for_a_filename():
    assert fetch.range_suffix(3600, 4200) == "1h00m00s-1h10m00s"
    assert fetch.range_suffix(0, None) == "0h00m00s-end"


def test_downloads_segments_in_order(tmp_path, granicus):
    # Segment 0 is served slowly so it completes last; the output must still
    # follow the playlist order, not the completion order.
    server = granicus(b"", segment_bodies={
        "/media_0.ts": b"AAAA", "/media_1.ts": b"BBBB", "/media_2.ts": b"CCCC"},
        delay_paths={"/media_0.ts": 0.4})
    out = tmp_path / "clip.ts"

    fetch.download_segments(
        [f"{server.origin}/media_{i}.ts" for i in range(3)],
        4, str(out), verbose=False)

    assert out.read_bytes() == b"AAAABBBBCCCC"


CHAPTER_PAGE = b"""<html><head><title>BOS Rules Committee</title></head><body>
<a href="https://archive-video.granicus.com/sanfrancisco/sanfrancisco_0fa97861.mp4">Download</a>
<section id="index">
<div class="index-point flex-col-center" role="link" time="14" data-id="1" tabindex="0" >
 11 ROLL CALL AND ANNOUNCEMENTS </div>
<div class="index-point flex-col-center" role="link" time="375" data-id="2" tabindex="0" >
 220848 Appointment, Treasury Oversight Committee </div>
<div time="470" class="index-point" role="link" data-id="3" tabindex="0" >
 220427 Administrative Code &amp; Veterans </div>
</section></body></html>"""


def test_parses_chapters_from_the_player_page():
    chapters = fetch.parse_chapters(CHAPTER_PAGE.decode())

    assert chapters == [
        (14, "11 ROLL CALL AND ANNOUNCEMENTS"),
        (375, "220848 Appointment, Treasury Oversight Committee"),
        (470, "220427 Administrative Code & Veterans"),
    ]


def test_a_page_without_chapters_has_none():
    assert fetch.parse_chapters(PLAYER_PAGE.decode()) == []


def test_each_chapter_ends_where_the_next_one_begins():
    ranges = fetch.chapter_ranges([(14, "one"), (375, "two"), (470, "three")])

    assert ranges[0] == (14, 375, "one")
    assert ranges[1] == (375, 470, "two")


def test_the_last_chapter_runs_to_the_end_of_the_meeting():
    ranges = fetch.chapter_ranges([(14, "one"), (375, "two")])

    assert ranges[-1] == (375, None, "two")


def test_chapter_filenames_hang_off_the_meeting_name():
    name = fetch.chapter_filename(
        "BOS Rules Committee-2022-09-12.mp4",
        "220427 Administrative Code / Veterans: part 2")

    # Removing the slash and colon leaves doubled spaces, which collapse.
    assert name == (
        "BOS Rules Committee-2022-09-12 "
        "220427 Administrative Code Veterans part 2.mp4")


def test_chapter_filenames_stay_within_a_sane_length():
    name = fetch.chapter_filename("meeting.mp4", "x" * 400)

    assert len(name.encode()) < 255
