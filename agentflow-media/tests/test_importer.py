import sys
import threading
import time
import types
import wave
from pathlib import Path

import pytest
from PIL import Image

from app.importer import FetchError, check_url, import_path, import_url, ytdlp_fetch
from app.stats import Stats
from app.store import Store
from tests.fakes import URL, fake_fetch


def test_import_url_stores_media_and_meta(tmp_path):
    s, calls, stats = Store(tmp_path), [], Stats()
    meta = import_url(s, URL, fake_fetch(calls), stats)
    assert meta["cached"] is False and meta["title"] == "Lecture 1" and meta["durationMs"] == 3600500
    assert meta["sourceUrl"] == URL and meta["audio"] == "audio.m4a" and meta["video"] == "video.mp4"
    d = s.dir(meta["mediaKey"])
    assert (d / "audio.m4a").read_bytes() == b"AUDIO" and (d / "video.mp4").exists()
    assert s.read_json(meta["mediaKey"], "meta.json")["title"] == "Lecture 1"
    assert "cached" not in s.read_json(meta["mediaKey"], "meta.json") and stats.downloads == 1


def test_same_url_is_not_downloaded_twice(tmp_path):
    s, calls, stats = Store(tmp_path), [], Stats()
    first = import_url(s, URL, fake_fetch(calls), stats)
    again = import_url(s, URL, fake_fetch(calls), stats)
    assert calls == [URL] and again["cached"] is True and again["mediaKey"] == first["mediaKey"]
    assert stats.downloads == 1


def test_same_audio_from_another_url_reuses_the_media(tmp_path):
    s, calls, stats = Store(tmp_path), [], Stats()
    a = import_url(s, URL, fake_fetch(calls), stats)
    b = import_url(s, "https://youtu.be/abc123", fake_fetch(calls), stats)
    assert b["mediaKey"] == a["mediaKey"] and b["cached"] is True and len(calls) == 2


def test_audio_only_source_has_no_video(tmp_path):
    meta = import_url(Store(tmp_path), URL, fake_fetch([], video=None), Stats())
    assert meta["video"] is None


def test_concurrent_imports_of_one_url_download_once(tmp_path):
    s, calls, stats = Store(tmp_path), [], Stats()
    inner = fake_fetch(calls)

    def slow(url, dest):
        time.sleep(0.2)
        return inner(url, dest)

    threads = [threading.Thread(target=import_url, args=(s, URL, slow, stats)) for _ in range(3)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert calls == [URL]


@pytest.mark.parametrize("bad", [
    "ftp://www.youtube.com/x", "http://localhost/x", "https://evil.example/watch?v=1",
    "https://www.youtube.com.evil.example/x", "file:///etc/passwd", "not a url", "",
])
def test_rejects_urls_outside_the_allowlist(bad):
    with pytest.raises(ValueError):
        check_url(bad)


def test_failed_download_leaves_nothing_behind(tmp_path):
    s = Store(tmp_path)

    def fetch(url, dest):
        (dest / "audio.m4a.part").write_bytes(b"x")
        raise RuntimeError("HTTP 403")

    with pytest.raises(FetchError):
        import_url(s, URL, fetch, Stats())
    assert s.url_key(URL) is None and not list((tmp_path / "tmp").glob("*"))


def test_import_path_moves_file_from_inbox(tmp_path):
    s, inbox = Store(tmp_path), tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "lec 1.mp4").write_bytes(b"LOCAL")
    meta = import_path(s, str(inbox / "lec 1.mp4"), inbox, probe=lambda p: 12.0)
    assert meta["title"] == "lec 1" and meta["durationMs"] == 12000 and meta["sourceUrl"] is None
    assert meta["audio"] == meta["video"] == "source.mp4" and meta["cached"] is False
    assert not (inbox / "lec 1.mp4").exists() and (s.dir(meta["mediaKey"]) / "source.mp4").read_bytes() == b"LOCAL"


def test_importing_the_same_local_content_twice_is_cached(tmp_path):
    s, inbox = Store(tmp_path), tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "a.mp4").write_bytes(b"SAME")
    first = import_path(s, str(inbox / "a.mp4"), inbox, probe=lambda p: 1.0)
    (inbox / "b.mp4").write_bytes(b"SAME")
    again = import_path(s, str(inbox / "b.mp4"), inbox, probe=lambda p: 1.0)
    assert again["mediaKey"] == first["mediaKey"] and again["cached"] is True
    assert (inbox / "b.mp4").exists()            # 重复的文件留在原处,不删用户的东西


def test_import_path_rejects_outside_inbox_or_missing(tmp_path):
    s, inbox = Store(tmp_path), tmp_path / "inbox"
    inbox.mkdir()
    (tmp_path / "outside.mp4").write_bytes(b"x")
    for p in [str(inbox / ".." / "outside.mp4"), "/etc/passwd", str(inbox / "missing.mp4"), str(inbox)]:
        with pytest.raises(ValueError):
            import_path(s, p, inbox, probe=lambda _: 1.0)
    assert (tmp_path / "outside.mp4").exists()


def test_symlink_pointing_outside_inbox_is_rejected(tmp_path):
    s, inbox = Store(tmp_path), tmp_path / "inbox"
    inbox.mkdir()
    (tmp_path / "secret.mp4").write_bytes(b"x")
    (inbox / "link.mp4").symlink_to(tmp_path / "secret.mp4")
    with pytest.raises(ValueError):
        import_path(s, str(inbox / "link.mp4"), inbox, probe=lambda _: 1.0)


def test_import_path_accepts_a_name_relative_to_the_inbox(tmp_path):
    s, inbox = Store(tmp_path), tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "lec2.mp4").write_bytes(b"REL")
    meta = import_path(s, "lec2.mp4", inbox, probe=lambda p: 1.0)
    assert meta["title"] == "lec2" and meta["cached"] is False


def test_unreadable_local_file_is_rejected_and_left_in_the_inbox(tmp_path):
    s, inbox = Store(tmp_path), tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "broken.mp4").write_bytes(b"x")

    def probe(p):
        raise OSError("moov atom not found")

    with pytest.raises(ValueError):
        import_path(s, str(inbox / "broken.mp4"), inbox, probe=probe)
    assert (inbox / "broken.mp4").exists() and not (tmp_path / "media").exists()


# ---- 整体 review 之后补的 ----

@pytest.mark.parametrize("ok", [
    "https://www.youtube.com/watch?v=abc123", "https://www.youtube.com/watch?v=abc&list=PLx&index=2",
    "https://m.youtube.com/watch?v=abc", "https://youtu.be/abc123", "https://www.youtube.com/live/abc123",
])
def test_accepts_links_to_one_video(ok):
    check_url(ok)


@pytest.mark.parametrize("bad", [
    "https://www.youtube.com/playlist?list=PLS01nW3RtgoqGkm4UeqNeZLccW", "https://www.youtube.com/@berkeley",
    "https://www.youtube.com/channel/UCabc/videos", "https://www.youtube.com/watch?list=PLx",
    "https://www.youtube.com/", "https://youtu.be/",
])
def test_rejects_playlist_and_channel_links(bad):
    """播放列表和频道不在第一版范围内;放进去 yt-dlp 会把里面的视频全下一遍。"""
    with pytest.raises(ValueError):
        check_url(bad)


def fake_ytdlp(monkeypatch, video_fails=False):
    """顶替 yt_dlp 模块:记录被要求下载什么,不联网。"""
    asked = []

    class DownloadError(Exception):
        pass

    class YoutubeDL:
        def __init__(self, opts):
            self.opts = opts

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def extract_info(self, url, download):
            kind = "video" if self.opts["format"].startswith("bestvideo") else "audio"
            asked.append(kind)
            if kind == "video" and video_fails:
                raise DownloadError("HTTP Error 403: Forbidden")
            path = Path(self.opts["outtmpl"].replace("%(ext)s", "mp4" if kind == "video" else "m4a"))
            path.write_bytes(kind.encode())
            return {"id": "abc", "title": "T", "duration": 60, "webpage_url": url,
                    "requested_downloads": [{"filepath": str(path)}]}

    mod = types.SimpleNamespace(YoutubeDL=YoutubeDL, utils=types.SimpleNamespace(DownloadError=DownloadError))
    monkeypatch.setitem(sys.modules, "yt_dlp", mod)
    return asked, DownloadError


def test_ytdlp_fetch_gets_audio_and_video_as_two_files(tmp_path, monkeypatch):
    asked, _ = fake_ytdlp(monkeypatch)
    got = ytdlp_fetch(URL, tmp_path)
    assert got.audio.name == "audio.m4a" and got.video.name == "video.mp4"
    assert got.title == "T" and got.duration_s == 60.0 and got.source_url == URL and asked == ["audio", "video"]


def test_a_failed_video_download_is_an_error_not_a_lecture_without_picture(tmp_path, monkeypatch):
    _, DownloadError = fake_ytdlp(monkeypatch, video_fails=True)
    with pytest.raises(DownloadError):
        ytdlp_fetch(URL, tmp_path)


def test_files_that_are_not_lectures_are_rejected_by_the_real_probe(tmp_path):
    s, inbox = Store(tmp_path), tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "notes.txt").write_text("just some notes\n" * 50)
    Image.new("RGB", (64, 64), "white").save(inbox / "slide.jpg")
    for name in ["notes.txt", "slide.jpg"]:
        with pytest.raises(ValueError):
            import_path(s, name, inbox)          # 真实的探测(PyAV),不是假的
        assert (inbox / name).exists()
    assert not (tmp_path / "media").exists()


def test_a_local_audio_recording_is_imported_by_the_real_probe(tmp_path):
    s, inbox = Store(tmp_path), tmp_path / "inbox"
    inbox.mkdir()
    with wave.open(str(inbox / "rec.wav"), "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(16000)
        w.writeframes(b"\0\0" * 32000)
    meta = import_path(s, "rec.wav", inbox)
    assert meta["durationMs"] == 2000 and meta["audio"] == "source.wav"


# ---- 计划 2 整体 review 之后补的 ----

def test_importing_a_local_file_again_after_it_was_moved_gives_the_same_media(tmp_path):
    """第一次导入把文件移出了 inbox。重试(或者子任务被重复投递)再用同一个文件名来导入,不能因为文件不在了就失败。"""
    s, inbox = Store(tmp_path), tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "lec 1.mp4").write_bytes(b"LOCAL")
    first = import_path(s, "lec 1.mp4", inbox, probe=lambda p: 12.0)
    again = import_path(s, "lec 1.mp4", inbox, probe=lambda p: 12.0)
    assert again["cached"] is True and again["mediaKey"] == first["mediaKey"] and again["title"] == "lec 1"
    assert import_path(s, str(inbox / "lec 1.mp4"), inbox, probe=lambda p: 12.0)["mediaKey"] == first["mediaKey"]
    with pytest.raises(ValueError):
        import_path(s, "never-imported.mp4", inbox, probe=lambda p: 12.0)


def test_a_new_file_under_an_old_name_is_imported_as_new_media(tmp_path):
    s, inbox = Store(tmp_path), tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "week1.mp4").write_bytes(b"FIRST")
    first = import_path(s, "week1.mp4", inbox, probe=lambda p: 1.0)
    (inbox / "week1.mp4").write_bytes(b"SECOND, RE-RECORDED")
    second = import_path(s, "week1.mp4", inbox, probe=lambda p: 2.0)
    assert second["cached"] is False and second["mediaKey"] != first["mediaKey"]
    assert import_path(s, "week1.mp4", inbox, probe=lambda p: 2.0)["mediaKey"] == second["mediaKey"]
