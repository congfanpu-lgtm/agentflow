import threading
import time

import pytest

from app.importer import FetchError, check_url, import_path, import_url
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
