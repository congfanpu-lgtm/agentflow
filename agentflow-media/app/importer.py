"""Bring a lecture into the store: from a YouTube link, or from a file dropped into the inbox."""
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from urllib.parse import urlsplit

from .config import ALLOWED_HOSTS
from .stats import Stats
from .store import Store, file_key


@dataclass
class Fetched:
    audio: Path
    video: Path | None
    title: str
    duration_s: float
    source_url: str


Fetcher = Callable[[str, Path], Fetched]


class FetchError(RuntimeError):
    pass


def check_url(url: str) -> None:
    """The service downloads whatever it is given, so it is only given YouTube (no SSRF, no file://)."""
    u = urlsplit(url)   # itself raises ValueError on malformed input
    if u.scheme not in ("http", "https") or u.hostname not in ALLOWED_HOSTS:
        raise ValueError(f"only YouTube links are accepted, got: {url!r}")


def _adopt(store: Store, tmp: Path, meta: dict) -> dict:
    # meta.json goes in before the rename, so a media directory never exists without its meta
    (tmp / "meta.json").write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    fresh = store.adopt(tmp, meta["mediaKey"])
    return {**(meta if fresh else store.read_json(meta["mediaKey"], "meta.json")), "cached": not fresh}


def import_url(store: Store, url: str, fetch: Fetcher, stats: Stats) -> dict:
    check_url(url)
    with store.lock("url:" + url):   # the same link requested twice at once downloads once
        key = store.url_key(url)
        if key and (meta := store.read_json(key, "meta.json")):
            return {**meta, "cached": True}
        tmp = store.new_tmp()
        try:
            got = fetch(url, tmp)
        except Exception as e:
            shutil.rmtree(tmp, ignore_errors=True)
            raise FetchError(f"download failed: {e}") from e
        stats.downloads += 1
        key = file_key(got.audio)
        meta = _adopt(store, tmp, {
            "mediaKey": key, "title": got.title, "durationMs": round(got.duration_s * 1000),
            "sourceUrl": got.source_url, "audio": got.audio.name,
            "video": got.video.name if got.video else None})
        store.remember_url(url, key)
        return meta


def media_duration(path: Path) -> float:
    import av   # lazy: only the real probe needs it

    with av.open(str(path)) as c:
        return (c.duration or 0) / av.time_base


def import_path(store: Store, path: str, inbox: Path, probe: Callable[[Path], float] = media_duration) -> dict:
    root = inbox.resolve()
    src = (root / path).resolve()   # a bare name means "in the inbox"; resolve() follows symlinks and ".."
    if not src.is_relative_to(root) or not src.is_file():
        raise ValueError(f"not a file inside the inbox: {path!r}")
    key = file_key(src)
    if meta := store.read_json(key, "meta.json"):
        return {**meta, "cached": True}   # the duplicate stays where it is: never delete the user's file
    try:
        duration_s = probe(src)
    except Exception as e:
        raise ValueError(f"not a readable media file: {path!r} ({e})") from e
    name = "source" + src.suffix
    tmp = store.new_tmp()
    shutil.move(src, tmp / name)
    return _adopt(store, tmp, {
        "mediaKey": key, "title": src.stem, "durationMs": round(duration_s * 1000),
        "sourceUrl": None, "audio": name, "video": name})


def ytdlp_fetch(url: str, dest: Path) -> Fetched:
    """Audio and picture are fetched as two files and never merged, so no ffmpeg is needed."""
    import yt_dlp   # lazy: tests never download

    def grab(fmt: str, name: str):
        opts = {"format": fmt, "outtmpl": str(dest / f"{name}.%(ext)s"),
                "quiet": True, "noprogress": True, "noplaylist": True}
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
        return Path(info["requested_downloads"][0]["filepath"]), info

    audio, info = grab("bestaudio[ext=m4a]/bestaudio", "audio")
    try:
        video, _ = grab("bestvideo[height<=720][ext=mp4]/bestvideo[height<=720]", "video")
    except yt_dlp.utils.DownloadError:
        video = None   # audio-only source: the frames step then yields an empty list
    return Fetched(audio, video, info.get("title") or info["id"],
                   float(info.get("duration") or 0), info.get("webpage_url") or url)
