from app.importer import Fetched

URL = "https://www.youtube.com/watch?v=abc123"
KEY = "0123456789abcdef0123456789abcdef"


def fake_fetch(calls, audio=b"AUDIO", video=b"VIDEO"):
    def fetch(url, dest):
        calls.append(url)
        (dest / "audio.m4a").write_bytes(audio)
        v = None
        if video is not None:
            v = dest / "video.mp4"
            v.write_bytes(video)
        return Fetched(audio=dest / "audio.m4a", video=v, title="Lecture 1", duration_s=3600.5, source_url=url)
    return fetch
