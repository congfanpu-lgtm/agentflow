import time

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


class FakeTranscriber:
    DEFAULT = [{"startMs": 0, "endMs": 4200, "text": "Welcome to lecture one."},
               {"startMs": 4200, "endMs": 9000, "text": "Today: agents."}]

    def __init__(self, delay=0.0, fail=False, utterances=None):
        self.calls = self.active = self.max_active = 0
        self.delay, self.fail = delay, fail
        self.utterances = self.DEFAULT if utterances is None else utterances

    def transcribe(self, audio):
        self.calls += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            time.sleep(self.delay)
            if self.fail:
                raise RuntimeError("CUDA out of memory")
            return "en", list(self.utterances)
        finally:
            self.active -= 1
