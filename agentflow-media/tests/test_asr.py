import sys
import threading
import types
from pathlib import Path

import pytest

from app.asr import WhisperTranscriber, transcribe_media
from app.stats import Stats
from app.store import Store
from tests.fakes import KEY, FakeTranscriber

K2 = "f" * 32


def media(tmp_path, key=KEY):
    s = Store(tmp_path)
    s.write_json(key, "meta.json", {"mediaKey": key, "audio": "audio.m4a", "video": None, "durationMs": 9000})
    (s.dir(key) / "audio.m4a").write_bytes(b"a")
    return s


def run_all(threads):
    [t.start() for t in threads]
    [t.join() for t in threads]


def test_transcribe_writes_transcript(tmp_path):
    s, t, stats = media(tmp_path), FakeTranscriber(), Stats()
    out = transcribe_media(s, KEY, t, "large-v3", stats)
    assert out == {"mediaKey": KEY, "model": "large-v3", "cached": False, "utterances": 2, "durationMs": 9000}
    doc = s.read_json(KEY, "transcript.large-v3.json")
    assert doc["model"] == "large-v3" and doc["language"] == "en"
    assert doc["utterances"][1] == {"startMs": 4200, "endMs": 9000, "text": "Today: agents."}
    assert stats.asr_runs == 1


def test_second_call_uses_the_cache(tmp_path):
    s, t, stats = media(tmp_path), FakeTranscriber(), Stats()
    transcribe_media(s, KEY, t, "large-v3", stats)
    again = transcribe_media(s, KEY, t, "large-v3", stats)
    assert again["cached"] is True and again["utterances"] == 2 and t.calls == 1 and stats.asr_runs == 1


def test_another_model_is_a_cache_miss(tmp_path):
    s, t, stats = media(tmp_path), FakeTranscriber(), Stats()
    transcribe_media(s, KEY, t, "large-v3", stats)
    assert transcribe_media(s, KEY, t, "large-v3-turbo", stats)["cached"] is False and t.calls == 2


def test_failure_leaves_no_transcript(tmp_path):
    s, stats = media(tmp_path), Stats()
    with pytest.raises(RuntimeError):
        transcribe_media(s, KEY, FakeTranscriber(fail=True), "large-v3", stats)
    assert s.read_json(KEY, "transcript.large-v3.json") is None and stats.asr_runs == 0


def test_silent_media_gives_an_empty_transcript(tmp_path):
    s = media(tmp_path)
    out = transcribe_media(s, KEY, FakeTranscriber(utterances=[]), "large-v3", Stats())
    assert out["utterances"] == 0 and s.read_json(KEY, "transcript.large-v3.json")["utterances"] == []


def test_only_one_transcription_runs_at_a_time(tmp_path):
    s = media(tmp_path)
    media(tmp_path, K2)
    t, stats = FakeTranscriber(delay=0.2), Stats()
    run_all([threading.Thread(target=transcribe_media, args=(s, k, t, "large-v3", stats)) for k in (KEY, K2)])
    assert t.calls == 2 and t.max_active == 1


def test_concurrent_calls_for_the_same_media_transcribe_once(tmp_path):
    s, t, stats = media(tmp_path), FakeTranscriber(delay=0.2), Stats()
    run_all([threading.Thread(target=transcribe_media, args=(s, KEY, t, "large-v3", stats)) for _ in range(3)])
    assert t.calls == 1 and stats.asr_runs == 1


def test_unknown_media_raises_key_error(tmp_path):
    with pytest.raises(KeyError):
        transcribe_media(Store(tmp_path), KEY, FakeTranscriber(), "large-v3", Stats())


# ---- 整体 review 之后补的 ----

def test_a_model_name_with_a_slash_still_caches(tmp_path):
    s, t, stats = media(tmp_path), FakeTranscriber(), Stats()
    tag = "Systran/faster-whisper-large-v3"
    assert transcribe_media(s, KEY, t, tag, stats)["cached"] is False
    assert transcribe_media(s, KEY, t, tag, stats)["cached"] is True and t.calls == 1


def test_whisper_is_asked_for_timestamps_inside_each_chunk(monkeypatch):
    """批处理管线默认不要时间戳:一个约 30 秒的静音检测块就是一条,从句子中间开始和结束。"""
    seen = {}
    Seg = types.SimpleNamespace

    class Pipe:
        def __init__(self, model):
            pass

        def transcribe(self, audio, **kw):
            seen.update(kw)
            return iter([Seg(start=0.0, end=2.5, text=" Hello. "), Seg(start=2.5, end=2.6, text="  "),
                         Seg(start=2.6, end=7.04, text="Next sentence.")]), Seg(language="en")

    monkeypatch.setitem(sys.modules, "faster_whisper",
                        types.SimpleNamespace(BatchedInferencePipeline=Pipe, WhisperModel=lambda *a, **k: None))
    lang, utts = WhisperTranscriber("large-v3", "cpu", "int8", 16).transcribe(Path("a.m4a"))
    assert seen["without_timestamps"] is False and seen["batch_size"] == 16
    assert lang == "en" and utts == [{"startMs": 0, "endMs": 2500, "text": "Hello."},
                                     {"startMs": 2600, "endMs": 7040, "text": "Next sentence."}]
