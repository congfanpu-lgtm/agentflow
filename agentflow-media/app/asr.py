"""Speech to text. One lecture = one call on the whole file; the result is cached per model."""
import re
import threading
from pathlib import Path
from typing import Protocol

from .stats import Stats
from .store import NotFound, Store

# One GPU, one transcription at a time: measured on the RTX 5080 (design doc section 10), extra workers
# give no speed-up and push VRAM to the limit. In-process lock, so the service must run as one process.
GPU_LOCK = threading.Lock()


class Transcriber(Protocol):
    def transcribe(self, audio: Path) -> tuple[str, list[dict]]:
        """Returns (language code, [{"startMs", "endMs", "text"}, ...])."""


def transcript_file(model_tag: str) -> str:
    # The model is part of the cache key: only a new model re-runs. A tag like "Systran/faster-whisper-large-v3"
    # must stay ONE file name, or the write fails after the whole lecture has been transcribed.
    return "transcript." + re.sub(r"[^A-Za-z0-9._-]", "_", model_tag) + ".json"


def transcribe_media(store: Store, key: str, transcriber: Transcriber, model_tag: str, stats: Stats) -> dict:
    meta = store.read_json(key, "meta.json")
    if meta is None:
        raise NotFound(key)
    name = transcript_file(model_tag)

    def result(doc: dict, cached: bool) -> dict:
        return {"mediaKey": key, "model": model_tag, "cached": cached,
                "utterances": len(doc["utterances"]), "durationMs": meta["durationMs"]}

    if doc := store.read_json(key, name):
        return result(doc, True)
    with GPU_LOCK:
        if doc := store.read_json(key, name):   # someone transcribed this lecture while we waited
            return result(doc, True)
        language, utterances = transcriber.transcribe(store.dir(key) / meta["audio"])
        doc = {"model": model_tag, "language": language, "utterances": utterances}
        store.write_json(key, name, doc)
        stats.asr_runs += 1
    return result(doc, False)


class WhisperTranscriber:
    """The whole file goes to the batched pipeline, which splits on silence itself. Do not pre-chunk:
    fixed chunks were 8-10x slower and hallucinated at chunk ends (design doc section 10)."""

    def __init__(self, model: str, device: str, compute: str, batch: int):
        from faster_whisper import BatchedInferencePipeline, WhisperModel   # lazy: tests never load it

        self._pipe = BatchedInferencePipeline(WhisperModel(model, device=device, compute_type=compute))
        self._batch = batch

    def transcribe(self, audio: Path) -> tuple[str, list[dict]]:
        # without_timestamps=False: the batched pipeline defaults to True, and then one ~30 s silence-detection
        # chunk comes back as ONE utterance that starts and ends mid-sentence. With timestamps, Whisper splits
        # each chunk at its own sentence-ish boundaries, which is what "jump to this sentence" needs.
        segments, info = self._pipe.transcribe(str(audio), batch_size=self._batch, without_timestamps=False)
        return info.language, [
            {"startMs": round(s.start * 1000), "endMs": round(s.end * 1000), "text": s.text.strip()}
            for s in segments if s.text.strip()
        ]
