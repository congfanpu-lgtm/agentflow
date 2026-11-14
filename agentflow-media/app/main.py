"""HTTP surface of the media service. Run as ONE process: the GPU lock and the counters live in memory."""
import re
from functools import lru_cache

from fastapi import Depends, FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, model_validator

from . import config
from .asr import Transcriber, WhisperTranscriber, transcribe_media, transcript_file
from .frames import Ocr, RapidOcr, frames_media
from .importer import FetchError, Fetcher, import_path, import_url, ytdlp_fetch
from .stats import Stats
from .store import Store

app = FastAPI(title="agentflow-media")
_FRAME_NAME = re.compile(r"\d{9}\.jpg")


@lru_cache
def get_store() -> Store:
    return Store(config.MEDIA_ROOT)


@lru_cache
def get_stats() -> Stats:
    return Stats()


def get_fetcher() -> Fetcher:
    return ytdlp_fetch


@lru_cache
def _whisper() -> WhisperTranscriber:
    return WhisperTranscriber(config.ASR_MODEL, config.ASR_DEVICE, config.ASR_COMPUTE, config.ASR_BATCH)


@lru_cache
def _rapid() -> RapidOcr:
    return RapidOcr()


class _LazyTranscriber:
    """Loads the model on the first real transcription, so a cache hit or a 404 never touches the GPU."""

    def transcribe(self, audio):
        return _whisper().transcribe(audio)


class _LazyOcr:
    def read(self, img):
        return _rapid().read(img)


def get_transcriber() -> Transcriber:
    return _LazyTranscriber()


def get_ocr() -> Ocr:
    return _LazyOcr()


# ponytail: every ValueError is a 400 and every KeyError a 404, including one raised by a bug.
# Switch to dedicated exception classes if a masked bug ever costs time.
@app.exception_handler(ValueError)
def _bad_request(_, e: ValueError):
    return JSONResponse({"detail": str(e)}, 400)


@app.exception_handler(KeyError)
def _not_found(_, e: KeyError):
    return JSONResponse({"detail": f"not found: {e.args[0] if e.args else ''}"}, 404)


@app.exception_handler(FetchError)
def _bad_gateway(_, e: FetchError):
    return JSONResponse({"detail": str(e)}, 502)


@app.exception_handler(Exception)
def _failed(_, e: Exception):
    return JSONResponse({"detail": f"{type(e).__name__}: {e}"}, 500)   # the caller records why a step failed


class ImportRequest(BaseModel):
    url: str | None = None
    path: str | None = None

    @model_validator(mode="after")
    def _exactly_one(self):
        if (self.url is None) == (self.path is None):
            raise ValueError("give exactly one of url or path")
        return self


def _found(doc: dict | None, what: str) -> dict:
    if doc is None:
        raise KeyError(what)
    return doc


# Plain `def` routes on purpose: FastAPI runs them in a thread pool, so a minute-long transcription
# does not block /health or another lecture's import.

@app.get("/health")
def health():
    return {"status": "ok", "asrModel": config.ASR_MODEL}


@app.get("/stats")
def stats(s: Stats = Depends(get_stats)):
    return {"downloads": s.downloads, "asrRuns": s.asr_runs, "ocrRuns": s.ocr_runs}


@app.post("/media/import")
def import_media(req: ImportRequest, store: Store = Depends(get_store), fetch: Fetcher = Depends(get_fetcher),
                 s: Stats = Depends(get_stats)):
    if req.url is not None:
        return import_url(store, req.url, fetch, s)
    return import_path(store, req.path, store.root / "inbox")


@app.post("/media/{key}/transcribe")
def transcribe(key: str, store: Store = Depends(get_store), t: Transcriber = Depends(get_transcriber),
               s: Stats = Depends(get_stats)):
    return transcribe_media(store, key, t, config.ASR_MODEL, s)


@app.post("/media/{key}/frames")
def frames(key: str, store: Store = Depends(get_store), ocr: Ocr = Depends(get_ocr), s: Stats = Depends(get_stats)):
    return frames_media(store, key, ocr, s)


@app.get("/media/{key}/meta")
def get_meta(key: str, store: Store = Depends(get_store)):
    return _found(store.read_json(key, "meta.json"), key)


@app.get("/media/{key}/transcript")
def get_transcript(key: str, store: Store = Depends(get_store)):
    return _found(store.read_json(key, transcript_file(config.ASR_MODEL)), f"transcript of {key}")


@app.get("/media/{key}/frames")
def get_frames(key: str, store: Store = Depends(get_store)):
    return _found(store.read_json(key, "frames.json"), f"frames of {key}")


@app.get("/media/{key}/frames/{name}")
def get_frame(key: str, name: str, store: Store = Depends(get_store)):
    if not _FRAME_NAME.fullmatch(name):   # the name becomes a path: only what frames_media writes is served
        raise ValueError(f"bad frame name: {name!r}")
    path = store.dir(key) / "frames" / name
    if not path.is_file():
        raise KeyError(f"frame {name} of {key}")
    return FileResponse(path, media_type="image/jpeg")
