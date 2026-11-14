"""Slides: decode key frames only, OCR a frame only when the picture changed."""
from pathlib import Path
from typing import Iterator, Protocol

import av
import numpy as np
from PIL import Image

from .stats import Stats
from .store import NotFound, Store

MIN_TEXT = 10   # frames whose text is shorter than this carry nothing worth indexing


def keyframes(video: Path) -> Iterator[tuple[int, Image.Image]]:
    with av.open(str(video)) as container:
        if not container.streams.video:
            return   # an audio-only file imported from the inbox: no picture, no frames
        stream = container.streams.video[0]
        stream.codec_context.skip_frame = "NONKEY"   # key frames only: an hour of lecture takes seconds
        for frame in container.decode(stream):
            yield round(float(frame.pts * stream.time_base) * 1000), frame.to_image()


def small(img: Image.Image) -> np.ndarray:
    return np.asarray(img.convert("L").resize((160, 90)), dtype=np.int16)


def changed(a: np.ndarray, b: np.ndarray) -> bool:
    """Share of pixels whose brightness moved by more than 24. Measured on a synthetic lecture:
    same slide = 0.0, a text-only slide change = 0.022-0.030, so 0.003 separates them with margin."""
    return float((np.abs(a - b) > 24).mean()) > 0.003


class Ocr(Protocol):
    def read(self, img: Image.Image) -> str:
        """Text on the image, lines joined with a newline."""


class RapidOcr:
    def __init__(self):
        from rapidocr_onnxruntime import RapidOCR   # lazy: loads ONNX models

        self._engine = RapidOCR()

    def read(self, img: Image.Image) -> str:
        result, _ = self._engine(np.asarray(img.convert("RGB")))
        return "\n".join(line[1] for line in result or [])


def frames_media(store: Store, key: str, ocr: Ocr, stats: Stats) -> dict:
    # ponytail: 拍摄讲台的视频几乎每个关键帧都算"变了",OCR 次数接近关键帧数;真遇到再加每分钟上限
    meta = store.read_json(key, "meta.json")
    if meta is None:
        raise NotFound(key)
    with store.lock("frames:" + key):   # a retry that arrives while the first run is still going waits
        if (doc := store.read_json(key, "frames.json")) is not None:
            return {"mediaKey": key, "cached": True, "frames": len(doc["frames"])}
        frames: list[dict] = []
        if meta["video"]:
            (store.dir(key) / "frames").mkdir(exist_ok=True)
            seen = text_kept = None
            for ts_ms, img in keyframes(store.dir(key) / meta["video"]):
                now = small(img)
                if seen is not None and not changed(seen, now):
                    continue
                seen = now   # compare against the last frame we OCRed, so slow fades still add up to a change
                text = ocr.read(img).strip()
                if len(text) < MIN_TEXT or text == text_kept:
                    continue
                text_kept = text
                name = f"frames/{ts_ms:09d}.jpg"
                img.save(store.dir(key) / name, quality=80)
                frames.append({"tsMs": ts_ms, "file": name, "text": text})
        store.write_json(key, "frames.json", {"frames": frames})   # written last: a failed run leaves no cache
        stats.ocr_runs += 1
        return {"mediaKey": key, "cached": False, "frames": len(frames)}
