"""Slides: decode key frames only, OCR a frame only when the picture changed."""
import difflib
import shutil
from pathlib import Path
from typing import Iterator, Protocol

import av
import numpy as np
from PIL import Image

from .stats import Stats
from .store import NotFound, Store

MIN_TEXT = 10   # frames whose text is shorter than this carry nothing worth indexing
# The file name carries the version of the algorithm: bumping it makes older results count as "not done yet".
# v2 = change detection ignores always-moving regions, near-duplicate text removed.
# v3 = of several similar readings of one slide the fullest is kept (v2 kept the first).
FRAMES_FILE = "frames.v3.json"
ROWS, COLS, BLOCK = 9, 16, 10   # the 160x90 thumbnail as 10x10-pixel blocks
BLOCK_SHARE = 0.05              # a block "changed" when more than 5% of its pixels moved
NOISY_SHARE = 0.5               # a block that changes between more than half of all key-frame pairs is always moving
MIN_BLOCKS = 2                  # the picture "changed" when at least this many steady blocks changed
SAME_TEXT = 0.8                 # two OCR results this similar are the same slide read twice


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


def block_changes(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """(9, 16) booleans: which blocks changed between two thumbnails (pixel brightness moved by more than 24)."""
    moved = np.abs(a - b) > 24
    return moved.reshape(ROWS, BLOCK, COLS, BLOCK).mean(axis=(1, 3)) > BLOCK_SHARE


def noisy_blocks(video: Path) -> np.ndarray:
    """(9, 16) booleans: blocks that keep moving through the whole recording, such as a speaker's webcam in a
    corner. On the first real lecture that corner made 76% of key frames count as "changed"."""
    counts, pairs, prev = np.zeros((ROWS, COLS)), 0, None
    for _, img in keyframes(video):
        now = small(img)
        if prev is not None:
            counts += block_changes(prev, now)
            pairs += 1
        prev = now
    if pairs < 4:   # too short to tell movement from slide changes
        return np.zeros((ROWS, COLS), dtype=bool)
    moving = counts / pairs > NOISY_SHARE
    # One block of margin around the moving region is ignored as well: an overlay's edge runs through the middle
    # of blocks, and its calmer part moves less often. Measured on the first real lecture: the lower half of the
    # webcam changed in 38-47% of pairs (under the 50% line) and caused 85% of the wasted OCR runs.
    padded = np.pad(moving, 1)
    return np.logical_or.reduce([padded[r:r + ROWS, c:c + COLS] for r in range(3) for c in range(3)])


def changed(a: np.ndarray, b: np.ndarray, ignore: np.ndarray | None = None) -> bool:
    blocks = block_changes(a, b)
    if ignore is not None:
        blocks &= ~ignore
    return int(blocks.sum()) >= MIN_BLOCKS


def same_slide(a: str, b: str) -> bool:
    """OCR of an unchanged slide differs in spacing from read to read ("AgentsMOOC" / "Agents MOOC")."""
    x, y = "".join(a.split()).lower(), "".join(b.split()).lower()
    return difflib.SequenceMatcher(None, x, y, autojunk=False).ratio() >= SAME_TEXT


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
    # ponytail: 一直在动的块是按整段视频统计的。镜头对着讲台超过一半时长的录像,所有块都会被当成"一直在动",
    # 幻灯片就找不到了;真遇到再改成按时间窗口统计
    meta = store.read_json(key, "meta.json")
    if meta is None:
        raise NotFound(key)
    with store.lock("frames:" + key):   # a retry that arrives while the first run is still going waits
        if (doc := store.read_json(key, FRAMES_FILE)) is not None:
            return {"mediaKey": key, "cached": True, "frames": len(doc["frames"])}
        frames: list[dict] = []
        n_key = n_ocr = 0
        if meta["video"]:
            video = store.dir(key) / meta["video"]
            ignore = noisy_blocks(video)   # first pass over the key frames (seconds); an unreadable video fails here
            shutil.rmtree(store.dir(key) / "frames", ignore_errors=True)   # images of an earlier or failed run
            (store.dir(key) / "frames").mkdir()
            seen = text_kept = None
            for ts_ms, img in keyframes(video):
                n_key += 1
                now = small(img)
                if seen is not None and not changed(seen, now, ignore):
                    continue
                seen = now   # compare against the last frame we OCRed, so slow fades still add up to a change
                n_ocr += 1
                text = ocr.read(img).strip()
                if len(text) < MIN_TEXT:
                    continue
                if text_kept is not None and same_slide(text, text_kept):
                    # The same slide read again. If there is more on it now (bullets appearing one by one),
                    # keep the fuller reading and picture, at the time the slide first appeared:
                    # keeping only the first reading would leave the last bullets out of the index.
                    if len(text) > len(text_kept):
                        text_kept = frames[-1]["text"] = text
                        img.save(store.dir(key) / frames[-1]["file"], quality=80)
                    continue
                text_kept = text
                name = f"frames/{ts_ms:09d}.jpg"
                img.save(store.dir(key) / name, quality=80)
                frames.append({"tsMs": ts_ms, "file": name, "text": text})
        # written last: a failed run leaves no cache
        store.write_json(key, FRAMES_FILE, {"frames": frames, "keyframes": n_key, "ocred": n_ocr})
        stats.ocr_runs += 1
        return {"mediaKey": key, "cached": False, "frames": len(frames)}
