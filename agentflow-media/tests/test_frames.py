import shutil
import threading
import wave

import av
import numpy as np
import pytest
from PIL import Image, ImageDraw

from app.frames import FRAMES_FILE, RapidOcr, changed, frames_media, keyframes, noisy_blocks, same_slide, small
from app.stats import Stats
from app.store import Store
from tests.fakes import KEY, CountingOcr

SLIDES = ["Gradient descent: theta = theta - eta * grad", "ReAct: reason then act with tools",
          "Evaluation of LLM agents"]


def slide(text):
    img = Image.new("RGB", (1280, 720), "white")
    ImageDraw.Draw(img).text((80, 120), text, fill="black", font_size=48)
    return img


@pytest.fixture(scope="module")
def video(tmp_path_factory):
    """60 秒,每 20 秒换一页,每 2 秒一个关键帧。"""
    path = tmp_path_factory.mktemp("v") / "video.mp4"
    out = av.open(str(path), "w")
    st = out.add_stream("libx264", rate=2)
    st.width, st.height, st.pix_fmt = 1280, 720, "yuv420p"
    st.codec_context.gop_size = 4
    for i in range(120):
        for p in st.encode(av.VideoFrame.from_image(slide(SLIDES[i // 40]))):
            out.mux(p)
    for p in st.encode():
        out.mux(p)
    out.close()
    return path


def media(tmp_path, video):
    s = Store(tmp_path)
    s.write_json(KEY, "meta.json", {"mediaKey": KEY, "audio": "audio.m4a", "video": "video.mp4", "durationMs": 60000})
    shutil.copy(video, s.dir(KEY) / "video.mp4")
    return s


def test_keyframes_are_decoded_with_timestamps(video):
    ks = list(keyframes(video))
    assert len(ks) == 30 and ks[0][0] == 0 and ks[1][0] == 2000 and ks[0][1].size == (1280, 720)


def test_changed_detects_a_text_only_slide_change():
    a, b = small(slide(SLIDES[0])), small(slide(SLIDES[1]))
    assert not changed(a, a.copy()) and changed(a, b)


def test_one_frame_per_slide_with_its_text(tmp_path, video):
    s, stats = media(tmp_path, video), Stats()
    out = frames_media(s, KEY, RapidOcr(), stats)
    assert out == {"mediaKey": KEY, "cached": False, "frames": 3}
    fs = s.read_json(KEY, FRAMES_FILE)["frames"]
    assert [f["tsMs"] for f in fs] == [0, 20000, 40000]
    assert [f["file"] for f in fs] == ["frames/000000000.jpg", "frames/000020000.jpg", "frames/000040000.jpg"]
    assert "Gradient descent" in fs[0]["text"] and "ReAct" in fs[1]["text"] and "Evaluation" in fs[2]["text"]
    assert all((s.dir(KEY) / f["file"]).is_file() for f in fs) and stats.ocr_runs == 1


def test_second_call_uses_the_cache(tmp_path, video):
    s, ocr, stats = media(tmp_path, video), CountingOcr("some slide text here"), Stats()
    frames_media(s, KEY, ocr, stats)
    n = ocr.calls
    assert frames_media(s, KEY, ocr, stats)["cached"] is True and ocr.calls == n and stats.ocr_runs == 1


def test_only_changed_keyframes_are_ocred(tmp_path, video):
    ocr = CountingOcr("some slide text here")
    frames_media(media(tmp_path, video), KEY, ocr, Stats())
    assert ocr.calls == 3                       # 30 个关键帧里只有 3 次画面变化


def test_frames_with_too_little_text_are_dropped(tmp_path, video):
    s = media(tmp_path, video)
    assert frames_media(s, KEY, CountingOcr("  ab \n"), Stats())["frames"] == 0
    assert s.read_json(KEY, FRAMES_FILE)["frames"] == []


def test_identical_text_is_kept_once(tmp_path, video):
    assert frames_media(media(tmp_path, video), KEY, CountingOcr("the same text on every slide"), Stats())["frames"] == 1


def test_media_without_video_has_no_frames(tmp_path):
    s = Store(tmp_path)
    s.write_json(KEY, "meta.json", {"mediaKey": KEY, "audio": "audio.m4a", "video": None, "durationMs": 1})
    assert frames_media(s, KEY, CountingOcr("x" * 20), Stats()) == {"mediaKey": KEY, "cached": False, "frames": 0}
    assert s.read_json(KEY, FRAMES_FILE)["frames"] == []


def test_unreadable_video_fails_without_writing_frames(tmp_path):
    s = Store(tmp_path)
    s.write_json(KEY, "meta.json", {"mediaKey": KEY, "audio": "a", "video": "video.mp4", "durationMs": 1})
    (s.dir(KEY) / "video.mp4").write_bytes(b"not a video")
    with pytest.raises(Exception):
        frames_media(s, KEY, CountingOcr("x" * 20), Stats())
    assert s.read_json(KEY, FRAMES_FILE) is None


def test_unknown_media_raises_key_error(tmp_path):
    with pytest.raises(KeyError):
        frames_media(Store(tmp_path), KEY, CountingOcr("x" * 20), Stats())


def test_file_without_a_video_stream_has_no_frames(tmp_path):
    """本地导入的纯音频文件:meta 里 video 和 audio 是同一个文件,但里面没有画面。"""
    s = Store(tmp_path)
    s.write_json(KEY, "meta.json", {"mediaKey": KEY, "audio": "source.wav", "video": "source.wav", "durationMs": 1000})
    with wave.open(str(s.dir(KEY) / "source.wav"), "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(16000)
        w.writeframes(b"\0\0" * 16000)
    assert frames_media(s, KEY, CountingOcr("x" * 20), Stats())["frames"] == 0
    assert s.read_json(KEY, FRAMES_FILE)["frames"] == []


def test_concurrent_calls_for_the_same_media_ocr_once(tmp_path, video):
    s, ocr, stats = media(tmp_path, video), CountingOcr("some slide text here"), Stats()
    threads = [threading.Thread(target=frames_media, args=(s, KEY, ocr, stats)) for _ in range(3)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert ocr.calls == 3 and stats.ocr_runs == 1


# ---- 计划 2 任务 1:截帧去重 ----

def encode_with_webcam(path, lower_half_period):
    """三页幻灯片 + 右上角 190×100 的"讲者摄像头"小窗。上半部分每个关键帧都变;
    下半部分每 lower_half_period 个关键帧变一次(真实录像里肩膀比脸动得少)。"""
    out = av.open(str(path), "w")
    st = out.add_stream("libx264", rate=2)
    st.width, st.height, st.pix_fmt = 1280, 720, "yuv420p"
    st.codec_context.gop_size = 4
    for i in range(120):
        img = slide(SLIDES[i // 40])
        k = i // 4                                         # 每 4 帧一个关键帧
        top, bottom = (40 if k % 2 == 0 else 200), (40 if (k // lower_half_period) % 2 == 0 else 200)
        img.paste((top, top, top), (1090, 0, 1280, 50))
        img.paste((bottom, bottom, bottom), (1090, 50, 1280, 100))
        for p in st.encode(av.VideoFrame.from_image(img)):
            out.mux(p)
    for p in st.encode():
        out.mux(p)
    out.close()
    return path


@pytest.fixture(scope="module")
def webcam_video(tmp_path_factory):
    return encode_with_webcam(tmp_path_factory.mktemp("w") / "video.mp4", lower_half_period=1)


@pytest.fixture(scope="module")
def calm_webcam_video(tmp_path_factory):
    return encode_with_webcam(tmp_path_factory.mktemp("c") / "video.mp4", lower_half_period=3)


def test_the_calmer_edge_of_the_webcam_is_ignored_too(tmp_path, calm_webcam_video):
    """真实的那一讲里,小窗下半部分只在 38% 到 47% 的相邻关键帧之间有变化,没到一半,
    却触发了 85% 的多余识别(docs/benchmarks.md 第 7 节)。"""
    mask = noisy_blocks(calm_webcam_video)
    assert mask[:2, 14:].all() and not mask[3:, :].any() and not mask[:, :12].any()
    s, ocr = media(tmp_path, calm_webcam_video), CountingOcr("some slide text here")
    frames_media(s, KEY, ocr, Stats())
    assert ocr.calls == 3


def test_noisy_blocks_marks_only_the_webcam_corner(webcam_video, video):
    mask = noisy_blocks(webcam_video)
    assert mask.shape == (9, 16) and mask[0, 14:].all()
    assert mask.sum() <= 12 and not mask[3:, :].any() and not mask[:, :12].any()   # 小窗 6 块 + 一圈边
    assert not noisy_blocks(video).any()


def test_webcam_overlay_does_not_trigger_ocr(tmp_path, webcam_video):
    s, ocr = media(tmp_path, webcam_video), CountingOcr("some slide text here")
    frames_media(s, KEY, ocr, Stats())
    doc = s.read_json(KEY, FRAMES_FILE)
    assert ocr.calls == 3 and doc["keyframes"] == 30 and doc["ocred"] == 3


def test_slides_are_still_found_next_to_a_webcam(tmp_path, webcam_video):
    s = media(tmp_path, webcam_video)
    assert frames_media(s, KEY, RapidOcr(), Stats())["frames"] == 3
    fs = s.read_json(KEY, FRAMES_FILE)["frames"]
    assert [f["tsMs"] for f in fs] == [0, 20000, 40000]
    assert "Gradient descent" in fs[0]["text"] and "ReAct" in fs[1]["text"] and "Evaluation" in fs[2]["text"]


def test_same_slide_ignores_ocr_spacing_jitter():
    a = "YannDubois\nIntroduction to training\nLLMs for Al agents\nLLM AgentsMOOC series\nYannDubois|Sep.15th 2025"
    b = "YannDub\nIntroduction to training\nLLMs for Al agents\nLLM Agents MOOC series\nYannDubois |Sep.15th 2025"
    assert same_slide(a, b)
    assert not same_slide(a, "ChatGPT\nLLMs\nWhat are you?\nLLMs & chatbots took over the world")


def test_repeated_ocr_of_one_slide_is_kept_once(tmp_path, video, monkeypatch):
    class JitterOcr:
        calls = 0

        def read(self, img):
            self.calls += 1
            return "LLM Agents MOOC series" if self.calls % 2 else "LLM AgentsMOOC  series"

    monkeypatch.setattr("app.frames.changed", lambda *a, **k: True)     # 每个关键帧都送去识别
    s, ocr = media(tmp_path, video), JitterOcr()
    assert frames_media(s, KEY, ocr, Stats())["frames"] == 1 and ocr.calls == 30


def test_a_rerun_does_not_keep_images_of_an_earlier_run(tmp_path, video):
    s = media(tmp_path, video)
    (s.dir(KEY) / "frames").mkdir()
    (s.dir(KEY) / "frames" / "999999000.jpg").write_bytes(b"stale")
    frames_media(s, KEY, CountingOcr("some slide text here"), Stats())
    assert not (s.dir(KEY) / "frames" / "999999000.jpg").exists()
