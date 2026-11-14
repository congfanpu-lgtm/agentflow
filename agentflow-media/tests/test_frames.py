import shutil
import threading
import wave

import av
import pytest
from PIL import Image, ImageDraw

from app.frames import RapidOcr, changed, frames_media, keyframes, small
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
    fs = s.read_json(KEY, "frames.json")["frames"]
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
    assert s.read_json(KEY, "frames.json") == {"frames": []}


def test_identical_text_is_kept_once(tmp_path, video):
    assert frames_media(media(tmp_path, video), KEY, CountingOcr("the same text on every slide"), Stats())["frames"] == 1


def test_media_without_video_has_no_frames(tmp_path):
    s = Store(tmp_path)
    s.write_json(KEY, "meta.json", {"mediaKey": KEY, "audio": "audio.m4a", "video": None, "durationMs": 1})
    assert frames_media(s, KEY, CountingOcr("x" * 20), Stats()) == {"mediaKey": KEY, "cached": False, "frames": 0}
    assert s.read_json(KEY, "frames.json") == {"frames": []}


def test_unreadable_video_fails_without_writing_frames(tmp_path):
    s = Store(tmp_path)
    s.write_json(KEY, "meta.json", {"mediaKey": KEY, "audio": "a", "video": "video.mp4", "durationMs": 1})
    (s.dir(KEY) / "video.mp4").write_bytes(b"not a video")
    with pytest.raises(Exception):
        frames_media(s, KEY, CountingOcr("x" * 20), Stats())
    assert s.read_json(KEY, "frames.json") is None


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
    assert s.read_json(KEY, "frames.json") == {"frames": []}


def test_concurrent_calls_for_the_same_media_ocr_once(tmp_path, video):
    s, ocr, stats = media(tmp_path, video), CountingOcr("some slide text here"), Stats()
    threads = [threading.Thread(target=frames_media, args=(s, KEY, ocr, stats)) for _ in range(3)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert ocr.calls == 3 and stats.ocr_runs == 1
