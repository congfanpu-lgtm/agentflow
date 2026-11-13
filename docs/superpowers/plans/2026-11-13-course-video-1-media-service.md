# 课程录播 · 计划 1：媒体服务（下载、转写、截帧识别） Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增一个跑在 GPU 宿主机上的 Python 服务 `agentflow-media`：给一个 YouTube 链接或本地文件，下载、转写成带时间戳的文本、截出幻灯片并识别上面的文字，结果按内容哈希缓存在磁盘上。

**Architecture:** 一个 FastAPI 服务，和 `agentflow-rag` 同样的目录约定。媒体和产物放在 `MEDIA_ROOT/media/<mediaKey>/`，`mediaKey` 是音频文件内容的哈希，所以同一内容不会重复下载、转写、识别。转写用 faster-whisper 的批处理管线，进程内一把锁保证同一时刻只有一路在 GPU 上跑。下载器、转写器、OCR 都是可替换的依赖，测试用假实现，不联网、不需要 GPU。

**Tech Stack:** Python 3.12、FastAPI、faster-whisper 1.2.1（ctranslate2 4.9.0）、PyAV 15.1.0、rapidocr-onnxruntime 1.4.4、yt-dlp、pytest。

**Spec:** `docs/superpowers/specs/2026-11-13-course-video-qa-design.md`（第 4、6、10 节）。

**这是四份计划里的第一份。** 后三份在本计划做完后再写，因为它们依赖这里定下的接口：
- 计划 2：引擎上的摄入（课程和讲次的表、`LECTURE_INGEST` 任务类型、分段、片段索引、每讲知识点目录、课程库和单讲页）。
- 计划 3：问答 agent、三层核验、问答页。
- 计划 4：评测（题集、各对比系统的跑法、判官校准、统计、报告）和故障演练。

## Global Constraints

- Python 3.12。依赖版本按 `requirements.txt` 固定：`faster-whisper==1.2.1`、`ctranslate2==4.9.0`、`av==15.1.0`、`rapidocr-onnxruntime==1.4.4`。PyAV 必须低于 16（16 起去掉了 faster-whisper 用到的 `metadata_errors` 参数）。
- 服务只监听 `127.0.0.1`，**单进程**（`--workers 1`）：GPU 锁和计数器都在进程内，多进程会失效。
- 同一时刻只允许一个转写在跑（设计文档第 10 节：单卡上并行转写没有收益，显存会涨到上限）。
- 第一版只接受 YouTube 链接和 `MEDIA_ROOT/inbox/` 下的本地文件。B 站、播放列表不在本计划内。
- 所有 JSON 字段用 camelCase（Java 侧消费）。时间一律是整数毫秒，字段名以 `Ms` 结尾。
- 测试不联网、不需要 GPU、不下载模型：下载器和转写器用假实现；OCR 用 rapidocr 轮子自带的模型。
- 媒体目录不进仓库。下载的视频只供个人学习，服务不对外网开放。
- 服务器上长期运行的服务（agentflow、stockbrief、mock-interview、finance tracker、dashboard）一律不动。服务器上只在 `~/project/agentflow-drill`（独立克隆）和 `~/agentflow-media-data` 里操作，端口用 8010。
- 提交信息用 Conventional Commits、中文描述，结尾带仓库现行的两行 trailer（`Co-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>` 和 `Claude-Session: …`）。按 `CLAUDE.md`，每个任务完成后提交并推送到 `main`。
- 本机跑测试：`cd agentflow-media && .venv/bin/python -m pytest -q`。

## 接口约定（计划 2 到 4 依赖这一节）

| 方法和路径 | 请求 | 成功响应 |
|---|---|---|
| `GET /health` | | `{"status":"ok","asrModel":"large-v3"}` |
| `GET /stats` | | `{"downloads":0,"asrRuns":0,"ocrRuns":0}`（进程启动以来真正干活的次数，命中缓存不计） |
| `POST /media/import` | `{"url":"…"}` 或 `{"path":"…"}`，二选一 | meta（见下） |
| `POST /media/{mediaKey}/transcribe` | | `{"mediaKey","model","cached","utterances":数量,"durationMs"}` |
| `POST /media/{mediaKey}/frames` | | `{"mediaKey","cached","frames":数量}` |
| `GET /media/{mediaKey}/meta` | | meta |
| `GET /media/{mediaKey}/transcript` | | `{"model","language","utterances":[{"startMs","endMs","text"}]}` |
| `GET /media/{mediaKey}/frames` | | `{"frames":[{"tsMs","file":"frames/000020000.jpg","text"}]}` |
| `GET /media/{mediaKey}/frames/{name}` | `name` 形如 `000020000.jpg` | JPEG |

- meta：`{"mediaKey","title","durationMs","sourceUrl"(本地文件为 null),"audio","video"(没有画面为 null),"cached"}`。`cached` 只出现在 import 的响应里。
- `mediaKey`：32 位小写十六进制。
- 错误：请求体不合法 422；key、URL、路径、文件名不合规 400；媒体或产物不存在 404；下载失败 502；其他 500。错误体 `{"detail":"…"}`。
- `transcribe` 和 `frames` 是同步的长请求（一小时的课分别约 1 分钟和几分钟），调用方超时要设够。

## Review Focus

设计文档没有逐条写、但用起来最可能出事的五种情况。每条都在对应任务里有测试：

1. **转写或识别到一半失败**（显存不足、进程被杀）：不能留下半个结果文件被下次当成缓存。→ 任务 1 `test_write_json_is_atomic`、任务 3 `test_failure_leaves_no_transcript`、任务 4 `test_unreadable_video_fails_without_writing_frames`。
2. **同一个链接或同一讲被同时请求两次**：只下载一次、只转写一次。→ 任务 2 `test_concurrent_imports_of_one_url_download_once`、任务 3 `test_concurrent_calls_for_the_same_media_transcribe_once`。
3. **key、帧文件名、本地路径里带 `..` 或符号链接**：读不到、写不到媒体目录以外的文件。→ 任务 1 `test_rejects_keys_that_are_not_32_hex`、任务 2 `test_import_path_rejects_outside_inbox_or_missing` 和 `test_symlink_pointing_outside_inbox_is_rejected`、任务 5 `test_frame_file_name_is_validated`。
4. **没有画面、全程没人说话、视频损坏**：前两种给出空列表，不算失败；损坏要报错。→ 任务 3 `test_silent_media_gives_an_empty_transcript`、任务 4 `test_media_without_video_has_no_frames`。
5. **两个转写同时到达**：排队，不同时占显存。→ 任务 3 `test_only_one_transcription_runs_at_a_time`。

---

### Task 1: 脚手架、磁盘存储、计数器

**Files:**
- Create: `agentflow-media/requirements.txt`、`agentflow-media/requirements-gpu.txt`、`agentflow-media/app/__init__.py`（空）、`agentflow-media/app/config.py`、`agentflow-media/app/store.py`、`agentflow-media/app/stats.py`、`agentflow-media/tests/__init__.py`（空）、`agentflow-media/tests/test_store.py`
- Modify: `.gitignore`（加一行 `agentflow-media/data/`）、`.github/workflows/ci.yml`（加 `media` job）

**Interfaces:**
- Produces:
  - `app.store.Store(root: Path)`，方法：`dir(key) -> Path`（key 不合规抛 `ValueError`，不创建目录）、`exists(key) -> bool`、`read_json(key, name) -> dict | None`、`write_json(key, name, data) -> None`（原子写）、`url_key(url) -> str | None`、`remember_url(url, key) -> None`、`adopt(tmp_dir: Path, key) -> bool`、`new_tmp() -> Path`、`lock(name: str) -> threading.Lock`（同名返回同一把锁）。
  - `app.store.file_key(path: Path) -> str`：文件内容 SHA-256 的前 32 位十六进制，分块读。
  - `app.stats.Stats`：dataclass，字段 `downloads: int = 0`、`asr_runs: int = 0`、`ocr_runs: int = 0`。
  - `app.config`：见步骤 3。
- 目录布局：`<root>/media/<key>/`（媒体和产物）、`<root>/by-url/<url 的 SHA-256 前 32 位>`（文本文件，内容是 key）、`<root>/tmp/<随机名>/`（下载中）、`<root>/inbox/`（待导入的本地文件）。

- [ ] **Step 1: 建环境和依赖文件**

`agentflow-media/requirements.txt`：

```
fastapi
uvicorn[standard]
numpy
pillow
# 以下版本在 RTX 5080 上验证过(设计文档第 10 节)。PyAV 16 起去掉了 faster-whisper 用到的参数,不能升
faster-whisper==1.2.1
ctranslate2==4.9.0
av==15.1.0
rapidocr-onnxruntime==1.4.4
yt-dlp>=2026.10.26
pytest
httpx
```

`agentflow-media/requirements-gpu.txt`（只在有 NVIDIA 显卡的机器上装）：

```
nvidia-cublas-cu12
nvidia-cudnn-cu12==9.*
```

Run: `cd agentflow-media && python3.12 -m venv .venv && .venv/bin/pip install -q -r requirements.txt`
Expected: 安装成功。某个轮子在 Mac 上装不上就停下来报告，不要换版本。

- [ ] **Step 2: 写失败的测试** `agentflow-media/tests/test_store.py`

```python
import pytest

from app.store import Store, file_key

KEY = "0123456789abcdef0123456789abcdef"


def test_rejects_keys_that_are_not_32_hex(tmp_path):
    s = Store(tmp_path)
    for bad in ["", "abc", "../etc", KEY + "0", KEY.upper(), "g" * 32, KEY[:31] + "/"]:
        with pytest.raises(ValueError):
            s.dir(bad)


def test_json_roundtrip_and_missing(tmp_path):
    s = Store(tmp_path)
    assert s.read_json(KEY, "meta.json") is None and not s.exists(KEY)
    s.write_json(KEY, "meta.json", {"title": "讲座 1"})
    assert s.read_json(KEY, "meta.json") == {"title": "讲座 1"} and s.exists(KEY)


def test_write_json_is_atomic(tmp_path, monkeypatch):
    s = Store(tmp_path)
    s.write_json(KEY, "t.json", {"v": 1})

    def boom(*a, **k):
        raise OSError("disk full")

    monkeypatch.setattr("app.store.os.replace", boom)
    with pytest.raises(OSError):
        s.write_json(KEY, "t.json", {"v": 2})
    assert s.read_json(KEY, "t.json") == {"v": 1}
    assert [p.name for p in s.dir(KEY).iterdir()] == ["t.json"]   # 没留下临时文件


def test_file_key_is_content_hash(tmp_path):
    a, b = tmp_path / "a.bin", tmp_path / "b.bin"
    a.write_bytes(b"x" * 3_000_000)
    b.write_bytes(b"x" * 3_000_000)
    assert file_key(a) == file_key(b)
    Store(tmp_path).dir(file_key(a))   # 是合法 key,不抛异常
    b.write_bytes(b"y")
    assert file_key(a) != file_key(b)


def test_url_index(tmp_path):
    s = Store(tmp_path)
    assert s.url_key("https://youtu.be/abc") is None
    s.remember_url("https://youtu.be/abc", KEY)
    assert s.url_key("https://youtu.be/abc") == KEY and s.url_key("https://youtu.be/other") is None


def test_adopt_moves_once(tmp_path):
    s = Store(tmp_path)
    t1 = s.new_tmp()
    (t1 / "audio.m4a").write_bytes(b"a")
    assert s.adopt(t1, KEY) is True
    assert (s.dir(KEY) / "audio.m4a").read_bytes() == b"a" and not t1.exists()
    t2 = s.new_tmp()
    (t2 / "audio.m4a").write_bytes(b"b")
    assert s.adopt(t2, KEY) is False                      # 已有:不覆盖,临时目录删掉
    assert (s.dir(KEY) / "audio.m4a").read_bytes() == b"a" and not t2.exists()


def test_lock_is_per_name(tmp_path):
    s = Store(tmp_path)
    assert s.lock("a") is s.lock("a") and s.lock("a") is not s.lock("b")
```

- [ ] **Step 3: 跑测试确认失败**

Run: `cd agentflow-media && .venv/bin/python -m pytest tests/test_store.py -q`
Expected: FAIL，`ModuleNotFoundError: No module named 'app.store'`

- [ ] **Step 4: 实现 `app/config.py`、`app/stats.py`、`app/store.py`**

`app/config.py` 只读环境变量，值如下：

```python
MEDIA_ROOT = Path(os.environ.get("MEDIA_ROOT", Path(__file__).resolve().parent.parent / "data")).resolve()
ASR_MODEL = os.environ.get("MEDIA_ASR_MODEL", "large-v3")
ASR_DEVICE = os.environ.get("MEDIA_ASR_DEVICE", "cuda")
ASR_COMPUTE = os.environ.get("MEDIA_ASR_COMPUTE", "float16")
ASR_BATCH = int(os.environ.get("MEDIA_ASR_BATCH", "16"))
ALLOWED_HOSTS = {"www.youtube.com", "youtube.com", "m.youtube.com", "youtu.be"}
```

`Store.write_json`：写到同目录下的临时文件再 `os.replace`，失败时删掉临时文件。key 的正则是 `^[0-9a-f]{32}$`。`adopt` 用 `os.rename`，目标已存在就删掉临时目录并返回 `False`。

- [ ] **Step 5: 跑测试确认通过**

Run: `cd agentflow-media && .venv/bin/python -m pytest tests/test_store.py -q`
Expected: `7 passed`

- [ ] **Step 6: 接进 CI**

在 `.github/workflows/ci.yml` 的 `jobs:` 下加：

```yaml
  media:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: pip install -r agentflow-media/requirements.txt
      - run: cd agentflow-media && pytest -q
```

- [ ] **Step 7: 提交并推送**

```bash
git add .gitignore .github/workflows/ci.yml agentflow-media
git commit -m "feat(media): 媒体服务的脚手架和按内容哈希的磁盘存储"
git push
```

---

### Task 2: 导入（YouTube 链接、本地文件）

**Files:**
- Create: `agentflow-media/app/importer.py`、`agentflow-media/tests/fakes.py`、`agentflow-media/tests/test_importer.py`

**Interfaces:**
- Consumes: `Store`、`file_key`、`Stats`、`config.ALLOWED_HOSTS`。
- Produces:
  - `app.importer.Fetched`：dataclass，`audio: Path`、`video: Path | None`、`title: str`、`duration_s: float`、`source_url: str`。
  - `app.importer.Fetcher = Callable[[str, Path], Fetched]`（参数：链接、空的目标目录）。
  - `app.importer.FetchError(RuntimeError)`。
  - `check_url(url: str) -> None`：不是 http(s) 或主机不在白名单时抛 `ValueError`。
  - `import_url(store, url, fetch: Fetcher, stats: Stats) -> dict`：返回 meta 加 `"cached"`。
  - `import_path(store, path: str, inbox: Path, probe: Callable[[Path], float] = media_duration) -> dict`。
  - `media_duration(path: Path) -> float`：用 PyAV 读时长（秒）。
  - `ytdlp_fetch(url, dest) -> Fetched`：真实下载器。
  - `tests/fakes.py`：`URL`、`KEY`、`fake_fetch(calls, audio=b"AUDIO", video=b"VIDEO")`。
- meta.json 的内容：`{"mediaKey","title","durationMs","sourceUrl","audio","video"}`。链接导入时 `audio`、`video` 是下载到的文件名；本地文件导入时两者都是 `source<原扩展名>`，`title` 是不带扩展名的文件名。

- [ ] **Step 1: 写假下载器** `agentflow-media/tests/fakes.py`

```python
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
```

- [ ] **Step 2: 写失败的测试** `agentflow-media/tests/test_importer.py`

```python
import threading
import time

import pytest

from app.importer import FetchError, check_url, import_path, import_url
from app.stats import Stats
from app.store import Store
from tests.fakes import URL, fake_fetch


def test_import_url_stores_media_and_meta(tmp_path):
    s, calls, stats = Store(tmp_path), [], Stats()
    meta = import_url(s, URL, fake_fetch(calls), stats)
    assert meta["cached"] is False and meta["title"] == "Lecture 1" and meta["durationMs"] == 3600500
    assert meta["sourceUrl"] == URL and meta["audio"] == "audio.m4a" and meta["video"] == "video.mp4"
    d = s.dir(meta["mediaKey"])
    assert (d / "audio.m4a").read_bytes() == b"AUDIO" and (d / "video.mp4").exists()
    assert s.read_json(meta["mediaKey"], "meta.json")["title"] == "Lecture 1"
    assert "cached" not in s.read_json(meta["mediaKey"], "meta.json") and stats.downloads == 1


def test_same_url_is_not_downloaded_twice(tmp_path):
    s, calls, stats = Store(tmp_path), [], Stats()
    first = import_url(s, URL, fake_fetch(calls), stats)
    again = import_url(s, URL, fake_fetch(calls), stats)
    assert calls == [URL] and again["cached"] is True and again["mediaKey"] == first["mediaKey"]
    assert stats.downloads == 1


def test_same_audio_from_another_url_reuses_the_media(tmp_path):
    s, calls, stats = Store(tmp_path), [], Stats()
    a = import_url(s, URL, fake_fetch(calls), stats)
    b = import_url(s, "https://youtu.be/abc123", fake_fetch(calls), stats)
    assert b["mediaKey"] == a["mediaKey"] and b["cached"] is True and len(calls) == 2


def test_audio_only_source_has_no_video(tmp_path):
    meta = import_url(Store(tmp_path), URL, fake_fetch([], video=None), Stats())
    assert meta["video"] is None


def test_concurrent_imports_of_one_url_download_once(tmp_path):
    s, calls, stats = Store(tmp_path), [], Stats()
    inner = fake_fetch(calls)

    def slow(url, dest):
        time.sleep(0.2)
        return inner(url, dest)

    threads = [threading.Thread(target=import_url, args=(s, URL, slow, stats)) for _ in range(3)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert calls == [URL]


@pytest.mark.parametrize("bad", [
    "ftp://www.youtube.com/x", "http://localhost/x", "https://evil.example/watch?v=1",
    "https://www.youtube.com.evil.example/x", "file:///etc/passwd", "not a url", "",
])
def test_rejects_urls_outside_the_allowlist(bad):
    with pytest.raises(ValueError):
        check_url(bad)


def test_failed_download_leaves_nothing_behind(tmp_path):
    s = Store(tmp_path)

    def fetch(url, dest):
        (dest / "audio.m4a.part").write_bytes(b"x")
        raise RuntimeError("HTTP 403")

    with pytest.raises(FetchError):
        import_url(s, URL, fetch, Stats())
    assert s.url_key(URL) is None and not list((tmp_path / "tmp").glob("*"))


def test_import_path_moves_file_from_inbox(tmp_path):
    s, inbox = Store(tmp_path), tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "lec 1.mp4").write_bytes(b"LOCAL")
    meta = import_path(s, str(inbox / "lec 1.mp4"), inbox, probe=lambda p: 12.0)
    assert meta["title"] == "lec 1" and meta["durationMs"] == 12000 and meta["sourceUrl"] is None
    assert meta["audio"] == meta["video"] == "source.mp4" and meta["cached"] is False
    assert not (inbox / "lec 1.mp4").exists() and (s.dir(meta["mediaKey"]) / "source.mp4").read_bytes() == b"LOCAL"


def test_importing_the_same_local_content_twice_is_cached(tmp_path):
    s, inbox = Store(tmp_path), tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "a.mp4").write_bytes(b"SAME")
    first = import_path(s, str(inbox / "a.mp4"), inbox, probe=lambda p: 1.0)
    (inbox / "b.mp4").write_bytes(b"SAME")
    again = import_path(s, str(inbox / "b.mp4"), inbox, probe=lambda p: 1.0)
    assert again["mediaKey"] == first["mediaKey"] and again["cached"] is True
    assert (inbox / "b.mp4").exists()            # 重复的文件留在原处,不删用户的东西


def test_import_path_rejects_outside_inbox_or_missing(tmp_path):
    s, inbox = Store(tmp_path), tmp_path / "inbox"
    inbox.mkdir()
    (tmp_path / "outside.mp4").write_bytes(b"x")
    for p in [str(inbox / ".." / "outside.mp4"), "/etc/passwd", str(inbox / "missing.mp4"), str(inbox)]:
        with pytest.raises(ValueError):
            import_path(s, p, inbox, probe=lambda _: 1.0)
    assert (tmp_path / "outside.mp4").exists()


def test_symlink_pointing_outside_inbox_is_rejected(tmp_path):
    s, inbox = Store(tmp_path), tmp_path / "inbox"
    inbox.mkdir()
    (tmp_path / "secret.mp4").write_bytes(b"x")
    (inbox / "link.mp4").symlink_to(tmp_path / "secret.mp4")
    with pytest.raises(ValueError):
        import_path(s, str(inbox / "link.mp4"), inbox, probe=lambda _: 1.0)
```

- [ ] **Step 3: 跑测试确认失败**

Run: `cd agentflow-media && .venv/bin/python -m pytest tests/test_importer.py -q`
Expected: FAIL，`ModuleNotFoundError: No module named 'app.importer'`

- [ ] **Step 4: 实现 `app/importer.py`**

`import_url` 的顺序：`check_url` → 拿 `store.lock("url:" + url)` → 查 `store.url_key(url)`，命中就读 meta 返回 `cached=True` → `store.new_tmp()` → `fetch`（任何异常都删掉临时目录并抛 `FetchError`）→ `stats.downloads += 1` → `key = file_key(fetched.audio)` → `store.adopt(tmp, key)`；返回 `True` 时写 meta.json，返回 `False` 说明内容已有，`cached=True` → `store.remember_url(url, key)`。

`import_path`：`Path(path).resolve()` 之后必须是 `inbox.resolve()` 下面的普通文件，否则 `ValueError`；key 是文件内容哈希；已存在则 `cached=True` 且不动原文件；否则把文件移进 `store.dir(key) / ("source" + 后缀)`。

`ytdlp_fetch` 分两次下载音频和画面，不合并，所以不需要 ffmpeg：

```python
def ytdlp_fetch(url: str, dest: Path) -> Fetched:
    import yt_dlp   # 懒加载:测试不需要它

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
        video = None            # 只有声音的来源:后面截帧得到空列表
    return Fetched(audio, video, info.get("title") or info["id"],
                   float(info.get("duration") or 0), info.get("webpage_url") or url)
```

- [ ] **Step 5: 跑测试确认通过**

Run: `cd agentflow-media && .venv/bin/python -m pytest tests/test_importer.py -q`
Expected: `17 passed`（7 个 URL 参数各算一个）

- [ ] **Step 6: 提交并推送**

```bash
git add agentflow-media
git commit -m "feat(media): 从 YouTube 链接和本地文件导入,同一内容只存一份"
git push
```

---

### Task 3: 转写（缓存、GPU 单路）

**Files:**
- Create: `agentflow-media/app/asr.py`、`agentflow-media/tests/test_asr.py`
- Modify: `agentflow-media/tests/fakes.py`（加 `FakeTranscriber`）

**Interfaces:**
- Consumes: `Store`、`Stats`。
- Produces:
  - `app.asr.Transcriber`：Protocol，`transcribe(audio: Path) -> tuple[str, list[dict]]`，返回（语言代码，utterance 列表），每条是 `{"startMs": int, "endMs": int, "text": str}`。
  - `app.asr.WhisperTranscriber(model, device, compute, batch)`：真实实现。
  - `app.asr.transcribe_media(store, key, transcriber, model_tag: str, stats) -> dict`：返回 `{"mediaKey","model","cached","utterances","durationMs"}`；媒体不存在抛 `KeyError`。
  - 产物文件：`transcript.<model_tag>.json`，内容 `{"model","language","utterances":[…]}`。缓存键里带模型名，换模型才会重跑。
  - `tests/fakes.py`：`FakeTranscriber(delay=0.0, fail=False, utterances=None)`，属性 `calls`、`max_active`。

- [ ] **Step 1: 在 `tests/fakes.py` 加假转写器**

```python
import time


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
```

- [ ] **Step 2: 写失败的测试** `agentflow-media/tests/test_asr.py`

```python
import threading

import pytest

from app.asr import transcribe_media
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
```

- [ ] **Step 3: 跑测试确认失败**

Run: `cd agentflow-media && .venv/bin/python -m pytest tests/test_asr.py -q`
Expected: FAIL，`ModuleNotFoundError: No module named 'app.asr'`

- [ ] **Step 4: 实现 `app/asr.py`**

`transcribe_media`：读 meta（没有就 `KeyError`）→ 先查缓存 → 拿模块级的 `GPU_LOCK = threading.Lock()` → **拿到锁以后再查一次缓存**（同一讲的并发请求只跑一次）→ 调 `transcriber.transcribe(store.dir(key) / meta["audio"])` → `store.write_json` → `stats.asr_runs += 1`。

真实实现（整个文件交给批处理管线，它自带静音检测切分；不要自己切片）：

```python
class WhisperTranscriber:
    def __init__(self, model: str, device: str, compute: str, batch: int):
        from faster_whisper import BatchedInferencePipeline, WhisperModel   # 懒加载
        self._pipe = BatchedInferencePipeline(WhisperModel(model, device=device, compute_type=compute))
        self._batch = batch

    def transcribe(self, audio: Path) -> tuple[str, list[dict]]:
        segments, info = self._pipe.transcribe(str(audio), batch_size=self._batch)
        return info.language, [
            {"startMs": round(s.start * 1000), "endMs": round(s.end * 1000), "text": s.text.strip()}
            for s in segments if s.text.strip()
        ]
```

- [ ] **Step 5: 跑测试确认通过**

Run: `cd agentflow-media && .venv/bin/python -m pytest tests/test_asr.py -q`
Expected: `8 passed`

- [ ] **Step 6: 提交并推送**

```bash
git add agentflow-media
git commit -m "feat(media): 转写结果按模型缓存,GPU 上同一时刻只跑一路"
git push
```

---

### Task 4: 截帧和画面文字识别

**Files:**
- Create: `agentflow-media/app/frames.py`、`agentflow-media/tests/test_frames.py`
- Modify: `agentflow-media/tests/fakes.py`（加 `CountingOcr`）

**Interfaces:**
- Consumes: `Store`、`Stats`。
- Produces:
  - `app.frames.keyframes(video: Path) -> Iterator[tuple[int, PIL.Image.Image]]`：只解码关键帧，产出（时间戳毫秒，图像）。
  - `app.frames.small(img) -> np.ndarray`（160×90 灰度，int16）和 `app.frames.changed(a, b) -> bool`。
  - `app.frames.Ocr`：Protocol，`read(img: PIL.Image.Image) -> str`（多行用 `\n` 连接）。
  - `app.frames.RapidOcr()`：真实实现。
  - `app.frames.frames_media(store, key, ocr, stats) -> dict`：返回 `{"mediaKey","cached","frames"}`；媒体不存在抛 `KeyError`。
  - 产物：`frames.json`（`{"frames":[{"tsMs","file","text"}]}`）和 `frames/<9 位毫秒>.jpg`。
  - `tests/fakes.py`：`CountingOcr(text)`，属性 `calls`。
- 取舍规则（数值在服务器上用合成视频量过：同一页相邻关键帧变化比例为 0，只换文字的翻页为 0.022 到 0.030）：
  - 第一张关键帧总是做 OCR。之后每张关键帧和**上一张做过 OCR 的**关键帧相比，160×90 灰度图上亮度差超过 24 的像素占比大于 `0.003`，才算"画面变了"，才做 OCR。
  - 识别出的文字去掉首尾空白后少于 10 个字符的帧丢弃；文字和上一张保留的帧完全相同的也丢弃。

- [ ] **Step 1: 在 `tests/fakes.py` 加假 OCR**

```python
class CountingOcr:
    def __init__(self, text):
        self.text, self.calls = text, 0

    def read(self, img):
        self.calls += 1
        return self.text
```

- [ ] **Step 2: 写失败的测试** `agentflow-media/tests/test_frames.py`

```python
import shutil

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
```

- [ ] **Step 3: 跑测试确认失败**

Run: `cd agentflow-media && .venv/bin/python -m pytest tests/test_frames.py -q`
Expected: FAIL，`ModuleNotFoundError: No module named 'app.frames'`

- [ ] **Step 4: 实现 `app/frames.py`**

这三段的写法测试定不死，照这个写：

```python
def keyframes(video: Path) -> Iterator[tuple[int, Image.Image]]:
    with av.open(str(video)) as container:
        stream = container.streams.video[0]
        stream.codec_context.skip_frame = "NONKEY"        # 只解码关键帧:一小时的课几秒钟
        for frame in container.decode(stream):
            yield round(float(frame.pts * stream.time_base) * 1000), frame.to_image()


def small(img: Image.Image) -> np.ndarray:
    return np.asarray(img.convert("L").resize((160, 90)), dtype=np.int16)


def changed(a: np.ndarray, b: np.ndarray) -> bool:
    return float((np.abs(a - b) > 24).mean()) > 0.003


class RapidOcr:
    def __init__(self):
        from rapidocr_onnxruntime import RapidOCR   # 懒加载
        self._engine = RapidOCR()

    def read(self, img: Image.Image) -> str:
        result, _ = self._engine(np.asarray(img.convert("RGB")))
        return "\n".join(line[1] for line in result or [])
```

`frames_media`：读 meta（没有就 `KeyError`）→ 查缓存 → `meta["video"]` 为空就直接写空列表 → 按上面"取舍规则"遍历关键帧，保留的帧存成 `frames/<tsMs:09d>.jpg`（JPEG 质量 80）→ 全部成功后才 `store.write_json(key, "frames.json", …)` → `stats.ocr_runs += 1`。函数顶上加一行注释：`# ponytail: 拍摄讲台的视频几乎每个关键帧都算"变了",OCR 次数接近关键帧数;真遇到再加每分钟上限`。

- [ ] **Step 5: 跑测试确认通过**

Run: `cd agentflow-media && .venv/bin/python -m pytest tests/test_frames.py -q`
Expected: `10 passed`

- [ ] **Step 6: 提交并推送**

```bash
git add agentflow-media
git commit -m "feat(media): 只在画面变化时截帧并识别文字"
git push
```

---

### Task 5: HTTP 接口

**Files:**
- Create: `agentflow-media/app/main.py`、`agentflow-media/run.sh`（可执行）、`agentflow-media/tests/test_api.py`
- Modify: `README.zh.md`（"RAG 检索与多角色 Agent"一节后面加一小节"媒体服务"：做什么、怎么启动、接口表指向本计划）

**Interfaces:**
- Consumes: 任务 1 到 4 的全部产物。
- Produces: 上面"接口约定"一节的全部路由，以及 FastAPI 依赖 `get_store`、`get_stats`、`get_fetcher`、`get_transcriber`、`get_ocr`（测试用 `app.dependency_overrides` 换掉）。`get_transcriber` 和 `get_ocr` 第一次用到时才加载模型，之后复用同一个实例。

- [ ] **Step 1: 写失败的测试** `agentflow-media/tests/test_api.py`

```python
import pytest
from fastapi.testclient import TestClient

from app.main import app, get_fetcher, get_ocr, get_stats, get_store, get_transcriber
from app.stats import Stats
from app.store import Store
from tests.fakes import URL, CountingOcr, FakeTranscriber, fake_fetch

MISSING = "f" * 32


@pytest.fixture
def client(tmp_path):
    store, stats, transcriber = Store(tmp_path), Stats(), FakeTranscriber()
    app.dependency_overrides.update({
        get_store: lambda: store, get_stats: lambda: stats, get_fetcher: lambda: fake_fetch([]),
        get_transcriber: lambda: transcriber, get_ocr: lambda: CountingOcr("slide text long enough"),
    })
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_import_transcribe_and_read_back(client):
    meta = client.post("/media/import", json={"url": URL}).json()
    key = meta["mediaKey"]
    assert meta["cached"] is False and meta["title"] == "Lecture 1"
    r = client.post(f"/media/{key}/transcribe")
    assert r.status_code == 200 and r.json()["utterances"] == 2 and r.json()["model"] == "large-v3"
    assert client.get(f"/media/{key}/transcript").json()["utterances"][0]["text"] == "Welcome to lecture one."
    assert client.get(f"/media/{key}/meta").json()["durationMs"] == 3600500
    assert client.get("/stats").json() == {"downloads": 1, "asrRuns": 1, "ocrRuns": 0}


def test_doing_it_all_again_does_no_work(client):
    key = client.post("/media/import", json={"url": URL}).json()["mediaKey"]
    client.post(f"/media/{key}/transcribe")
    assert client.post("/media/import", json={"url": URL}).json()["cached"] is True
    assert client.post(f"/media/{key}/transcribe").json()["cached"] is True
    assert client.get("/stats").json() == {"downloads": 1, "asrRuns": 1, "ocrRuns": 0}


def test_import_needs_exactly_one_of_url_or_path(client):
    assert client.post("/media/import", json={}).status_code == 422
    assert client.post("/media/import", json={"url": URL, "path": "/x"}).status_code == 422


def test_disallowed_url_is_400(client):
    assert client.post("/media/import", json={"url": "https://evil.example/x"}).status_code == 400


def test_download_failure_is_502(client):
    def broken(url, dest):
        raise RuntimeError("HTTP 403")
    app.dependency_overrides[get_fetcher] = lambda: broken
    r = client.post("/media/import", json={"url": URL})
    assert r.status_code == 502 and "detail" in r.json()


def test_unknown_or_malformed_key(client):
    assert client.get(f"/media/{MISSING}/meta").status_code == 404
    assert client.post(f"/media/{MISSING}/transcribe").status_code == 404
    assert client.post(f"/media/{MISSING}/frames").status_code == 404
    assert client.get("/media/NOT-A-KEY/meta").status_code == 400


def test_products_that_do_not_exist_yet_are_404(client):
    key = client.post("/media/import", json={"url": URL}).json()["mediaKey"]
    assert client.get(f"/media/{key}/transcript").status_code == 404
    assert client.get(f"/media/{key}/frames").status_code == 404


def test_frame_file_name_is_validated(client):
    key = client.post("/media/import", json={"url": URL}).json()["mediaKey"]
    assert client.get(f"/media/{key}/frames/meta.json").status_code == 400
    assert client.get(f"/media/{key}/frames/..%2Fmeta.json").status_code in (400, 404)
    assert client.get(f"/media/{key}/frames/000000000.jpg").status_code == 404


def test_health(client):
    assert client.get("/health").json() == {"status": "ok", "asrModel": "large-v3"}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd agentflow-media && .venv/bin/python -m pytest tests/test_api.py -q`
Expected: FAIL，`ModuleNotFoundError: No module named 'app.main'`

- [ ] **Step 3: 实现 `app/main.py` 和 `run.sh`**

- 路由函数用普通 `def`（不是 `async def`），FastAPI 会放进线程池跑，长请求不会卡住别的请求。
- 请求体：`class ImportRequest(BaseModel)`，`url: str | None = None`、`path: str | None = None`，用 `model_validator` 要求恰好给一个。
- 异常处理器：`ValueError` → 400，`KeyError` → 404，`FetchError` → 502，响应体都是 `{"detail": str(e)}`。
- 帧文件名必须匹配 `^\d{9}\.jpg$`，否则 400；文件不存在 404；用 `FileResponse` 返回。
- 本地文件导入的 inbox 固定是 `store` 根目录下的 `inbox/`。

`run.sh`：

```bash
#!/usr/bin/env bash
# 启动媒体服务。只监听本机;必须单进程(GPU 锁和计数器在进程内)。
set -euo pipefail
cd "$(dirname "$0")"
NV=$(echo .venv/lib/python3.*/site-packages/nvidia)
if [ -d "$NV/cublas/lib" ]; then   # 用 pip 装的 CUDA 库时,ctranslate2 要靠这个找到它们
  export LD_LIBRARY_PATH="$PWD/$NV/cublas/lib:$PWD/$NV/cudnn/lib:/usr/lib/wsl/lib:${LD_LIBRARY_PATH:-}"
fi
exec .venv/bin/uvicorn app.main:app --host 127.0.0.1 --port "${MEDIA_PORT:-8010}" --workers 1
```

- [ ] **Step 4: 跑全部测试确认通过**

Run: `cd agentflow-media && .venv/bin/python -m pytest -q`
Expected: 全部通过（约 51 个），没有联网、没有加载转写模型。

- [ ] **Step 5: 在本机起服务，确认能启动**

Run: `cd agentflow-media && MEDIA_ASR_DEVICE=cpu ./run.sh & sleep 3; curl -s http://127.0.0.1:8010/health; kill %1`
Expected: `{"status":"ok","asrModel":"large-v3"}`（没有加载模型，所以很快）

- [ ] **Step 6: 提交并推送**

```bash
git add agentflow-media README.zh.md
git commit -m "feat(media): 媒体服务的 HTTP 接口"
git push
```

---

### Task 6: 在 Upton 上部署，用真实课程冒烟，记录数字

**Files:**
- Create: `agentflow-media/smoke.py`
- Modify: `docs/benchmarks.md`（加一节"课程录播：转写和截帧"）

**Interfaces:**
- Consumes: 任务 5 的 HTTP 接口。
- Produces: `smoke.py --base <服务地址> --url <YouTube 链接>`：只用标准库。依次调用 import、transcribe、frames，再全部重复一遍；打印一行 JSON：`{"mediaKey","title","durationMs","importS","transcribeS","realtimeX","utterances","framesS","frames","secondPassAllCached","statsAfter"}`。第二遍有任何一步不是 `cached`，或者 `/stats` 的三个计数在第二遍之后变了，就以非零状态退出。

- [ ] **Step 1: 写 `smoke.py`**（按上面的 Produces；用 `urllib.request`，每个请求超时 1800 秒）

- [ ] **Step 2: 提交并推送**（服务器上要 `git pull` 到这个文件）

```bash
git add agentflow-media/smoke.py
git commit -m "test(media): 真实链接的冒烟脚本"
git push
```

- [ ] **Step 3: 在服务器上装环境**

先确认不会碰到别的东西：

```bash
ssh Upton@100.65.10.56 'ss -ltn | grep -c ":8010 " ; cd ~/project/agentflow-drill && git status --short | wc -l && git rev-parse --abbrev-ref HEAD'
```
Expected: 第一行 `0`（端口空闲），第二行 `0`（克隆是干净的），第三行 `main`。任何一项不符就停下来报告，不要自己处理。

```bash
ssh Upton@100.65.10.56 '
set -e
mkdir -p ~/.local/bin ~/agentflow-media-data
[ -x ~/.local/bin/uv ] || cp ~/spike-asr-workers/bin/uv ~/.local/bin/uv
[ -d ~/agentflow-media-data/hf ] || mv ~/spike-asr-workers/hf ~/agentflow-media-data/hf || true   # 复用已下载的 large-v3;没有就在第一次转写时下载(约 3GB)
cd ~/project/agentflow-drill && git pull --ff-only
cd agentflow-media && ~/.local/bin/uv venv -q --python 3.12 .venv
~/.local/bin/uv pip install -q --python .venv/bin/python -r requirements.txt -r requirements-gpu.txt
.venv/bin/python -m pytest -q | tail -1'
```
Expected: 最后一行是 `… passed`。

- [ ] **Step 4: 启动服务**

```bash
ssh Upton@100.65.10.56 'cd ~/project/agentflow-drill/agentflow-media && MEDIA_ROOT=$HOME/agentflow-media-data HF_HOME=$HOME/agentflow-media-data/hf setsid nohup ./run.sh > ~/agentflow-media-data/media.log 2>&1 < /dev/null & sleep 4; curl -s http://127.0.0.1:8010/health'
```
Expected: `{"status":"ok","asrModel":"large-v3"}`

- [ ] **Step 5: 用评测课的第 1 讲冒烟**

从 https://rdi.berkeley.edu/agentic-ai/f25 找到第 1 讲的录像链接（YouTube）。页面上找不到 YouTube 链接就停下来报告。

```bash
ssh Upton@100.65.10.56 'cd ~/project/agentflow-drill/agentflow-media && .venv/bin/python smoke.py --base http://127.0.0.1:8010 --url "<第 1 讲的链接>"'
```
Expected: 退出码 0；`secondPassAllCached` 为 `true`；`utterances` 大于 100；`realtimeX` 大于 30（设计文档第 10 节测到 95）；`frames` 大于 5。`realtimeX` 低于 30 或 `frames` 为 0 都要停下来查原因（看 `~/agentflow-media-data/media.log`），不要改阈值。

- [ ] **Step 6: 抽查质量**

```bash
ssh Upton@100.65.10.56 'curl -s http://127.0.0.1:8010/media/<mediaKey>/transcript | python3 -c "import json,sys; d=json.load(sys.stdin); [print(u[\"startMs\"]//1000, u[\"text\"]) for u in d[\"utterances\"][:15]]"; curl -s http://127.0.0.1:8010/media/<mediaKey>/frames | python3 -c "import json,sys; [print(f[\"tsMs\"]//1000, repr(f[\"text\"][:80])) for f in json.load(sys.stdin)[\"frames\"][:10]]"'
```
人工看一眼：转写读得通、时间戳递增；截到的帧是幻灯片上的文字而不是乱码。把看到的问题（比如同一页被截了很多次）记进下一步的文档里，不在本计划里修。

- [ ] **Step 7: 把数字写进 `docs/benchmarks.md`**

新增一节"课程录播：转写和截帧"，包含三部分，全部用实测值：
1. 设计文档第 10 节的 8 行对比表和四条结论（原样搬过来，注明测试素材和日期）。
2. 本次冒烟的结果：视频时长、下载用时、转写用时和倍速、utterance 数、截帧用时和帧数、第二遍全部命中缓存。
3. 抽查时看到的问题。

- [ ] **Step 8: 提交并推送**

```bash
git add docs/benchmarks.md
git commit -m "docs: 课程录播的转写与截帧实测数据"
git push
```

服务留在服务器上继续跑，计划 2 要用。停掉的办法：`ssh Upton@100.65.10.56 'pkill -f "uvicorn app.main:app --host 127.0.0.1 --port 8010"'`。
