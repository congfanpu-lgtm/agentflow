# 课程录播 · 计划 2：引擎上的摄入（课程、讲次、分段、索引、知识点目录、两个页面） Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在课程库页面贴一个 YouTube 链接（或填一个本地文件名），AgentFlow 把这一讲下载、转写、截帧识别、分段、建索引、生成知识点目录，单讲页能看到逐字稿、幻灯片和知识点目录，并且点一下就跳到视频的那一秒。

**Architecture:** 一讲课是一个 `LECTURE_INGEST` 任务，分五步走现有的多阶段推进（`StageAdvancer`）：导入 → 转写 ∥ 截帧 → 建索引 → 知识点目录。每一步是一个普通的 Kafka 子任务，重试、超时重投、幂等、trace 都复用引擎已有的。重活在计划 1 的媒体服务里做，worker 只发 HTTP 请求；子任务的输入输出只带 `mediaKey` 和计数，逐字稿不经过 Kafka 和 MySQL。片段进 `agentflow-rag` 的一张新表，按课程隔离检索。课程和讲次是 MySQL 里两张新表。

**Tech Stack:** Java 17 / Spring Boot / MyBatis-Plus（server、worker）、Python 3.12 / FastAPI / pgvector（agentflow-rag）、agentflow-media（计划 1）、原生 HTML + JS（页面，和现有页面一样不引框架）。

**Spec:** `docs/superpowers/specs/2026-11-13-course-video-qa-design.md`（第 5、6、8 节）。计划 1：`docs/superpowers/plans/2026-11-13-course-video-1-media-service.md`；实测数据：`docs/benchmarks.md` 第 7 节。

**这是四份计划里的第二份。** 计划 3 是问答 agent、三层核验和问答页；计划 4 是评测和故障演练。

## Global Constraints

- Java 源码按 `maven.compiler.release=17` 写。子任务的 `kind` 不超过 16 个字符（`subtask.kind` 是 `VARCHAR(16)`），而且在所有处理器里唯一（worker 按它路由）。
- 所有 JSON 字段用 camelCase，时间是整数毫秒、字段名以 `Ms` 结尾。
- **所有测试和课程环境都用模拟模型。** 真实模型什么时候用、用哪个，是计划 4 里用户的决定。`.env`（里面是真实模型的 key）不能同步到服务器。
- 课程功能默认关闭：`agentflow.course.enabled=false`。公开 demo 用的也是这份代码，不能让路人触发下载和转写。
- 逐字稿和帧图片只存在媒体服务里；MySQL 里只有课程、讲次和知识点目录；片段和向量只在 pgvector 里。
- 不改 `documents` 表和 `/rag/ingest`、`/rag/search` 的行为（研究 agent 和 BEIR 评测在用）。
- 第一版的范围限制（不在本计划里做）：播放列表和 B 站；本地文件在页面上播放；知识点地图；摄入和问答的隔离（设计文档第 7 节）。
- **在哪里跑什么：**
  - Mac 上不启动 Docker。不需要 Spring 容器的 Java 单元测试在 Mac 上跑：`mvn -q -pl <模块> -am test -Dtest=<类名> -Dsurefire.failIfNoSpecifiedTests=false`。媒体服务的测试在 Mac 上跑：`cd agentflow-media && .venv/bin/python -m pytest -q`。
  - 需要 MySQL / Kafka / pgvector 的测试在 Upton 上跑（任务 2 建好环境之后）。代码用 `rsync` 同步到 `~/project/agentflow-course-dev`（不是 git 克隆，可以带着没提交的改动）。
  - 服务器上只动这些：`~/project/agentflow-course-dev`（新建）、`~/project/agentflow-drill/agentflow-media`（媒体服务，`git pull` 更新）、`~/agentflow-media-data`、compose 项目 `agentflow-coursetest` 和 `agentflow-course`。宿主机上只新占 `127.0.0.1:18090`。长期运行的服务（agentflow、stockbrief、mock-interview、finance tracker、dashboard）和 `~/project/agentflow` 一律不动。一次性容器必须带 `--user`。
- 执行时用到的两个 shell 函数（在 Mac 的仓库根目录下定义）：

```bash
SYNC() { rsync -az --delete --exclude .git --exclude .env --exclude target --exclude .venv --exclude __pycache__ \
  --exclude .pytest_cache --exclude .superpowers --exclude 'agentflow-media/data' ./ Upton@100.65.10.56:project/agentflow-course-dev/; }
R() { ssh Upton@100.65.10.56 "cd ~/project/agentflow-course-dev && $*"; }
```

- 提交信息用 Conventional Commits、中文描述，结尾带仓库现行的两行 trailer。按 `CLAUDE.md`，每个任务完成后提交并推送到 `main`。

## 接口约定（计划 3、4 依赖这一节）

**子任务（`LECTURE_INGEST` 的五步）**

| seq | kind | 输入 | 输出 |
|---|---|---|---|
| 0 | `LEC_IMPORT` | `{"lectureId","url"}` 或 `{"lectureId","path"}` | `{"mediaKey","title","durationMs","hasVideo"}` |
| 1 | `LEC_TRANSCRIBE` | `{"lectureId","mediaKey"}` | `{"utterances","cached"}` |
| 2 | `LEC_FRAMES` | `{"lectureId","mediaKey"}` | `{"frames","cached"}` |
| 3 | `LEC_INDEX` | `{"courseId","lectureId","mediaKey","durationMs","withSlides"}` | `{"speech","slides"}` |
| 4 | `LEC_CONCEPTS` | `{"lectureId","mediaKey","title"}`（可带 `"model"`） | `{"concepts":[{"name","startMs","summary"}],"usage"}` |

- 任务 payload：`{"lectureId","courseId"}`。链接和路径**只从 `lecture` 表读**，payload 里带了也不看。
- 终态：五步都成功 → `COMPLETED`（讲次 `READY`）；索引建成了但截帧或知识点目录失败 → `PARTIAL_FAILED`（讲次 `PARTIAL`）；索引没建成 → `FAILED`（讲次 `FAILED`）。

**检索服务（agentflow-rag）**

| 方法和路径 | 请求 | 响应 |
|---|---|---|
| `POST /segments/index` | `{"courseId":int,"lectureId":int,"segments":[{"startMs","endMs","kind":"speech"\|"slide","text"}]}` | `{"indexed":n}`。**整讲替换**：先删这一讲原有的片段再写入，空列表等于清空 |
| `POST /segments/search` | `{"courseId":int,"query":str,"topK":int=5（1 到 50）,"lectureIds":[int]（可省）,"mode":"hybrid"\|"dense"\|"bm25"}` | `{"hits":[{"id","lectureId","startMs","endMs","kind","text","score"}]}` |

- 片段 id：`"<lectureId>:<kind>:<startMs>"`。

**server（都要 `agentflow.course.enabled=true`，否则 404）**

| 方法和路径 | 请求 | 响应 |
|---|---|---|
| `POST /api/v1/courses` | `{"name"}` | `{"id","name"}` |
| `GET /api/v1/courses` | | `[{"id","name","lectures":n,"ready":n}]` |
| `GET /api/v1/courses/{id}` | | `{"id","name","lectures":[讲次]}` |
| `POST /api/v1/courses/{id}/lectures` | `{"url"}` 或 `{"path"}`，可带 `"title"` | 讲次 |
| `GET /api/v1/lectures/{id}` | | 讲次，多一个 `"concepts"` |
| `POST /api/v1/lectures/{id}/retry` | | 讲次（只有 `FAILED`、`PARTIAL` 能重试，否则 400） |
| `GET /api/v1/lectures/{id}/transcript` | | 媒体服务的逐字稿原样返回 |
| `GET /api/v1/lectures/{id}/frames` | | 媒体服务的帧列表原样返回 |
| `GET /api/v1/lectures/{id}/frames/{name}` | `name` 形如 `000020000.jpg` | JPEG |

- 讲次：`{"id","courseId","seq","title","status","durationMs","sourceUrl","error","stages":[{"kind","status","error","seconds"}]}`。`status` 是 `INGESTING` / `READY` / `PARTIAL` / `FAILED`；`seconds` 只在这一步落定后有值。
- 错误体 `{"error":"原因"}`：请求不对 400，找不到 404，媒体服务不通 502。

## Review Focus

设计文档没有逐条写、但用起来最可能出事的六种情况。每条都在对应任务里有测试：

1. **同一讲被重复处理**（重试按钮、超时重投、worker 重复消费）：索引里不能出现两份片段。→ 任务 4 `test_reindex_replaces_the_lecture`、任务 9 `retryIsOnlyForFailedOrPartialLectures`。
2. **中间某一步失败**：转写失败整讲失败并写明原因；截帧或知识点目录失败算"部分完成"，语音片段照样能检索。→ 任务 8 `terminalStatusFollowsTheIndex`、`onFinishedRecordsWhichStepFailed`、`failedTranscriptionFailsTheLecture`。
3. **两门课的数据串了**：在 A 课里搜，不能搜出 B 课的片段。→ 任务 4 `test_search_never_crosses_courses`。
4. **模型输出不合规**（知识点目录）：片段编号越界、不是 JSON、空列表。带错时间的知识点不能入库。→ 任务 7 `dropsConceptsThatPointOutsideTheLecture`、`failsWhenNothingUsableComesBack`。
5. **转写很长、没有标点、或者是空的**：单条超长、全程没有句号、一条都没有。→ 任务 5 `cutsAtTheHardCapWhenThereIsNoPunctuation`、`emptyTranscriptGivesNoSegments`，任务 6 `indexFailsForGoodWhenNothingWasSaid`。
6. **路人通过公开接口触发摄入**：功能开关关着时课程接口不存在；通用任务接口也只能处理 `lecture` 表里已有的讲次，不能自带链接。→ 任务 8 `decomposeIgnoresALinkInThePayload`、任务 9 `CourseApiDisabledTest`。

---

### Task 1: 截帧去重（媒体服务）

计划 1 冒烟时发现：讲者摄像头小窗让 76% 的关键帧都算"画面变了"，同一页被截了很多次（一讲 896 帧，13 分钟）。数据见 `docs/benchmarks.md` 第 7 节。

**Files:**
- Modify: `agentflow-media/app/frames.py`、`agentflow-media/app/main.py`（读帧列表改用 `FRAMES_FILE`）、`agentflow-media/tests/test_frames.py`、`docs/benchmarks.md`（第 7 节补一段实测）

**Interfaces:**
- Produces（都在 `app.frames`）：
  - `FRAMES_FILE = "frames.v2.json"`：产物换名，等于让旧结果失效。内容 `{"frames":[{"tsMs","file","text"}],"keyframes":int,"ocred":int}`。
  - `block_changes(a, b) -> np.ndarray`：形状 `(9, 16)` 的布尔数组。160×90 的小图切成 10×10 的块；一块里亮度差超过 24 的像素多于 5%，这块算变了。
  - `noisy_blocks(video: Path) -> np.ndarray`：`(9, 16)` 布尔数组。在超过一半的相邻关键帧对里都变了的块，是"一直在动"的区域（摄像头小窗）。相邻对少于 4 个时全为 `False`。
  - `changed(a, b, ignore=None) -> bool`：`ignore` 以外至少有 2 个块变了。
  - `same_slide(a: str, b: str) -> bool`：去掉所有空白、转小写后，`difflib.SequenceMatcher(None, x, y, autojunk=False).ratio() >= 0.8`。
- `frames_media` 的新流程：先整段扫一遍算 `noisy_blocks` → 清空 `frames/` 目录 → 第二遍只对"和上一张识别过的帧相比变了（忽略一直在动的块）"的关键帧做 OCR → 文字和上一张保留的帧 `same_slide` 就丢弃 → 写 `FRAMES_FILE`。返回值不变。

- [ ] **Step 1: 写失败的测试**，追加到 `agentflow-media/tests/test_frames.py`（文件顶部的导入加上 `import numpy as np`，以及 `FRAMES_FILE, noisy_blocks, same_slide`；把文件里已有的 `"frames.json"` 字面量全部换成 `FRAMES_FILE`）

```python
@pytest.fixture(scope="module")
def webcam_video(tmp_path_factory):
    """和 video 一样的三页幻灯片,右上角多一个每个关键帧都在变的"讲者摄像头"小窗(190×100)。"""
    path = tmp_path_factory.mktemp("w") / "video.mp4"
    out = av.open(str(path), "w")
    st = out.add_stream("libx264", rate=2)
    st.width, st.height, st.pix_fmt = 1280, 720, "yuv420p"
    st.codec_context.gop_size = 4
    for i in range(120):
        img = slide(SLIDES[i // 40])
        gray = 40 if (i // 4) % 2 == 0 else 200          # 每 4 帧一个关键帧,相邻关键帧的小窗一明一暗
        img.paste((gray, gray, gray), (1090, 0, 1280, 100))
        for p in st.encode(av.VideoFrame.from_image(img)):
            out.mux(p)
    for p in st.encode():
        out.mux(p)
    out.close()
    return path


def test_noisy_blocks_marks_only_the_webcam_corner(webcam_video, video):
    mask = noisy_blocks(webcam_video)
    assert mask.shape == (9, 16) and mask[0, 14:].all()
    assert mask.sum() <= 8 and not mask[3:, :].any() and not mask[:, :12].any()
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
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd agentflow-media && .venv/bin/python -m pytest tests/test_frames.py -q`
Expected: FAIL，`ImportError: cannot import name 'FRAMES_FILE'`

- [ ] **Step 3: 实现**（`app/frames.py` 按上面的 Interfaces；`app/main.py` 的 `get_frames` 改读 `FRAMES_FILE`）。`frames_media` 顶上原来的 `ponytail:` 注释换成：`# ponytail: 一直在动的块是按整段视频统计的。镜头对着讲台超过一半时长的录像,所有块都会被当成"一直在动",幻灯片就找不到了;真遇到再改成按时间窗口统计`。

- [ ] **Step 4: 跑全部测试确认通过**

Run: `cd agentflow-media && .venv/bin/python -m pytest -q`
Expected: 全部通过（81 个）。

- [ ] **Step 5: 提交并推送**

```bash
git add agentflow-media && git commit -m "fix(media): 截帧忽略一直在动的区域,文字相近的帧只留一张" && git push
```

- [ ] **Step 6: 在真实的那一讲上量效果**

```bash
ssh Upton@100.65.10.56 'cd ~/project/agentflow-drill && git pull -q --ff-only && pkill -f "[u]vicorn app.main:app --host 127.0.0.1 --port 8010"; sleep 2
cd agentflow-media; MEDIA_ROOT=$HOME/agentflow-media-data HF_HOME=$HOME/agentflow-media-data/hf setsid nohup ./run.sh > ~/agentflow-media-data/media.log 2>&1 < /dev/null &
sleep 5; K=2032f640a62a2633d5293a4d0b2a621f
time curl -s -m 1800 -X POST http://127.0.0.1:8010/media/$K/frames; echo
.venv/bin/python - <<EOF
import json, difflib
d = "/home/Upton/agentflow-media-data/media/$K/"
new = json.load(open(d + "frames.v2.json")); old = json.load(open(d + "frames.json"))["frames"]
n = lambda t: "".join(t.split()).lower()
same = lambda a, b: difflib.SequenceMatcher(None, n(a), n(b), autojunk=False).ratio() >= 0.8
distinct = []
for f in old:
    if not distinct or not same(distinct[-1]["text"], f["text"]): distinct.append(f)
found = sum(any(same(o["text"], f["text"]) for f in new["frames"]) for o in distinct)
print("keyframes", new["keyframes"], "ocred", new["ocred"], "kept", len(new["frames"]),
      "| old distinct slides", len(distinct), "found again", found, f"({found/len(distinct):.1%})")
EOF'
```

Expected: `ocred` 小于 300（原来 974），`kept` 小于 250（原来 896），用时小于 300 秒（原来 806 秒），旧结果里不同的页有 95% 以上还能在新结果里找到。**任何一项不达标就停下来查原因**（先看哪些块被标成了"一直在动"、漏掉的是哪些页），不要直接调阈值凑数。

- [ ] **Step 7: 把实测写进 `docs/benchmarks.md` 第 7 节**（在 "What the spot check found" 后面加一小节 "Frame dedup (fixed)"：改之前和改之后的 OCR 次数、保留帧数、用时、找回率），提交并推送：`git commit -m "docs: 截帧去重前后的实测数据"`。

---

### Task 2: 课程环境和测试的跑法

**Files:**
- Create: `docker/course.yml`、`scripts/course-env.sh`（可执行）
- Modify: `agentflow-media/run.sh`（监听地址改成 `"${MEDIA_HOST:-127.0.0.1}"`）、`README.zh.md`（媒体服务一节加两行：课程环境怎么起、测试怎么跑）

**Interfaces:**
- Produces: `scripts/course-env.sh test-up | test-java <mvn 参数> | test-rag [pytest 参数] | test-down | up | down`。`test-*` 用只有中间件的项目 `agentflow-coursetest`（测试和在跑的 server/worker 共用 Kafka 会互相抢消息）；`up` 起整套 `agentflow-course`，server 在 `127.0.0.1:18090`。

- [ ] **Step 1: 写 `docker/course.yml`**

```yaml
# 课程录播专用:独立的容器名和数据卷,不占宿主机端口(server 除外,映射到 18090)。用法见 scripts/course-env.sh。
# worker 固定用模拟模型:.env 里可能是真模型和 key。两个 worker 各写一遍:compose 2.40 下经锚点引用的 !reset 不生效。
services:
  mysql: {container_name: !reset null, ports: !reset []}
  redis: {container_name: !reset null, ports: !reset []}
  mongo: {container_name: !reset null, ports: !reset []}
  kafka: {container_name: !reset null, ports: !reset []}
  pgvector: {container_name: !reset null, ports: !reset []}
  rag: {container_name: !reset null, ports: !reset []}
  server:
    container_name: !reset null
    ports: !override ["127.0.0.1:18090:8080"]
    extra_hosts: ["host.docker.internal:host-gateway"]
    environment: {AGENTFLOW_COURSE_ENABLED: "true", MEDIA_BASEURL: "http://host.docker.internal:8010"}
  worker-1:
    container_name: !reset null
    extra_hosts: ["host.docker.internal:host-gateway"]
    environment: {LLM_PROVIDER: mock, LLM_DEFAULTMODEL: mock-small, LLM_MODEL: mock-small, LLM_MOCK_DELAYMS: "0",
                  OPENAI_API_KEY: "", TAVILY_API_KEY: "", MEDIA_BASEURL: "http://host.docker.internal:8010"}
  worker-2:
    container_name: !reset null
    extra_hosts: ["host.docker.internal:host-gateway"]
    environment: {LLM_PROVIDER: mock, LLM_DEFAULTMODEL: mock-small, LLM_MODEL: mock-small, LLM_MOCK_DELAYMS: "0",
                  OPENAI_API_KEY: "", TAVILY_API_KEY: "", MEDIA_BASEURL: "http://host.docker.internal:8010"}
```

- [ ] **Step 2: 写 `scripts/course-env.sh`**

```bash
#!/usr/bin/env bash
# 课程录播的两套隔离环境(都叠加 docker/course.yml,都固定用模拟模型):
#   agentflow-coursetest  只有中间件,给测试用(测试和在跑的 server/worker 共用 Kafka 会互相抢消息)
#   agentflow-course      整套服务,server 在 127.0.0.1:18090
# 在有 Docker 的机器上运行。媒体服务(agentflow-media)要另外启动,并且监听容器能访问到的地址,见 README。
set -euo pipefail
cd "$(dirname "$0")/.."
T() { docker compose -p agentflow-coursetest -f docker-compose.yml -f docker/course.yml "$@"; }
C() { docker compose -p agentflow-course -f docker-compose.yml -f docker/course.yml "$@"; }
ME="$(id -u):$(id -g)"
case "${1:-}" in
  test-up)   T up -d --wait mysql redis mongo kafka pgvector && T build -q rag ;;
  test-java) shift
    docker run --rm --user "$ME" --network agentflow-coursetest_default -v "$PWD":/src -v "$HOME/.m2":/m2 -w /src \
      -e SPRING_DATASOURCE_URL='jdbc:mysql://mysql:3306/agentflow?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai' \
      -e SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:19092 -e SPRING_DATA_REDIS_HOST=redis \
      -e SPRING_DATA_MONGODB_URI=mongodb://mongo:27017/agentflow_trace \
      maven:3.9-eclipse-temurin-21 mvn -B -q -Duser.home=/m2 -Dmaven.repo.local=/m2/repository "$@" ;;
  test-rag)  shift
    docker run --rm --user "$ME" --network agentflow-coursetest_default -v "$PWD/agentflow-rag":/app -w /app \
      -e HOME=/tmp -e PYTHONDONTWRITEBYTECODE=1 \
      -e RAG_DB_URL=postgresql://postgres:agentflow@pgvector:5432/agentflow_rag_test \
      agentflow-coursetest-rag python -m pytest -q -p no:cacheprovider "$@" ;;
  test-down) T down -v ;;
  up)        C up -d --build --wait ;;
  down)      C down ;;
  *) echo "usage: $0 test-up | test-java <mvn args> | test-rag [pytest args] | test-down | up | down" >&2; exit 2 ;;
esac
```

- [ ] **Step 3: 同步到服务器，起测试环境，跑现有的全部测试作为基线**

```bash
SYNC && R './scripts/course-env.sh test-up && sleep 60 && ./scripts/course-env.sh test-java test 2>&1 | tail -15; ./scripts/course-env.sh test-rag 2>&1 | tail -3'
```

Expected: Java 没有失败的测试（`mvn -q` 成功时几乎没有输出，失败会打印 `Tests run: … Failures`）；rag 最后一行是 `… passed`。`sleep 60` 是因为 Kafka 刚起来时消费组还没稳定。**基线不绿就停下来报告**（先分清是环境问题还是代码问题），不要带着红的基线往下做。

- [ ] **Step 4: 让容器能访问到媒体服务**

```bash
ssh Upton@100.65.10.56 'GW=$(docker network inspect bridge -f "{{(index .IPAM.Config 0).Gateway}}"); echo "gateway=$GW"
cd ~/project/agentflow-drill && git pull -q --ff-only; pkill -f "[u]vicorn app.main:app --host 127.0.0.1 --port 8010"; sleep 2; cd agentflow-media
MEDIA_HOST=$GW MEDIA_ROOT=$HOME/agentflow-media-data HF_HOME=$HOME/agentflow-media-data/hf setsid nohup ./run.sh > ~/agentflow-media-data/media.log 2>&1 < /dev/null &
sleep 5; docker run --rm --add-host host.docker.internal:host-gateway curlimages/curl:8.11.1 -s -m 5 http://host.docker.internal:8010/health'
```

（这一步要先提交并推送 `run.sh` 的改动，服务器上才 `git pull` 得到。）
Expected: `{"status":"ok","asrModel":"large-v3"}`。访问不到就停下来报告：说明这台机器上容器到宿主机的路由和预想的不一样，不要改成监听 `0.0.0.0`（那会把服务暴露到局域网）。

- [ ] **Step 5: 提交并推送**（`git commit -m "chore: 课程录播的隔离环境和测试脚本"`；Step 4 需要的 `run.sh` 改动可以先单独提交）

---

### Task 3: worker 调媒体服务；不可重试的错误

链接写错这类错误重试也没用。现在所有失败都要走完 5 秒、30 秒、5 分钟三次重试才算失败，等于写错一个链接要等近 6 分钟才看到结果。

**Files:**
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/media/MediaClient.java`、`MediaProperties.java`、`agentflow-worker/src/main/java/com/agentflow/worker/retry/NonRetryableException.java`、`agentflow-worker/src/test/java/com/agentflow/worker/media/MediaClientTest.java`
- Modify: `agentflow-worker/src/main/java/com/agentflow/worker/listener/SubtaskListener.java`、`agentflow-worker/src/test/java/com/agentflow/worker/listener/SubtaskListenerTest.java`、`agentflow-worker/src/main/resources/application.yml`（加 `media.base-url: http://localhost:8010`）

**Interfaces:**
- Produces:
  - `NonRetryableException extends RuntimeException`（构造参数 `String message`）。
  - `MediaProperties`（`@ConfigurationProperties("media")`，注册方式和 `RagProperties` 一样）：`baseUrl = "http://localhost:8010"`、`timeoutMs = 1_200_000`。
  - `MediaClient`（`@Component`，构造参数 `MediaProperties`，JDK `HttpClient`、固定 HTTP/1.1，和 `RagClient` 同样的写法）：
    - `JsonNode importMedia(String url, String path)`：`POST /media/import`，body 只带不为 null 的那个。
    - `JsonNode transcribe(String mediaKey)`、`JsonNode frames(String mediaKey)`：`POST /media/{key}/transcribe`、`/frames`。
    - `JsonNode transcript(String mediaKey)`、`JsonNode frameList(String mediaKey)`：`GET /media/{key}/transcript`、`/frames`。
    - 状态码 4xx → `NonRetryableException("media <状态码>: <响应里的 detail,最多 300 字>")`；其他非 2xx 和网络错误 → `IllegalStateException`（会重试）。
  - `SubtaskListener`：处理器抛 `NonRetryableException` 时直接发失败的 `ResultMessage`（`errorMsg` 是异常的消息），记一条 `PROCESSED / FAILED_FOR_GOOD` 的 trace，不进重试路由。

- [ ] **Step 1: 写失败的测试** `MediaClientTest.java`

```java
package com.agentflow.worker.media;

import com.agentflow.worker.retry.NonRetryableException;
import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** MediaClient 单测:JDK HttpServer 起桩,记录收到的请求(离线可跑)。 */
class MediaClientTest {

    private static final String KEY = "0123456789abcdef0123456789abcdef";
    private HttpServer server;
    private final List<String> seen = new ArrayList<>();   // "METHOD path body"
    private int status = 200;
    private String reply = "{}";

    @BeforeEach
    void up() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", ex -> {
            String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            seen.add(ex.getRequestMethod() + " " + ex.getRequestURI().getPath() + " " + body);
            byte[] b = reply.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(status, b.length);
            ex.getResponseBody().write(b);
            ex.close();
        });
        server.start();
    }

    @AfterEach
    void down() {
        server.stop(0);
    }

    private MediaClient client() {
        MediaProperties props = new MediaProperties();
        props.setBaseUrl("http://localhost:" + server.getAddress().getPort());
        return new MediaClient(props);
    }

    @Test
    void importSendsOnlyTheSourceThatWasGiven() {
        reply = "{\"mediaKey\":\"" + KEY + "\",\"title\":\"L1\",\"durationMs\":7101000,\"video\":\"video.mp4\",\"cached\":false}";
        JsonNode meta = client().importMedia("https://youtu.be/abc", null);
        assertEquals(KEY, meta.get("mediaKey").asText());
        assertEquals("POST /media/import {\"url\":\"https://youtu.be/abc\"}", seen.get(0));
        client().importMedia(null, "lec1.mp4");
        assertEquals("POST /media/import {\"path\":\"lec1.mp4\"}", seen.get(1));
    }

    @Test
    void stepsAndReadsUseTheMediaKeyInThePath() {
        reply = "{\"utterances\":[{\"startMs\":0,\"endMs\":4200,\"text\":\"Hi.\"}],\"frames\":[]}";
        MediaClient c = client();
        c.transcribe(KEY);
        c.frames(KEY);
        assertEquals("Hi.", c.transcript(KEY).get("utterances").get(0).get("text").asText());
        assertTrue(c.frameList(KEY).get("frames").isEmpty());
        assertEquals(List.of("POST /media/" + KEY + "/transcribe ", "POST /media/" + KEY + "/frames ",
                "GET /media/" + KEY + "/transcript ", "GET /media/" + KEY + "/frames "), seen);
    }

    @Test
    void aBadRequestIsNotWorthRetrying() {
        status = 400;
        reply = "{\"detail\":\"only YouTube links are accepted, got: 'https://evil.example/x'\"}";
        NonRetryableException e = assertThrows(NonRetryableException.class,
                () -> client().importMedia("https://evil.example/x", null));
        assertTrue(e.getMessage().startsWith("media 400: only YouTube links are accepted"), e.getMessage());
    }

    @Test
    void aServerSideFailureIsRetried() {
        status = 502;
        reply = "{\"detail\":\"download failed: HTTP Error 403\"}";
        RuntimeException e = assertThrows(RuntimeException.class, () -> client().importMedia("https://youtu.be/abc", null));
        assertFalse(e instanceof NonRetryableException);
        assertTrue(e.getMessage().contains("502"), e.getMessage());
    }
}
```

- [ ] **Step 2: 在 `SubtaskListenerTest.java` 里加一个用例**（放在类的最后；需要的话补 `assertFalse` 的静态导入和 `SubtaskProcessor`、`NonRetryableException` 的导入）

```java
    @Test
    void aFailureThatRetryingCannotFixIsReportedAtOnce() {
        SubtaskProcessor bad = new SubtaskProcessor() {
            @Override
            public String type() {
                return "BAD_INPUT";
            }

            @Override
            public String process(String inputJson) {
                throw new NonRetryableException("media 400: only YouTube links are accepted");
            }
        };
        SubtaskListener l = new SubtaskListener(java.util.List.of(bad), template, guard, retryRouter,
                mock(TraceEmitter.class));
        l.onMessage(new SubtaskMessage(1L, 13L, "uuid-4", "BAD_INPUT", 0, "{}"), null);
        ArgumentCaptor<ResultMessage> captor = ArgumentCaptor.forClass(ResultMessage.class);
        verify(template).send(eq(Topics.RESULT), eq("13"), captor.capture());
        assertFalse(captor.getValue().isSuccess());
        assertEquals("media 400: only YouTube links are accepted", captor.getValue().getErrorMsg());
        verify(retryRouter, never()).route(any(), org.mockito.ArgumentMatchers.anyInt(), any());
    }
```

- [ ] **Step 3: 跑测试确认失败**

Run: `mvn -q -pl agentflow-worker -am test -Dtest='MediaClientTest,SubtaskListenerTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败，找不到 `MediaClient`、`NonRetryableException`。

- [ ] **Step 4: 实现**（按 Interfaces）

- [ ] **Step 5: 跑测试确认通过**

Run: 同 Step 3。Expected: `BUILD SUCCESS`，两个类的用例全部通过。

- [ ] **Step 6: 提交并推送**（`git commit -m "feat(worker): 媒体服务客户端;重试也没用的错误直接判失败"`）

---

### Task 4: 片段表和按课程隔离的检索（agentflow-rag）

**Files:**
- Create: `agentflow-rag/app/segments.py`、`agentflow-rag/tests/test_segments.py`
- Modify: `agentflow-rag/app/main.py`（两个路由，`lifespan` 里加 `segments.init()`）、`agentflow-rag/app/store.py`（把"向量列的模型或维度变了就重新编码"抽成按表名参数化的函数，两张表共用；`documents` 的行为不变）

**Interfaces:**
- Produces（`app.segments`）：
  - 表 `segments(id text PRIMARY KEY, course_id bigint NOT NULL, lecture_id bigint NOT NULL, start_ms bigint NOT NULL, end_ms bigint NOT NULL, kind text NOT NULL, content text NOT NULL, embedding vector(D) NOT NULL)`，索引 `(course_id, lecture_id)`。
  - `init() -> None`：建表，同步向量列，从库里按课程重建 BM25。
  - `index(course_id: int, lecture_id: int, segs: list[dict]) -> int`：一个事务里先删这一讲的行再插入；提交后重建这门课的 BM25。
  - `search(course_id: int, query: str, k: int, lecture_ids: list[int] | None = None, mode: str = "hybrid") -> list[dict]`：返回 `[{"id","lectureId","startMs","endMs","kind","text","score"}]`。
  - HTTP：见"接口约定"。`kind` 不是 `speech`/`slide`、`topK` 不在 1 到 50 → 422。
- BM25 一门课一个内存索引（`dict[int, BM25]`），复用 `app.rank` 的 `BM25` 和 `rrf`。按讲过滤是在这门课的前 100 个词法候选里再筛，加一行注释：`# ponytail: 讲次过滤在全课前 100 个候选之后做,课很大、过滤很窄时词法候选会不足 k 个;真遇到再给每讲建索引`。

- [ ] **Step 1: 写失败的测试** `agentflow-rag/tests/test_segments.py`

```python
import random

import pytest
from fastapi.testclient import TestClient

from app import segments
from app.main import app


def seg(start_s, text, kind="speech", dur_s=60):
    return {"startMs": start_s * 1000, "endMs": (start_s + dur_s) * 1000, "kind": kind, "text": text}


LECTURE_1 = [seg(0, "Welcome. Today we cover pre-training of large language models."),
             seg(60, "Gradient descent updates the parameters using the gradient of the loss."),
             seg(120, "Reinforcement learning from human feedback aligns the model.")]


@pytest.fixture
def course(db):
    segments.init()
    return random.randrange(10 ** 6, 10 ** 9)      # 每个用例一门新课,互不影响,也不用清表


def test_search_returns_the_segment_with_its_time(course):
    assert segments.index(course, 1, LECTURE_1) == 3
    hit = segments.search(course, "how does gradient descent work", 1)[0]
    assert hit["id"] == "1:speech:60000" and hit["lectureId"] == 1 and hit["kind"] == "speech"
    assert hit["startMs"] == 60000 and hit["endMs"] == 120000 and "Gradient descent" in hit["text"]


@pytest.mark.parametrize("mode", ["hybrid", "dense", "bm25"])
def test_search_never_crosses_courses(course, mode):
    other = course + 1
    segments.index(course, 1, LECTURE_1)
    segments.index(other, 9, [seg(0, "Gradient descent, explained in the other course.")])
    hits = segments.search(course, "gradient descent", 10, mode=mode)
    assert hits and {h["lectureId"] for h in hits} == {1}
    assert {h["lectureId"] for h in segments.search(other, "gradient descent", 10, mode=mode)} == {9}


def test_search_can_be_limited_to_some_lectures(course):
    segments.index(course, 1, LECTURE_1)
    segments.index(course, 2, [seg(0, "Stochastic gradient descent uses one sample per step.")])
    assert {h["lectureId"] for h in segments.search(course, "gradient descent", 10)} == {1, 2}
    assert {h["lectureId"] for h in segments.search(course, "gradient descent", 10, lecture_ids=[2])} == {2}


def test_reindex_replaces_the_lecture(course):
    segments.index(course, 1, [seg(0, "Kafka partitions are rebalanced across consumers.")])
    assert segments.index(course, 1, [seg(0, "Redis runs Lua scripts atomically.")]) == 1
    for mode in ("bm25", "dense", "hybrid"):
        hits = segments.search(course, "kafka partitions rebalanced", 10, mode=mode)
        assert all("Kafka" not in h["text"] for h in hits), mode
    assert len(segments.search(course, "redis lua", 10)) == 1


def test_an_empty_list_clears_the_lecture(course):
    segments.index(course, 1, LECTURE_1)
    assert segments.index(course, 1, []) == 0 and segments.search(course, "gradient descent", 10) == []


def test_a_course_without_segments_has_no_hits(course):
    assert segments.search(course, "anything", 5) == []


def test_slide_and_speech_at_the_same_second_are_both_kept(course):
    segments.index(course, 1, [seg(60, "The speaker explains attention."),
                               seg(60, "Attention is all you need", kind="slide")])
    assert {h["id"] for h in segments.search(course, "attention", 10)} == {"1:speech:60000", "1:slide:60000"}


def test_lexical_index_is_rebuilt_from_the_table(course):
    segments.index(course, 1, LECTURE_1)
    segments.init()                                  # 等于服务重启
    assert segments.search(course, "reinforcement learning", 1, mode="bm25")[0]["startMs"] == 120000


def test_http_index_and_search(course):
    with TestClient(app) as c:
        r = c.post("/segments/index", json={"courseId": course, "lectureId": 1, "segments": LECTURE_1})
        assert r.status_code == 200 and r.json() == {"indexed": 3}
        hits = c.post("/segments/search", json={"courseId": course, "query": "pre-training", "topK": 2}).json()["hits"]
        assert hits[0]["startMs"] == 0 and set(hits[0]) == {"id", "lectureId", "startMs", "endMs", "kind", "text", "score"}
        bad = {"courseId": course, "lectureId": 1, "segments": [seg(0, "x", kind="video")]}
        assert c.post("/segments/index", json=bad).status_code == 422
        assert c.post("/segments/search", json={"courseId": course, "query": "x", "topK": 51}).status_code == 422
```

- [ ] **Step 2: 跑测试确认失败**

Run: `SYNC && R './scripts/course-env.sh test-rag tests/test_segments.py 2>&1 | tail -5'`
Expected: FAIL，`ImportError: cannot import name 'segments'`

- [ ] **Step 3: 实现**（按 Interfaces）

- [ ] **Step 4: 跑 rag 的全部测试确认通过**

Run: `SYNC && R './scripts/course-env.sh test-rag 2>&1 | tail -3'`
Expected: 全部通过，原有用例一个不少。

- [ ] **Step 5: 提交并推送**（`git commit -m "feat(rag): 课程片段表,按课程隔离的混合检索"`）

---

### Task 5: 分段（纯函数）

**Files:**
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/lecture/Segmenter.java`、`agentflow-worker/src/test/java/com/agentflow/worker/lecture/SegmenterTest.java`

**Interfaces:**
- Produces（`com.agentflow.worker.lecture.Segmenter`，全是静态方法，不依赖 Spring）：
  - `record Segment(long startMs, long endMs, String kind, String text)`（嵌套在 `Segmenter` 里）。
  - `static List<Segment> speech(JsonNode utterances)`：输入是媒体服务逐字稿里的 `utterances` 数组。
  - `static List<Segment> slides(JsonNode frames, long durationMs)`：输入是帧列表的 `frames` 数组；每帧一个片段，从这一帧的 `tsMs` 到下一帧的 `tsMs`（最后一帧到 `durationMs`）。
  - 常量：`TARGET_MS = 45_000`、`MAX_MS = 90_000`、`OVERLAP_MS = 10_000`。
- 语音分段规则：依次把 utterance 加进当前片段；加完之后，片段跨度达到 `TARGET_MS` 且这条 utterance 以 `. ? ! 。 ？ ！` 结尾，或者跨度达到 `MAX_MS`，就收尾。下一个片段从上一个片段末尾的几条 utterance 开始（从后往前取，总时长不超过 `OVERLAP_MS`；一条都放不下就不重叠）。输入结束时，当前片段如果只有重叠带过来的内容就不输出。片段的文字是各条 utterance 用空格连接。
- 上限 90 秒是为了不超过向量模型的 512 token 窗口（每分钟约 155 个词）；设计文档写的是 1 到 2 分钟，这里收紧了。

- [ ] **Step 1: 写失败的测试** `SegmenterTest.java`

```java
package com.agentflow.worker.lecture;

import com.agentflow.worker.lecture.Segmenter.Segment;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SegmenterTest {

    private static final ObjectMapper OM = new ObjectMapper();

    /** n 条 utterance,每条 10 秒、首尾相接;编号在 sentenceEnds 里的以句号结尾。 */
    private static ArrayNode utterances(int n, Set<Integer> sentenceEnds) {
        ArrayNode arr = OM.createArrayNode();
        for (int i = 0; i < n; i++) {
            arr.addObject().put("startMs", i * 10_000L).put("endMs", (i + 1) * 10_000L)
                    .put("text", "u" + i + (sentenceEnds.contains(i) ? "." : ""));
        }
        return arr;
    }

    @Test
    void cutsAtTheFirstSentenceEndAfterTheTarget() {
        List<Segment> segs = Segmenter.speech(utterances(12, Set.of(1, 5)));
        assertEquals(2, segs.size());
        assertEquals(new Segment(0, 60_000, "speech", "u0 u1. u2 u3 u4 u5."), segs.get(0));   // u1 的句号太早,不切
        assertEquals(50_000, segs.get(1).startMs());                                           // 带上一段的最后 10 秒
        assertEquals(120_000, segs.get(1).endMs());
        assertTrue(segs.get(1).text().startsWith("u5. u6"));
    }

    @Test
    void cutsAtTheHardCapWhenThereIsNoPunctuation() {
        List<Segment> segs = Segmenter.speech(utterances(20, Set.of()));
        assertEquals(List.of(0L, 80_000L, 160_000L), segs.stream().map(Segment::startMs).toList());
        assertEquals(List.of(90_000L, 170_000L, 200_000L), segs.stream().map(Segment::endMs).toList());
        String all = String.join(" ", segs.stream().map(Segment::text).toList());
        for (int i = 0; i < 20; i++) {
            assertTrue((" " + all + " ").contains(" u" + i + " "), "u" + i + " 没有进任何片段");
        }
    }

    @Test
    void aTailThatIsOnlyOverlapIsNotEmitted() {
        assertEquals(1, Segmenter.speech(utterances(6, Set.of(5))).size());
    }

    @Test
    void anUtteranceLongerThanTheCapIsItsOwnSegment() {
        ArrayNode arr = OM.createArrayNode();
        arr.addObject().put("startMs", 0).put("endMs", 200_000).put("text", "a very long monologue");
        arr.addObject().put("startMs", 200_000).put("endMs", 205_000).put("text", "Next.");
        List<Segment> segs = Segmenter.speech(arr);
        assertEquals(new Segment(0, 200_000, "speech", "a very long monologue"), segs.get(0));
        assertEquals(new Segment(200_000, 205_000, "speech", "Next."), segs.get(1));              // 200 秒的那条放不进重叠
    }

    @Test
    void emptyTranscriptGivesNoSegments() {
        assertTrue(Segmenter.speech(OM.createArrayNode()).isEmpty());
        assertTrue(Segmenter.slides(OM.createArrayNode(), 60_000).isEmpty());
    }

    @Test
    void chinesePunctuationAlsoEndsASentence() {
        ArrayNode arr = OM.createArrayNode();
        for (int i = 0; i < 8; i++) {
            arr.addObject().put("startMs", i * 10_000L).put("endMs", (i + 1) * 10_000L)
                    .put("text", i == 4 ? "这是梯度下降。" : "第" + i + "句");
        }
        assertEquals(50_000, Segmenter.speech(arr).get(0).endMs());
    }

    @Test
    void eachSlideLastsUntilTheNextOne() throws Exception {
        List<Segment> segs = Segmenter.slides(OM.readTree(
                "[{\"tsMs\":0,\"file\":\"frames/000000000.jpg\",\"text\":\"Title\"},"
                        + "{\"tsMs\":20000,\"file\":\"frames/000020000.jpg\",\"text\":\"SGD: one sample per step\"}]"), 60_000);
        assertEquals(List.of(new Segment(0, 20_000, "slide", "Title"),
                new Segment(20_000, 60_000, "slide", "SGD: one sample per step")), segs);
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn -q -pl agentflow-worker -am test -Dtest=SegmenterTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败，找不到 `Segmenter`。

- [ ] **Step 3: 实现 `Segmenter`**（按 Interfaces 里的规则）

- [ ] **Step 4: 跑测试确认通过**（同 Step 2，Expected: 7 个用例通过）

- [ ] **Step 5: 提交并推送**（`git commit -m "feat(worker): 逐字稿按句子边界分段,幻灯片按出现时间分段"`）

---

### Task 6: 导入、转写、截帧、建索引四个处理器

**Files:**
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/lecture/LecImportProcessor.java`、`LecTranscribeProcessor.java`、`LecFramesProcessor.java`、`LecIndexProcessor.java`、`agentflow-worker/src/test/java/com/agentflow/worker/lecture/LecProcessorsTest.java`
- Modify: `agentflow-worker/src/main/java/com/agentflow/worker/rag/RagClient.java`、`RagProperties.java`（加 `indexTimeoutMs = 120_000`）、`agentflow-worker/src/test/java/com/agentflow/worker/rag/RagClientTest.java`

**Interfaces:**
- Consumes: `MediaClient`、`NonRetryableException`（任务 3）、`Segmenter`（任务 5）、`POST /segments/index`（任务 4）。
- Produces:
  - `RagClient.indexSegments(long courseId, long lectureId, List<Segmenter.Segment> segments) -> int`：发 `POST /segments/index`，返回 `indexed`；超时用 `indexTimeoutMs`。
  - 四个 `SubtaskProcessor`（都是 `@Component`，构造参数见测试），输入输出见"接口约定"：
    - `LecImportProcessor`（`LEC_IMPORT`）：输出里的 `hasVideo` = 媒体服务 meta 的 `video` 不为 null。
    - `LecTranscribeProcessor`（`LEC_TRANSCRIBE`）、`LecFramesProcessor`（`LEC_FRAMES`）：把媒体服务返回的计数和 `cached` 透传出去。
    - `LecIndexProcessor`（`LEC_INDEX`）：取逐字稿 → `Segmenter.speech`；`withSlides` 为真时取帧列表 → `Segmenter.slides`；一个语音片段都没有 → `NonRetryableException("这一讲没有识别出语音")`；否则 `indexSegments`，输出 `{"speech":n,"slides":m}`。

- [ ] **Step 1: 写失败的测试** `LecProcessorsTest.java`

```java
package com.agentflow.worker.lecture;

import com.agentflow.worker.lecture.Segmenter.Segment;
import com.agentflow.worker.media.MediaClient;
import com.agentflow.worker.rag.RagClient;
import com.agentflow.worker.retry.NonRetryableException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LecProcessorsTest {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final String KEY = "0123456789abcdef0123456789abcdef";
    private final MediaClient media = mock(MediaClient.class);
    private final RagClient rag = mock(RagClient.class);

    private static JsonNode json(String s) throws Exception {
        return OM.readTree(s);
    }

    @Test
    void importPassesTheLinkAndReportsWhatWasStored() throws Exception {
        when(media.importMedia("https://youtu.be/abc", null)).thenReturn(json(
                "{\"mediaKey\":\"" + KEY + "\",\"title\":\"LLM Agents Overview\",\"durationMs\":7101000,"
                        + "\"audio\":\"audio.m4a\",\"video\":\"video.mp4\",\"cached\":false}"));
        LecImportProcessor p = new LecImportProcessor(media);
        assertEquals("LEC_IMPORT", p.type());
        JsonNode out = json(p.process("{\"lectureId\":7,\"url\":\"https://youtu.be/abc\"}"));
        assertEquals(KEY, out.get("mediaKey").asText());
        assertEquals("LLM Agents Overview", out.get("title").asText());
        assertEquals(7101000, out.get("durationMs").asLong());
        assertTrue(out.get("hasVideo").asBoolean());
    }

    @Test
    void importOfALocalFileSendsThePath() throws Exception {
        when(media.importMedia(null, "lec1.wav")).thenReturn(json(
                "{\"mediaKey\":\"" + KEY + "\",\"title\":\"lec1\",\"durationMs\":2000,\"video\":null}"));
        JsonNode out = json(new LecImportProcessor(media).process("{\"lectureId\":7,\"path\":\"lec1.wav\"}"));
        assertFalse(out.get("hasVideo").asBoolean());
    }

    @Test
    void transcribeAndFramesReportCounts() throws Exception {
        when(media.transcribe(KEY)).thenReturn(json("{\"mediaKey\":\"" + KEY + "\",\"utterances\":1298,\"cached\":false}"));
        when(media.frames(KEY)).thenReturn(json("{\"mediaKey\":\"" + KEY + "\",\"frames\":120,\"cached\":true}"));
        String in = "{\"lectureId\":7,\"mediaKey\":\"" + KEY + "\"}";
        LecTranscribeProcessor t = new LecTranscribeProcessor(media);
        LecFramesProcessor f = new LecFramesProcessor(media);
        assertEquals("LEC_TRANSCRIBE", t.type());
        assertEquals("LEC_FRAMES", f.type());
        assertEquals(1298, json(t.process(in)).get("utterances").asInt());
        JsonNode frames = json(f.process(in));
        assertEquals(120, frames.get("frames").asInt());
        assertTrue(frames.get("cached").asBoolean());
    }

    private void transcript(String utterances) throws Exception {
        when(media.transcript(KEY)).thenReturn(json("{\"model\":\"large-v3\",\"language\":\"en\",\"utterances\":" + utterances + "}"));
    }

    private static String indexInput(boolean withSlides) {
        return "{\"courseId\":3,\"lectureId\":7,\"mediaKey\":\"" + KEY + "\",\"durationMs\":60000,\"withSlides\":" + withSlides + "}";
    }

    @Test
    @SuppressWarnings("unchecked")
    void indexSendsSpeechAndSlideSegmentsOfTheLecture() throws Exception {
        transcript("[{\"startMs\":0,\"endMs\":5000,\"text\":\"Welcome.\"},{\"startMs\":5000,\"endMs\":9000,\"text\":\"Today: agents.\"}]");
        when(media.frameList(KEY)).thenReturn(json("{\"frames\":[{\"tsMs\":0,\"file\":\"frames/000000000.jpg\",\"text\":\"Title slide\"},"
                + "{\"tsMs\":20000,\"file\":\"frames/000020000.jpg\",\"text\":\"Agenda\"}]}"));
        when(rag.indexSegments(anyLong(), anyLong(), anyList())).thenReturn(3);
        LecIndexProcessor p = new LecIndexProcessor(media, rag);
        assertEquals("LEC_INDEX", p.type());
        JsonNode out = json(p.process(indexInput(true)));
        assertEquals(1, out.get("speech").asInt());
        assertEquals(2, out.get("slides").asInt());
        ArgumentCaptor<List<Segment>> segs = ArgumentCaptor.forClass(List.class);
        verify(rag).indexSegments(eq(3L), eq(7L), segs.capture());
        assertEquals(List.of(new Segment(0, 9000, "speech", "Welcome. Today: agents."),
                new Segment(0, 20000, "slide", "Title slide"), new Segment(20000, 60000, "slide", "Agenda")), segs.getValue());
    }

    @Test
    void indexWithoutSlidesNeverAsksForFrames() throws Exception {
        transcript("[{\"startMs\":0,\"endMs\":5000,\"text\":\"Welcome.\"}]");
        when(rag.indexSegments(anyLong(), anyLong(), anyList())).thenReturn(1);
        JsonNode out = json(new LecIndexProcessor(media, rag).process(indexInput(false)));
        assertEquals(0, out.get("slides").asInt());
        verify(media, never()).frameList(any());
    }

    @Test
    void indexFailsForGoodWhenNothingWasSaid() throws Exception {
        transcript("[]");
        assertThrows(NonRetryableException.class, () -> new LecIndexProcessor(media, rag).process(indexInput(true)));
        verify(rag, never()).indexSegments(anyLong(), anyLong(), anyList());
    }
}
```

- [ ] **Step 2: 在 `RagClientTest.java` 里加一个用例**（`up()` 里再注册一个 `/segments/index` 的桩：把请求体存进字段 `lastBody`，返回 `{"indexed":2}`）

```java
    @Test
    void indexSegmentsPostsTheLectureAndReturnsTheCount() throws Exception {
        int n = client().indexSegments(3, 7, List.of(
                new com.agentflow.worker.lecture.Segmenter.Segment(0, 60000, "speech", "Welcome."),
                new com.agentflow.worker.lecture.Segmenter.Segment(0, 20000, "slide", "Title")));
        assertEquals(2, n);
        com.fasterxml.jackson.databind.JsonNode body = new com.fasterxml.jackson.databind.ObjectMapper().readTree(lastBody);
        assertEquals(3, body.get("courseId").asLong());
        assertEquals(7, body.get("lectureId").asLong());
        assertEquals("slide", body.get("segments").get(1).get("kind").asText());
        assertEquals(20000, body.get("segments").get(1).get("endMs").asLong());
        assertEquals("Welcome.", body.get("segments").get(0).get("text").asText());
    }
```

- [ ] **Step 3: 跑测试确认失败**

Run: `mvn -q -pl agentflow-worker -am test -Dtest='LecProcessorsTest,RagClientTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败，找不到四个处理器和 `indexSegments`。

- [ ] **Step 4: 实现**（按 Interfaces）

- [ ] **Step 5: 跑测试确认通过**（同 Step 3）

- [ ] **Step 6: 提交并推送**（`git commit -m "feat(worker): 讲次摄入的导入、转写、截帧、建索引处理器"`）

---

### Task 7: 知识点目录

每讲一次模型调用。模型看到的是按时间编号的片段，输出的是"知识点 + 片段编号"，时间由代码按编号查出来——模型没有机会写出一个不存在的时间。

**Files:**
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/lecture/LecConceptsProcessor.java`、`agentflow-worker/src/test/java/com/agentflow/worker/lecture/LecConceptsProcessorTest.java`
- Modify: `agentflow-worker/src/main/java/com/agentflow/worker/agent/Prompts.java`（加 `CONCEPTS_TAG = "[concepts]"` 和 `CONCEPTS`）、`agentflow-worker/src/main/java/com/agentflow/worker/llm/MockLlmClient.java`（加一个分支）、`agentflow-worker/src/test/java/com/agentflow/worker/llm/MockLlmClientTest.java`

**Interfaces:**
- Consumes: `MediaClient.transcript`、`Segmenter.speech`、`LlmGateway.chat`、`TextUtil.parseObject`、`Usage`。
- Produces:
  - `LecConceptsProcessor`（`LEC_CONCEPTS`，`@Component`，构造参数 `(MediaClient, LlmGateway, LlmModelProperties)`）。常量 `MAX_CONCEPTS = 30`、`MAX_TOKENS = 2000`。
  - 发给模型的消息：system 是 `Prompts.system(Prompts.CONCEPTS, <标题和第一个片段的文字>)`（语言跟着课程走）；user 是一行 `标题:<title>`，然后每个语音片段一行 `[<编号>] <mm:ss> <文字>`（编号从 0 起；分钟数可以超过 59，比如 `118:20`）。
  - 要求模型只输出 `{"concepts":[{"name":"…","segment":<编号>,"summary":"一句话"}]}`，10 到 30 条，`name` 用课上的原词。
  - 校验：`name` 去空白后非空（超过 80 字截断）；`segment` 是整数且在范围内；`summary` 超过 200 字截断。不合规的条目丢弃；同名只留最早的一条；按 `startMs` 排序；最多 30 条。一条合规的都没有（或者输出不是 JSON）→ `IllegalStateException`（会重试）。
  - 输出：`{"concepts":[{"name","startMs","summary"}],"usage":{…}}`（`usage` 和其他处理器一样用 `Usage.of`）。
  - `MockLlmClient`：system 以 `Prompts.CONCEPTS_TAG` 开头时，从 user 消息里找出所有 `[n]` 开头的行，每隔 5 个片段出一个知识点：`name` = `"知识点 " + (序号)`，`segment` = 那个编号，`summary` = 那一行文字的前 60 个字符。最多 30 个。

- [ ] **Step 1: 写失败的测试** `LecConceptsProcessorTest.java`

```java
package com.agentflow.worker.lecture;

import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.TokenUsage;
import com.agentflow.worker.agent.Prompts;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.llm.LlmModelProperties;
import com.agentflow.worker.media.MediaClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LecConceptsProcessorTest {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final String KEY = "0123456789abcdef0123456789abcdef";
    private static final String INPUT = "{\"lectureId\":7,\"mediaKey\":\"" + KEY + "\",\"title\":\"LLM Agents Overview\"}";
    private final MediaClient media = mock(MediaClient.class);
    private final LlmGateway gateway = mock(LlmGateway.class);

    /** 30 条 utterance,每条 20 秒、以句号结尾 → 每 3 条(60 秒)一个片段,片段 i 从 i*60 秒开始(20 秒的一条放不进 10 秒的重叠)。 */
    @BeforeEach
    void transcript() throws Exception {
        ArrayNode arr = OM.createArrayNode();
        for (int i = 0; i < 30; i++) {
            arr.addObject().put("startMs", i * 20_000L).put("endMs", (i + 1) * 20_000L).put("text", "Point " + i + ".");
        }
        when(media.transcript(KEY)).thenReturn(OM.createObjectNode().put("language", "en").set("utterances", arr));
    }

    private LecConceptsProcessor processor() {
        LlmModelProperties props = new LlmModelProperties();
        LlmModelProperties.Model m = new LlmModelProperties.Model();
        m.setName("m");
        props.setModels(List.of(m));
        return new LecConceptsProcessor(media, gateway, props);
    }

    private JsonNode run(String llmOutput) throws Exception {
        when(gateway.chat(any())).thenReturn(new ChatResponse(llmOutput, new TokenUsage(5, 5), "m"));
        return OM.readTree(processor().process(INPUT));
    }

    @Test
    void theModelSeesNumberedSegmentsWithTheirTimes() throws Exception {
        run("{\"concepts\":[{\"name\":\"Pre-training\",\"segment\":0,\"summary\":\"s\"}]}");
        ArgumentCaptor<ChatRequest> req = ArgumentCaptor.forClass(ChatRequest.class);
        verify(gateway).chat(req.capture());
        assertTrue(req.getValue().getMessages().get(0).getContent().startsWith(Prompts.CONCEPTS_TAG));
        String user = req.getValue().getMessages().get(req.getValue().getMessages().size() - 1).getContent();
        assertTrue(user.startsWith("标题:LLM Agents Overview\n[0] 00:00 Point 0. Point 1. Point 2."), user.substring(0, 80));
        assertTrue(user.contains("\n[1] 01:00 Point 3. Point 4. Point 5.\n[2] 02:00 "), "片段按编号和起始时间逐行列出");
        assertEquals("LEC_CONCEPTS", processor().type());
    }

    @Test
    void segmentNumbersBecomeStartTimesInOrder() throws Exception {
        JsonNode out = run("{\"concepts\":[{\"name\":\"RLHF\",\"segment\":3,\"summary\":\"reward from people\"},"
                + "{\"name\":\"Pre-training\",\"segment\":0,\"summary\":\"next token prediction\"}]}");
        JsonNode c = out.get("concepts");
        assertEquals(2, c.size());
        assertEquals("Pre-training", c.get(0).get("name").asText());
        assertEquals(0, c.get(0).get("startMs").asLong());
        assertEquals("RLHF", c.get(1).get("name").asText());
        assertEquals(180_000, c.get(1).get("startMs").asLong());
        assertEquals("reward from people", c.get(1).get("summary").asText());
        assertTrue(out.has("usage"));
    }

    @Test
    void dropsConceptsThatPointOutsideTheLecture() throws Exception {
        JsonNode c = run("{\"concepts\":[{\"name\":\"Good\",\"segment\":1,\"summary\":\"s\"},"
                + "{\"name\":\"Too far\",\"segment\":999,\"summary\":\"s\"},{\"name\":\"Negative\",\"segment\":-1,\"summary\":\"s\"},"
                + "{\"name\":\"  \",\"segment\":2,\"summary\":\"s\"},{\"name\":\"No number\",\"summary\":\"s\"},"
                + "{\"name\":\"Good\",\"segment\":5,\"summary\":\"again, later\"},{\"name\":\"Text number\",\"segment\":\"2\",\"summary\":\"s\"}]}")
                .get("concepts");
        assertEquals(1, c.size());
        assertEquals("Good", c.get(0).get("name").asText());
        assertEquals(60_000, c.get(0).get("startMs").asLong());
    }

    @Test
    void failsWhenNothingUsableComesBack() {
        assertThrows(IllegalStateException.class, () -> run("{\"concepts\":[]}"));
        assertThrows(IllegalStateException.class, () -> run("{\"concepts\":[{\"name\":\"x\",\"segment\":999}]}"));
        assertThrows(IllegalStateException.class, () -> run("I could not find any concepts, sorry."));
    }

    @Test
    void keepsAtMostThirty() throws Exception {
        StringBuilder sb = new StringBuilder("{\"concepts\":[");
        for (int i = 0; i < 40; i++) {
            sb.append(i == 0 ? "" : ",").append("{\"name\":\"c").append(i).append("\",\"segment\":0,\"summary\":\"s\"}");
        }
        assertEquals(30, run(sb.append("]}").toString()).get("concepts").size());
    }
}
```

- [ ] **Step 2: 在 `MockLlmClientTest.java` 里加一个用例**

```java
    @Test
    void conceptsPromptGetsConceptsThatPointAtRealSegments() throws Exception {
        String user = "标题:Demo\n[0] 00:00 first\n[1] 00:50 second\n[2] 01:40 third\n[3] 02:30 fourth\n[4] 03:20 fifth\n[5] 04:10 sixth";
        com.agentflow.common.llm.ChatResponse resp = new MockLlmClient().chat(new com.agentflow.common.llm.ChatRequest("mock-small",
                java.util.List.of(new com.agentflow.common.llm.ChatMessage("system", com.agentflow.worker.agent.Prompts.CONCEPTS),
                        new com.agentflow.common.llm.ChatMessage("user", user)), 2000));
        com.fasterxml.jackson.databind.JsonNode c = new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(resp.getContent()).get("concepts");
        org.junit.jupiter.api.Assertions.assertEquals(2, c.size());
        org.junit.jupiter.api.Assertions.assertEquals(0, c.get(0).get("segment").asInt());
        org.junit.jupiter.api.Assertions.assertEquals(5, c.get(1).get("segment").asInt());
        org.junit.jupiter.api.Assertions.assertEquals("sixth", c.get(1).get("summary").asText());
    }
```

- [ ] **Step 3: 跑测试确认失败**

Run: `mvn -q -pl agentflow-worker -am test -Dtest='LecConceptsProcessorTest,MockLlmClientTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败，找不到 `LecConceptsProcessor`、`Prompts.CONCEPTS_TAG`。

- [ ] **Step 4: 实现**（按 Interfaces）

- [ ] **Step 5: 跑 worker 模块里不需要中间件的测试确认没有弄坏别的**

Run: `mvn -q -pl agentflow-worker -am test -Dtest='Lec*Test,SegmenterTest,MockLlmClientTest,PromptsTest,MediaClientTest,RagClientTest,SubtaskListenerTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `BUILD SUCCESS`。

- [ ] **Step 6: 提交并推送**（`git commit -m "feat(worker): 每讲的知识点目录,时间由片段编号查出"`）

---

### Task 8: 两张表和 `LECTURE_INGEST` 的阶段推进

**Files:**
- Create: `docker/mysql/migrations/2026-11-13-course-video.sql`、`agentflow-server/src/main/java/com/agentflow/server/entity/CourseEntity.java`、`LectureEntity.java`、`agentflow-server/src/main/java/com/agentflow/server/mapper/CourseMapper.java`、`LectureMapper.java`、`agentflow-server/src/main/java/com/agentflow/server/coordinator/LectureIngestDecomposer.java`、`agentflow-server/src/test/java/com/agentflow/server/coordinator/LectureIngestDecomposerTest.java`、`agentflow-server/src/test/java/com/agentflow/server/service/LectureIngestFlowTest.java`
- Modify: `docker/mysql/init.sql`（末尾加同样的两张表，注释写明和迁移文件保持一致）

**Interfaces:**
- Produces:
  - 表（迁移文件开头照 `2026-11-09-stock-brief.sql` 的写法，注明已有数据卷的环境怎么手动执行）：

```sql
CREATE TABLE IF NOT EXISTS course (
  id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  name       VARCHAR(200) NOT NULL,
  created_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS lecture (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  course_id   BIGINT        NOT NULL,
  seq         INT           NOT NULL,          -- 课内序号,从 1 起,按添加顺序
  title       VARCHAR(300)  NULL,              -- 用户没填时,摄入完成后用视频自己的标题
  source_url  VARCHAR(1024) NULL,
  source_path VARCHAR(1024) NULL,
  status      VARCHAR(16)   NOT NULL,          -- INGESTING / READY / PARTIAL / FAILED
  media_key   CHAR(32)      NULL,
  duration_ms BIGINT        NULL,
  task_uuid   VARCHAR(36)   NULL,              -- 最近一次摄入任务
  concepts    JSON          NULL,              -- [{"name","startMs","summary"}]
  error       VARCHAR(512)  NULL,
  created_at  DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at  DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_course_seq (course_id, seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

  - `CourseEntity`、`LectureEntity`（Lombok `@Data`，`@TableName`，字段是上面各列的驼峰名；`concepts` 是 `String`）、`CourseMapper`、`LectureMapper`（`extends BaseMapper`）。
  - `LectureIngestDecomposer`（`@Component`，构造参数 `(ObjectMapper, LectureMapper)`）：`TYPE = "LECTURE_INGEST"`，五个 kind 常量。行为见"接口约定"和下面的测试。`onFinished` 按任务终态写讲次：`COMPLETED` → `READY`，`PARTIAL_FAILED` → `PARTIAL`，其他 → `FAILED`；写入 `mediaKey`、`durationMs`、`concepts`；`title` 只在原来为空时写；`error` 是第一个失败步骤的 `"<kind>: <error_msg>"`（最多 512 字），没有失败就是 null。

- [ ] **Step 1: 写失败的测试** `LectureIngestDecomposerTest.java`

```java
package com.agentflow.server.coordinator;

import com.agentflow.common.state.TaskStatus;
import com.agentflow.server.entity.LectureEntity;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.LectureMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LectureIngestDecomposerTest {

    private static final String KEY = "0123456789abcdef0123456789abcdef";
    private static final String IMPORTED = "{\"mediaKey\":\"" + KEY + "\",\"title\":\"LLM Agents Overview\","
            + "\"durationMs\":7101000,\"hasVideo\":true}";
    private static final String CONCEPTS = "{\"concepts\":[{\"name\":\"Pre-training\",\"startMs\":0,\"summary\":\"s\"}],\"usage\":{}}";

    private final ObjectMapper om = new ObjectMapper();
    private final LectureMapper lectures = mock(LectureMapper.class);
    private final LectureIngestDecomposer d = new LectureIngestDecomposer(om, lectures);

    private LectureEntity lecture(String url, String path, String title) {
        LectureEntity l = new LectureEntity();
        l.setId(7L);
        l.setCourseId(3L);
        l.setSourceUrl(url);
        l.setSourcePath(path);
        l.setTitle(title);
        l.setStatus("INGESTING");
        when(lectures.selectById(7L)).thenReturn(l);
        return l;
    }

    private static SubtaskEntity sub(int seq, String kind, String status, String output, String error) {
        SubtaskEntity s = new SubtaskEntity();
        s.setSeq(seq);
        s.setKind(kind);
        s.setStatus(status);
        s.setOutput(output);
        s.setErrorMsg(error);
        return s;
    }

    private static SubtaskEntity ok(int seq, String kind, String output) {
        return sub(seq, kind, "COMPLETED", output, null);
    }

    private static SubtaskEntity failed(int seq, String kind, String error) {
        return sub(seq, kind, "FAILED", null, error);
    }

    private static TaskEntity task(String status) {
        TaskEntity t = new TaskEntity();
        t.setType("LECTURE_INGEST");
        t.setStatus(status);
        t.setPayload("{\"lectureId\":7,\"courseId\":3}");
        return t;
    }

    /** 前 n 步都成功的子任务列表(n 最多 5)。 */
    private static List<SubtaskEntity> through(int n) {
        List<SubtaskEntity> all = List.of(ok(0, "LEC_IMPORT", IMPORTED), ok(1, "LEC_TRANSCRIBE", "{\"utterances\":1298}"),
                ok(2, "LEC_FRAMES", "{\"frames\":120}"), ok(3, "LEC_INDEX", "{\"speech\":150,\"slides\":120}"),
                ok(4, "LEC_CONCEPTS", CONCEPTS));
        return new ArrayList<>(all.subList(0, n));
    }

    private JsonNode in(SubtaskDef def) throws Exception {
        return om.readTree(def.inputJson());
    }

    @Test
    void decomposeIgnoresALinkInThePayload() throws Exception {
        lecture("https://youtu.be/abc", null, null);
        List<SubtaskDef> defs = d.decompose(om.readTree("{\"lectureId\":7,\"courseId\":3,\"url\":\"https://evil.example/x\"}"));
        assertEquals(1, defs.size());
        assertEquals("LEC_IMPORT", defs.get(0).kind());
        assertEquals(0, defs.get(0).seq());
        assertEquals("https://youtu.be/abc", in(defs.get(0)).get("url").asText());
        assertEquals(7, in(defs.get(0)).get("lectureId").asLong());
        assertFalse(in(defs.get(0)).has("path"));
    }

    @Test
    void decomposeUsesThePathOfALocalFile() throws Exception {
        lecture(null, "lec1.mp4", null);
        JsonNode input = in(d.decompose(om.readTree("{\"lectureId\":7,\"courseId\":3}")).get(0));
        assertEquals("lec1.mp4", input.get("path").asText());
        assertFalse(input.has("url"));
    }

    @Test
    void decomposeRejectsWhatIsNotAKnownLecture() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> d.decompose(null));
        assertThrows(IllegalArgumentException.class, () -> d.decompose(om.readTree("{}")));
        assertThrows(IllegalArgumentException.class, () -> d.decompose(om.readTree("{\"lectureId\":8,\"courseId\":3}")));
        lecture("https://youtu.be/abc", null, null);
        assertThrows(IllegalArgumentException.class, () -> d.decompose(om.readTree("{\"lectureId\":7,\"courseId\":4}")));
    }

    @Test
    void afterImportTranscriptionAndFramesRunSideBySide() throws Exception {
        List<SubtaskDef> next = d.nextStage(task("RUNNING"), through(1));
        assertEquals(List.of("LEC_TRANSCRIBE", "LEC_FRAMES"), next.stream().map(SubtaskDef::kind).toList());
        assertEquals(List.of(1, 2), next.stream().map(SubtaskDef::seq).toList());
        assertEquals(KEY, in(next.get(0)).get("mediaKey").asText());
        assertEquals(KEY, in(next.get(1)).get("mediaKey").asText());
    }

    @Test
    void theIndexIsBuiltWithSlidesOnlyWhenFramesSucceeded() throws Exception {
        List<SubtaskDef> next = d.nextStage(task("RUNNING"), through(3));
        assertEquals(List.of("LEC_INDEX"), next.stream().map(SubtaskDef::kind).toList());
        JsonNode input = in(next.get(0));
        assertEquals(3, next.get(0).seq());
        assertEquals(3, input.get("courseId").asLong());
        assertEquals(7, input.get("lectureId").asLong());
        assertEquals(KEY, input.get("mediaKey").asText());
        assertEquals(7101000, input.get("durationMs").asLong());
        assertTrue(input.get("withSlides").asBoolean());

        List<SubtaskEntity> framesFailed = through(2);
        framesFailed.add(failed(2, "LEC_FRAMES", "media 500: OCR crashed"));
        assertFalse(in(d.nextStage(task("RUNNING"), framesFailed).get(0)).get("withSlides").asBoolean());
    }

    @Test
    void afterTheIndexConceptsAreExtracted() throws Exception {
        List<SubtaskDef> next = d.nextStage(task("RUNNING"), through(4));
        assertEquals(List.of("LEC_CONCEPTS"), next.stream().map(SubtaskDef::kind).toList());
        assertEquals(4, next.get(0).seq());
        assertEquals(KEY, in(next.get(0)).get("mediaKey").asText());
        assertEquals("LLM Agents Overview", in(next.get(0)).get("title").asText());
    }

    @Test
    void aFailedStepThatOthersNeedEndsTheTask() {
        assertTrue(d.nextStage(task("RUNNING"), List.of(failed(0, "LEC_IMPORT", "media 400: bad link"))).isEmpty());
        List<SubtaskEntity> noTranscript = through(1);
        noTranscript.add(failed(1, "LEC_TRANSCRIBE", "CUDA out of memory"));
        noTranscript.add(ok(2, "LEC_FRAMES", "{\"frames\":120}"));
        assertTrue(d.nextStage(task("RUNNING"), noTranscript).isEmpty());
        List<SubtaskEntity> noIndex = through(3);
        noIndex.add(failed(3, "LEC_INDEX", "这一讲没有识别出语音"));
        assertTrue(d.nextStage(task("RUNNING"), noIndex).isEmpty());
        assertTrue(d.nextStage(task("RUNNING"), through(5)).isEmpty());
    }

    @Test
    void terminalStatusFollowsTheIndex() {
        assertEquals(TaskStatus.COMPLETED, d.terminalStatus(task("RUNNING"), through(5), TaskStatus.COMPLETED));
        List<SubtaskEntity> noSlides = through(2);
        noSlides.add(failed(2, "LEC_FRAMES", "x"));
        noSlides.add(ok(3, "LEC_INDEX", "{\"speech\":150,\"slides\":0}"));
        noSlides.add(ok(4, "LEC_CONCEPTS", CONCEPTS));
        assertEquals(TaskStatus.PARTIAL_FAILED, d.terminalStatus(task("RUNNING"), noSlides, TaskStatus.PARTIAL_FAILED));
        List<SubtaskEntity> noConcepts = through(4);
        noConcepts.add(failed(4, "LEC_CONCEPTS", "x"));
        assertEquals(TaskStatus.PARTIAL_FAILED, d.terminalStatus(task("RUNNING"), noConcepts, TaskStatus.PARTIAL_FAILED));
        List<SubtaskEntity> noTranscript = through(1);
        noTranscript.add(failed(1, "LEC_TRANSCRIBE", "x"));
        noTranscript.add(ok(2, "LEC_FRAMES", "{\"frames\":120}"));
        assertEquals(TaskStatus.FAILED, d.terminalStatus(task("RUNNING"), noTranscript, TaskStatus.PARTIAL_FAILED),
                "导入和截帧成功了也没用:没有逐字稿就没有索引");
    }

    private LectureEntity saved() {
        ArgumentCaptor<LectureEntity> c = ArgumentCaptor.forClass(LectureEntity.class);
        verify(lectures).updateById(c.capture());
        return c.getValue();
    }

    @Test
    void onFinishedMakesTheLectureReady() throws Exception {
        lecture("https://youtu.be/abc", null, null);
        d.onFinished(task("COMPLETED"), through(5));
        LectureEntity l = saved();
        assertEquals("READY", l.getStatus());
        assertEquals(KEY, l.getMediaKey());
        assertEquals(7101000L, l.getDurationMs().longValue());
        assertEquals("LLM Agents Overview", l.getTitle());
        assertEquals("Pre-training", om.readTree(l.getConcepts()).get(0).get("name").asText());
        assertNull(l.getError());
    }

    @Test
    void onFinishedKeepsTheTitleTheUserGave() {
        lecture("https://youtu.be/abc", null, "第 2 讲");
        d.onFinished(task("COMPLETED"), through(5));
        assertEquals("第 2 讲", saved().getTitle());
    }

    @Test
    void onFinishedRecordsWhichStepFailed() {
        lecture("https://youtu.be/abc", null, null);
        List<SubtaskEntity> noSlides = through(2);
        noSlides.add(failed(2, "LEC_FRAMES", "media 500: OCR crashed"));
        noSlides.add(ok(3, "LEC_INDEX", "{\"speech\":150,\"slides\":0}"));
        noSlides.add(ok(4, "LEC_CONCEPTS", CONCEPTS));
        d.onFinished(task("PARTIAL_FAILED"), noSlides);
        LectureEntity l = saved();
        assertEquals("PARTIAL", l.getStatus());
        assertEquals("LEC_FRAMES: media 500: OCR crashed", l.getError());
        assertEquals(KEY, l.getMediaKey());
    }

    @Test
    void failedTranscriptionFailsTheLecture() {
        lecture("https://youtu.be/abc", null, null);
        List<SubtaskEntity> subs = through(1);
        subs.add(failed(1, "LEC_TRANSCRIBE", "CUDA out of memory"));
        subs.add(ok(2, "LEC_FRAMES", "{\"frames\":120}"));
        d.onFinished(task("FAILED"), subs);
        LectureEntity l = saved();
        assertEquals("FAILED", l.getStatus());
        assertEquals("LEC_TRANSCRIBE: CUDA out of memory", l.getError());
        assertNull(l.getConcepts());
    }
}
```

- [ ] **Step 2: 写失败的测试** `LectureIngestFlowTest.java`（真实的结果入口 + 真实的库；照 `ResearchAgentFlowTest` 的写法：测试事务最后回滚，新阶段的子任务停在 `PENDING`）

```java
package com.agentflow.server.service;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.server.entity.CourseEntity;
import com.agentflow.server.entity.LectureEntity;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.CourseMapper;
import com.agentflow.server.mapper.LectureMapper;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** LECTURE_INGEST 的五步推进,经 ResultHandleService 真实入口,最后讲次那一行被写对。 */
@SpringBootTest
@Transactional
class LectureIngestFlowTest {

    private static final String KEY = "0123456789abcdef0123456789abcdef";
    @Autowired private ResultHandleService service;
    @Autowired private TaskMapper taskMapper;
    @Autowired private SubtaskMapper subtaskMapper;
    @Autowired private CourseMapper courseMapper;
    @Autowired private LectureMapper lectureMapper;
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;

    private long lectureId;
    private long taskId;

    /** 一门课、一讲(INGESTING)、一个 RUNNING 的任务和已发出的 LEC_IMPORT;返回 LEC_IMPORT 的子任务 id。 */
    private long fixture() {
        CourseEntity c = new CourseEntity();
        c.setName("flow-test");
        courseMapper.insert(c);
        LectureEntity l = new LectureEntity();
        l.setCourseId(c.getId());
        l.setSeq(1);
        l.setSourceUrl("https://youtu.be/abc");
        l.setStatus("INGESTING");
        lectureMapper.insert(l);
        lectureId = l.getId();
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setType("LECTURE_INGEST");
        t.setStatus("RUNNING");
        t.setPayload("{\"lectureId\":" + lectureId + ",\"courseId\":" + c.getId() + "}");
        t.setSubtaskTotal(1);
        t.setSubtaskDone(0);
        t.setSubtaskFailed(0);
        taskMapper.insert(t);
        taskId = t.getId();
        SubtaskEntity s = new SubtaskEntity();
        s.setSubtaskUuid(UUID.randomUUID().toString());
        s.setTaskId(taskId);
        s.setSeq(0);
        s.setKind("LEC_IMPORT");
        s.setStatus("DISPATCHED");
        s.setInput("{\"lectureId\":" + lectureId + ",\"url\":\"https://youtu.be/abc\"}");
        subtaskMapper.insert(s);
        return s.getId();
    }

    /** 这个 kind 的子任务(刚由阶段推进建出来,PENDING),标成已发出后返回它的 id。 */
    private long dispatched(String kind) {
        SubtaskEntity s = subtaskMapper.selectOne(new QueryWrapper<SubtaskEntity>().eq("task_id", taskId).eq("kind", kind));
        assertEquals("PENDING", s.getStatus(), kind);
        subtaskMapper.casStatus(s.getId(), "PENDING", "DISPATCHED");
        return s.getId();
    }

    private void ok(long subtaskId, String output) {
        service.handle(new ResultMessage(taskId, subtaskId, true, output, null));
    }

    @Test
    void fiveStepsThenTheLectureIsReady() {
        ok(fixture(), "{\"mediaKey\":\"" + KEY + "\",\"title\":\"LLM Agents Overview\",\"durationMs\":7101000,\"hasVideo\":true}");
        long transcribe = dispatched("LEC_TRANSCRIBE");
        long frames = dispatched("LEC_FRAMES");
        ok(transcribe, "{\"utterances\":1298,\"cached\":false}");
        assertTrue(subtaskMapper.selectList(new QueryWrapper<SubtaskEntity>().eq("task_id", taskId).eq("kind", "LEC_INDEX")).isEmpty(),
                "截帧还没落定,不能开始建索引");
        ok(frames, "{\"frames\":120,\"cached\":false}");
        ok(dispatched("LEC_INDEX"), "{\"speech\":150,\"slides\":120}");
        assertEquals("INGESTING", lectureMapper.selectById(lectureId).getStatus());
        ok(dispatched("LEC_CONCEPTS"), "{\"concepts\":[{\"name\":\"Pre-training\",\"startMs\":0,\"summary\":\"s\"}],\"usage\":{}}");

        assertEquals("COMPLETED", taskMapper.selectById(taskId).getStatus());
        LectureEntity l = lectureMapper.selectById(lectureId);
        assertEquals("READY", l.getStatus());
        assertEquals(KEY, l.getMediaKey());
        assertEquals("LLM Agents Overview", l.getTitle());
        assertTrue(l.getConcepts().contains("Pre-training"));
    }

    @Test
    void aFailedTranscriptionFailsTheLectureAndSaysWhy() {
        ok(fixture(), "{\"mediaKey\":\"" + KEY + "\",\"title\":\"T\",\"durationMs\":1000,\"hasVideo\":true}");
        long transcribe = dispatched("LEC_TRANSCRIBE");
        long frames = dispatched("LEC_FRAMES");
        service.handle(new ResultMessage(taskId, transcribe, false, null, "CUDA out of memory"));
        ok(frames, "{\"frames\":3,\"cached\":false}");

        assertEquals("FAILED", taskMapper.selectById(taskId).getStatus());
        LectureEntity l = lectureMapper.selectById(lectureId);
        assertEquals("FAILED", l.getStatus());
        assertEquals("LEC_TRANSCRIBE: CUDA out of memory", l.getError());
    }
}
```

- [ ] **Step 3: 跑测试确认失败**

Run: `mvn -q -pl agentflow-server -am test -Dtest=LectureIngestDecomposerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败，找不到 `LectureIngestDecomposer`、`LectureEntity`、`LectureMapper`。

- [ ] **Step 4: 实现**（表、实体、mapper、decomposer）

- [ ] **Step 5: 跑单元测试（Mac），再跑 server 的全部测试（Upton）**

Run: `mvn -q -pl agentflow-server -am test -Dtest=LectureIngestDecomposerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 12 个用例通过。

Run: `SYNC && R './scripts/course-env.sh test-down; ./scripts/course-env.sh test-up && sleep 60 && ./scripts/course-env.sh test-java -pl agentflow-server -am test 2>&1 | tail -15'`
Expected: 没有失败（`test-down` 再 `test-up` 是为了让 MySQL 用新的 `init.sql` 建出两张新表）。

- [ ] **Step 6: 提交并推送**（`git commit -m "feat(server): 课程和讲次的表,LECTURE_INGEST 的五步推进"`）

---

### Task 9: 课程接口

**Files:**
- Create: `agentflow-server/src/main/java/com/agentflow/server/course/CourseController.java`、`LectureService.java`、`MediaProxy.java`、`agentflow-server/src/test/java/com/agentflow/server/course/CourseApiTest.java`、`CourseApiDisabledTest.java`
- Modify: `agentflow-server/src/main/java/com/agentflow/server/product/ProductErrors.java`（`assignableTypes` 加上 `CourseController.class`）、`agentflow-server/src/main/resources/application.yml`（加 `agentflow.course.enabled: false` 和 `media.base-url: http://localhost:8010`）、`docker-compose.yml`（server 的环境变量加 `AGENTFLOW_COURSE_ENABLED: ${COURSE_ENABLED:-false}`、`MEDIA_BASEURL: ${MEDIA_BASEURL:-http://localhost:8010}`）

**Interfaces:**
- Consumes: 任务 8 的实体、mapper、`LectureIngestDecomposer.TYPE`，`TaskSubmitService.submit(type, payload)`。
- Produces:
  - `MediaProxy`（`@Component`）：`record Body(int status, String contentType, byte[] bytes)`；`Body get(String path)`：对 `media.base-url + path` 发 GET，把状态码、`Content-Type`、响应体原样带回；连不上抛 `ResponseStatusException(BAD_GATEWAY, "媒体服务不可用")`。
  - `LectureService`（`@Service`）：
    - `CourseEntity createCourse(String name)`：名字去空白后 1 到 200 字，否则 400。
    - `LectureEntity addLecture(long courseId, String url, String path, String title)`：课程不存在 404；`url` 和 `path` 必须恰好给一个，否则 400；`seq` = 这门课现有的最大值 + 1；插入讲次（`INGESTING`）→ `submit(LECTURE_INGEST, {"lectureId","courseId"})` → 把 `taskUuid` 写回讲次。**这个方法不加 `@Transactional`**：提交任务要等自己的事务提交后才发 Kafka，外面再包一层事务会让消息先于提交发出。提交任务抛异常时把讲次标成 `FAILED`、`error` 写异常消息，再把异常抛出去。
    - `LectureEntity retry(long lectureId)`：只允许 `FAILED`、`PARTIAL`，否则 400；状态改回 `INGESTING`、清空 `error`，再提交一个新任务。
  - `CourseController`（`@RestController`，`@ConditionalOnProperty(name = "agentflow.course.enabled", havingValue = "true")`）：路由和响应见"接口约定"。`stages` 来自讲次 `taskUuid` 对应任务的子任务，按 `seq` 排；`seconds` = `updatedAt - createdAt`（秒，取整），只在子任务是 `COMPLETED` 或 `FAILED` 时给出。讲次没有 `mediaKey` 时逐字稿和帧接口返回 404；帧文件名不匹配 `\d{9}\.jpg` 返回 400。

- [ ] **Step 1: 写失败的测试** `CourseApiTest.java`

```java
package com.agentflow.server.course;

import com.agentflow.server.entity.LectureEntity;
import com.agentflow.server.mapper.LectureMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 课程接口:建课、加讲次(提交摄入任务)、看进度、重试、读逐字稿和帧。 */
@SpringBootTest(properties = "agentflow.course.enabled=true")
@AutoConfigureMockMvc
@Transactional
class CourseApiTest {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final String KEY = "0123456789abcdef0123456789abcdef";
    @Autowired private MockMvc mvc;
    @Autowired private LectureMapper lectureMapper;
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;
    @MockBean private MediaProxy mediaProxy;

    private JsonNode postJson(String url, String body, int expected) throws Exception {
        String s = mvc.perform(post(url).contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().is(expected)).andReturn().getResponse().getContentAsString(StandardCharsets.UTF_8);
        return OM.readTree(s.isEmpty() ? "{}" : s);
    }

    private long course() throws Exception {
        return postJson("/api/v1/courses", "{\"name\":\"Agentic AI\"}", 200).get("id").asLong();
    }

    @Test
    void addingALectureStartsItsIngest() throws Exception {
        long c = course();
        JsonNode l = postJson("/api/v1/courses/" + c + "/lectures", "{\"url\":\"https://youtu.be/abc\"}", 200);
        assertEquals("INGESTING", l.get("status").asText());
        assertEquals(1, l.get("seq").asInt());
        JsonNode second = postJson("/api/v1/courses/" + c + "/lectures", "{\"path\":\"lec2.mp4\",\"title\":\"第 2 讲\"}", 200);
        assertEquals(2, second.get("seq").asInt());

        mvc.perform(get("/api/v1/courses/" + c)).andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("Agentic AI"))
                .andExpect(jsonPath("$.lectures.length()").value(2))
                .andExpect(jsonPath("$.lectures[0].sourceUrl").value("https://youtu.be/abc"))
                .andExpect(jsonPath("$.lectures[0].stages[0].kind").value("LEC_IMPORT"))
                .andExpect(jsonPath("$.lectures[0].stages.length()").value(1))
                .andExpect(jsonPath("$.lectures[1].title").value("第 2 讲"));
        mvc.perform(get("/api/v1/courses")).andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.id == " + c + ")].lectures").value(2))
                .andExpect(jsonPath("$[?(@.id == " + c + ")].ready").value(0));
    }

    @Test
    void aLectureNeedsExactlyOneSourceAndAnExistingCourse() throws Exception {
        long c = course();
        postJson("/api/v1/courses/" + c + "/lectures", "{}", 400);
        postJson("/api/v1/courses/" + c + "/lectures", "{\"url\":\"https://youtu.be/abc\",\"path\":\"a.mp4\"}", 400);
        postJson("/api/v1/courses/999999999/lectures", "{\"url\":\"https://youtu.be/abc\"}", 404);
        postJson("/api/v1/courses", "{\"name\":\"  \"}", 400);
        mvc.perform(get("/api/v1/courses/999999999")).andExpect(status().isNotFound());
    }

    private long lecture(long course, String status, String mediaKey) {
        LectureEntity l = new LectureEntity();
        l.setCourseId(course);
        l.setSeq(1);
        l.setSourceUrl("https://youtu.be/abc");
        l.setStatus(status);
        l.setMediaKey(mediaKey);
        l.setError("FAILED".equals(status) ? "LEC_IMPORT: media 502: download failed" : null);
        l.setConcepts("READY".equals(status) ? "[{\"name\":\"Pre-training\",\"startMs\":0,\"summary\":\"s\"}]" : null);
        lectureMapper.insert(l);
        return l.getId();
    }

    @Test
    void retryIsOnlyForFailedOrPartialLectures() throws Exception {
        long c = course();
        long failed = lecture(c, "FAILED", null);
        JsonNode again = postJson("/api/v1/lectures/" + failed + "/retry", "", 200);
        assertEquals("INGESTING", again.get("status").asText());
        assertTrue(again.get("error").isNull());
        assertEquals("LEC_IMPORT", again.get("stages").get(0).get("kind").asText());
        postJson("/api/v1/lectures/" + failed + "/retry", "", 400);                       // 已经在跑了
        postJson("/api/v1/lectures/" + lecture(c + 1, "READY", KEY) + "/retry", "", 400);
        postJson("/api/v1/lectures/999999999/retry", "", 404);
    }

    @Test
    void aReadyLectureShowsItsConceptsTranscriptAndFrames() throws Exception {
        long id = lecture(course(), "READY", KEY);
        mvc.perform(get("/api/v1/lectures/" + id)).andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("READY"))
                .andExpect(jsonPath("$.concepts[0].name").value("Pre-training"));
        when(mediaProxy.get("/media/" + KEY + "/transcript")).thenReturn(new MediaProxy.Body(200, "application/json",
                "{\"utterances\":[{\"startMs\":0,\"endMs\":4200,\"text\":\"Hi.\"}]}".getBytes(StandardCharsets.UTF_8)));
        mvc.perform(get("/api/v1/lectures/" + id + "/transcript")).andExpect(status().isOk())
                .andExpect(jsonPath("$.utterances[0].text").value("Hi."));
        when(mediaProxy.get("/media/" + KEY + "/frames/000020000.jpg"))
                .thenReturn(new MediaProxy.Body(200, "image/jpeg", new byte[]{1, 2, 3}));
        mvc.perform(get("/api/v1/lectures/" + id + "/frames/000020000.jpg")).andExpect(status().isOk())
                .andExpect(content().contentType("image/jpeg")).andExpect(content().bytes(new byte[]{1, 2, 3}));
        mvc.perform(get("/api/v1/lectures/" + id + "/frames/..%2Fmeta.json")).andExpect(status().is4xxClientError());
        mvc.perform(get("/api/v1/lectures/" + id + "/frames/meta.json")).andExpect(status().isBadRequest());
    }

    @Test
    void aLectureThatWasNeverImportedHasNoTranscript() throws Exception {
        long id = lecture(course(), "FAILED", null);
        mvc.perform(get("/api/v1/lectures/" + id + "/transcript")).andExpect(status().isNotFound());
        mvc.perform(get("/api/v1/lectures/" + id + "/frames")).andExpect(status().isNotFound());
    }
}
```

- [ ] **Step 2: 写失败的测试** `CourseApiDisabledTest.java`

```java
package com.agentflow.server.course;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 默认配置下课程接口不存在:公开 demo 用的也是这份代码。 */
@SpringBootTest
@AutoConfigureMockMvc
class CourseApiDisabledTest {

    @Autowired private MockMvc mvc;
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;

    @Test
    void courseRoutesDoNotExistUnlessSwitchedOn() throws Exception {
        mvc.perform(post("/api/v1/courses").contentType(MediaType.APPLICATION_JSON).content("{\"name\":\"x\"}"))
                .andExpect(status().isNotFound());
        mvc.perform(get("/api/v1/courses")).andExpect(status().isNotFound());
        mvc.perform(post("/api/v1/lectures/1/retry")).andExpect(status().isNotFound());
    }
}
```

- [ ] **Step 3: 跑测试确认失败**

Run: `SYNC && R './scripts/course-env.sh test-java -pl agentflow-server -am test -Dtest="CourseApi*Test" -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | tail -15'`
Expected: 编译失败，找不到 `MediaProxy`。

- [ ] **Step 4: 实现**（按 Interfaces）

- [ ] **Step 5: 跑 server 的全部测试确认通过**

Run: `SYNC && R './scripts/course-env.sh test-java -pl agentflow-server -am test 2>&1 | tail -15'`
Expected: 没有失败。

- [ ] **Step 6: 提交并推送**（`git commit -m "feat(server): 课程接口(默认关闭):建课、加讲次、进度、重试、逐字稿和帧"`）

---

### Task 10: 课程库页和单讲页

**Files:**
- Create: `agentflow-server/src/main/resources/static/courses.html`、`agentflow-server/src/main/resources/static/lecture.html`

**Interfaces:**
- Consumes: 任务 9 的接口。
- 两个页面都是单文件、原生 JS、不引框架和构建，颜色和字体沿用 `app.html` 里的 CSS 变量。所有来自接口的文字用 `textContent` 写进页面，不用 `innerHTML`（标题、逐字稿、OCR 文字都是外来内容）。

**`courses.html`（课程库）**
- 左边课程列表（名字、讲数、已完成数）和"新建课程"输入框；右边是选中课程的讲次表。
- 讲次表每行：序号、标题（没有就显示链接或文件名）、时长、状态、五个步骤的小标签（`导入` `转写` `截帧` `索引` `知识点`，按 `stages` 里的状态上色：没到灰、进行中蓝、成功绿、失败红，成功的显示用时秒数）、失败原因、"重试"按钮（只在 `FAILED`/`PARTIAL` 时出现）。`READY` 和 `PARTIAL` 的标题是到 `lecture.html?id=<id>` 的链接。
- "添加一讲"：一个输入框，填的东西以 `http` 开头就当链接，否则当 inbox 里的文件名；可选的标题输入框。
- 只要选中的课程里有 `INGESTING` 的讲次，就每 3 秒刷新一次；没有就停。
- 接口返回 404（功能没开）时，整页只显示一句"课程功能没有开启（`agentflow.course.enabled`）"。

**`lecture.html?id=<讲次 id>`（单讲）**
- 顶部：课程名 › 第 N 讲 · 标题 · 时长 · 状态。`PARTIAL` 时把 `error` 显示成一条黄色提示。
- 左上播放器：`sourceUrl` 是 YouTube 链接时用 YouTube IFrame Player API 嵌入；本地文件显示"本地文件暂不支持在页面里播放"，时间点照常显示但点了不跳。
- 左下逐字稿：每条 utterance 一行（`mm:ss` + 文字），点一行跳到那一秒并播放；上面一个搜索框，输入时只留下包含关键词的行（不区分大小写，纯前端过滤）；播放时当前那一行高亮（每秒查一次播放器的时间）。
- 右上知识点目录：`concepts`，每条是名字 + 时间 + 一句话，点了跳转。没有就显示"这一讲还没有知识点目录"。
- 右下幻灯片：每帧一张缩略图（`/api/v1/lectures/{id}/frames/{name}`，`loading="lazy"`）加时间，点了跳转，鼠标悬停显示 OCR 文字。帧接口 404 时显示"这一讲没有截到幻灯片"。

- [ ] **Step 1: 写两个页面**

- [ ] **Step 2: 检查没有用 `innerHTML` 写外来内容**

Run: `grep -n "innerHTML" agentflow-server/src/main/resources/static/courses.html agentflow-server/src/main/resources/static/lecture.html`
Expected: 没有输出（或者只有清空容器的 `innerHTML = ""`）。

- [ ] **Step 3: 提交并推送**（`git commit -m "feat(ui): 课程库页和单讲页"`）。页面在任务 11 里对着真实数据在浏览器里验证。

---

### Task 11: 在 Upton 上端到端跑通，记录数字

**Files:**
- Modify: `docs/benchmarks.md`（加第 8 节 "Lecture ingest on the engine"）、`README.zh.md`（"媒体服务"一节后面加"课程录播"小节：怎么起环境、两个页面的地址、当前的范围限制）

- [ ] **Step 1: 起整套环境**

```bash
SYNC && R './scripts/course-env.sh test-down; ./scripts/course-env.sh up 2>&1 | tail -5; curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:18090/api/v1/courses'
```
Expected: `200`。

- [ ] **Step 2: 摄入两讲**（第一讲的下载和转写已经在媒体服务的缓存里；第二讲是全新的，量的是完整的一遍）

```bash
R 'B=http://127.0.0.1:18090/api/v1; H="Content-Type: application/json"
C=$(curl -s -X POST $B/courses -H "$H" -d "{\"name\":\"Berkeley Agentic AI (F25)\"}" | python3 -c "import json,sys; print(json.load(sys.stdin)[\"id\"])")
curl -s -X POST $B/courses/$C/lectures -H "$H" -d "{\"url\":\"https://www.youtube.com/watch?v=r1qZpYAmqmg\"}"; echo
curl -s -X POST $B/courses/$C/lectures -H "$H" -d "{\"url\":\"https://www.youtube.com/watch?v=xqRAS6rAouo\"}"; echo; echo "course=$C"'
```

然后每隔一两分钟查一次 `GET /api/v1/courses/<id>`，直到两讲都不是 `INGESTING`（第二讲预计 10 分钟左右）。
Expected: 两讲都是 `READY`；每讲五个步骤都是 `COMPLETED`。任何一讲是 `FAILED` 或 `PARTIAL` 就停下来查（`docker logs agentflow-course-worker-1-1`、`~/agentflow-media-data/media.log`），不要重试到成功为止。

- [ ] **Step 3: 检查索引和检索**

```bash
R 'docker exec agentflow-course-rag-1 python -c "
import json, urllib.request
def post(p, b):
    r = urllib.request.Request(\"http://127.0.0.1:8000\" + p, data=json.dumps(b).encode(), headers={\"Content-Type\": \"application/json\"})
    return json.load(urllib.request.urlopen(r))
for q in [\"what is pre-training\", \"reinforcement learning from human feedback\", \"how are agents evaluated\"]:
    hits = post(\"/segments/search\", {\"courseId\": COURSE_ID, \"query\": q, \"topK\": 3})[\"hits\"]
    print(q); [print(\"   L%d %s %d:%02d %s\" % (h[\"lectureId\"], h[\"kind\"], h[\"startMs\"]//60000, h[\"startMs\"]//1000%60, h[\"text\"][:90])) for h in hits]
"'
```
（把 `COURSE_ID` 换成上一步的课程 id。）
Expected: 每个问题有 3 条命中，文字和问题相关，命中里既有 `speech` 也可能有 `slide`。人工看一眼，把例子记进文档。

- [ ] **Step 4: 失败的情况各试一次**

1. 加一讲播放列表链接（`https://www.youtube.com/playlist?list=PLS01nW3RtgoqGkm4UeqNeZLccW-OGc1fJ`）。Expected: 一分钟内变成 `FAILED`，`error` 以 `LEC_IMPORT: media 400` 开头，trace 里没有重试。
2. 对一讲 `READY` 的讲次点重试。Expected: 400。
3. 把同一个链接再加一次（另一讲）。Expected: 很快 `READY`（导入、转写、截帧都命中缓存），媒体服务的 `/stats` 计数不变。

- [ ] **Step 5: 在浏览器里看两个页面**（Mac 上 `ssh -N -L 18090:127.0.0.1:18090 Upton@100.65.10.56` 开隧道，打开 `http://localhost:18090/courses.html`）

逐项确认并截图：课程库显示两讲和五个步骤的状态；点进单讲页，播放器能播；点一行逐字稿、点一个知识点、点一张幻灯片都会跳到对应时间；搜索框能过滤；控制台没有报错。发现的问题在本任务里修，修完重新同步、重新 `up`。

- [ ] **Step 6: 把数字写进文档，提交并推送**

`docs/benchmarks.md` 第 8 节：两讲各自的时长、五个步骤的用时（`stages[].seconds`）、片段数（speech / slide）、知识点数（注明是模拟模型出的，只说明流程通了）、三次失败情况的结果、检索的例子。`README.zh.md` 的小节写明第一版的范围限制。
`git commit -m "docs: 课程录播摄入的端到端实测和使用说明"`

整套环境留在服务器上继续跑，计划 3 要用。停掉：`ssh Upton@100.65.10.56 'cd ~/project/agentflow-course-dev && ./scripts/course-env.sh down'`。
