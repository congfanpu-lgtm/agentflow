# Benchmarks & drills (2026-10-17)

Environment: Apple M5 (10 cores) · Docker Desktop · `docker compose` stack (MySQL, Redis, MongoDB, Kafka,
pgvector, RAG, server, **2 workers × 4 consumer threads**, 8 SUBTASK partitions). Numbers are machine-specific;
the ratios and pass/fail results are the point.

## 1. Scheduler throughput (no LLM)

`ECHO_BATCH` subtasks sleep 300 ms, so the ceiling is 8 threads ÷ 0.3 s = **26.7 subtasks/s**.

| Run | Tasks × subtasks | Concurrency | Submit P50 / P99 | End-to-end P50 / P99 | Throughput | Trace complete |
|---|---|---|---|---|---|---|
| `load_test.py --tasks 60 --items 5 --concurrency 10` | 60 × 5 | 10 | 44 / 579 ms | 1.9 / 2.9 s | 23.9 subtasks/s | 100% |
| `load_test.py --tasks 120 --items 5 --concurrency 20` | 120 × 5 | 20 | 36 / 357 ms | 3.8 / 4.3 s | **25.5 subtasks/s (96% of ceiling)** | 100% |

Reading it: Kafka dispatch, MySQL state CAS, idempotency checks, Mongo trace writes and polling
together add ~4% over pure work time at saturation; end-to-end latency grows with queue depth, not with
per-task overhead.

### Raising the ceiling (2026-11-12)

A worker thread handles one subtask at a time and commits its offset when it is done. So the number of subtasks
in flight equals the number of consumer threads, capped by the SUBTASK partition count. Both are now settings
(`SUBTASK_PARTITIONS`, `WORKER_CONCURRENCY`); the defaults stay at 8 and 2 × 4.

| Configuration | Ceiling | Measured | Limited by |
|---|---|---|---|
| 8 partitions, 2 × 4 threads | 26.7 /s | 25.1 /s | worker threads |
| 64 partitions, 2 × 32 threads, 1 result thread | 213 /s | 117–159 /s | the server: one thread consuming results, 785 results queued mid-run |
| 64 partitions, 2 × 32 threads, 3 result threads | 213 /s | **195–201 /s (92–94%)** | worker threads again |

Last row: 1,200 tasks × 5 subtasks from 300 concurrent clients. Submit P50 89–118 ms / P99 1.3–1.5 s,
end-to-end P50 7.1 s / P99 8.0–8.4 s, trace 100% complete.

**A race the higher concurrency exposed.** With three result threads, 3 of about 1,200 tasks hung with one
subtask left `DISPATCHED`. The dispatcher sends the message first and records `DISPATCHED` second. Under load a
result arrived in between, did not match the expected state, and was dropped as a duplicate. The result handler
now records `DISPATCHED` itself in that case. The three tasks recovered on their own ten minutes later through
the timeout redispatch and the worker's re-send (finding 2 in the fault matrix).

**Why in-flight work is still tied to partitions.** Untying it needs out-of-order offset commits. Spring
Kafka's `asyncAcks` pauses the consumer until a whole poll is acknowledged, redelivers more after a crash, and
takes listener failures away from the container's error handler. More partitions reaches the same in-flight
count and leaves the failure behaviour measured below unchanged. The cost: partitions cannot be removed, and a
rebalance moves more of them.

One operational note from upgrading a live deployment (3 → 8 partitions): a worker that is already running
picks up the added partitions only after its next metadata refresh, up to 5 minutes. Subtasks sent to the
new partitions waited about 4 minutes and then completed. Restart the workers to take over at once.

## 2. Fault matrix (2026-11-12)

`scripts/fault_matrix.py` submits 2 tasks/s (5 subtasks each) for 60 s, `docker kill`s one component 10 s in
(SIGKILL) and starts it again after the outage. It runs in its own compose project (`docker/chaos.yml`: separate
containers and volume), same shape as above: 2 workers × 4 threads. Each run answers three questions:

- **Lost?** Every *accepted* task (the submit call returned a `taskUuid`) must end `COMPLETED`. Submits refused
  during the outage are counted separately.
- **Duplicated?** A subtask with more than one `PROCESSED/OK` event in its trace was executed twice.
- **How long?** Seconds from the restart to the first new result, and to the backlog being cleared.

**30 s outage**

| Killed | Accepted / refused | Completed | Run twice | First result | Backlog cleared | What happens |
|---|---|---|---|---|---|---|
| 1 of 2 workers | 120 / 0 | 120 | 0 | 14.9 s | 26.6 s | Its partitions move after Kafka's 45 s session timeout, restart or not |
| Server | 52 / 68 | 52 | 0 | 14.4 s | 14.5 s | Results wait in Kafka. One server, so submits are refused while it is down |
| Kafka | 120 / 0 | 120 | 0 | 14.0 s | 31.4 s | Producers buffer and retry. Submit calls block until the broker is back (P99 60 s) |
| MySQL | 99 / 21 | 99 | 0 | 10.7 s | 22.2 s | The result consumer retries with backoff. Submits return 500 meanwhile |
| Redis | 120 / 0 | 120 | 0 | 4.4 s | 20.7 s | The subtask consumer retries its idempotency lookup with backoff |
| MongoDB | 120 / 0 | 120 | 0 | — | — | Tasks do not depend on it. Trace events wait in Kafka and are written after the restart |

Re-run at 64 partitions, 2 × 32 worker threads and 3 result threads: the same outcome for all six (accepted
tasks all completed, none run twice, nothing dropped), and the backlog clears in 11–17 s instead of 15–31 s.

**Minutes-long outage** (a failover, not a blip), before and after the fixes below

| Outage | Build | Completed | Stuck at the deadline | Run twice | Messages dropped |
|---|---|---|---|---|---|
| Redis, 180 s | before | 115 / 120 | 5 | 5 | 24 subtask messages |
| | after | **120 / 120** | 0 | 0 | 0 |
| MySQL, 330 s | before | 20 / 21 | 1, later `PARTIAL_FAILED` | 0 | 1 result |
| | after | **21 / 21** | 0 | 0 | 0 |

### What the drills found

1. **Messages dropped after 10 instant retries.** Spring Kafka's default error handler retries a failed record
   10 times with no delay and then skips it. With Redis down the workers skipped subtask messages; with MySQL
   down the server skipped a result. Now: exponential backoff, 1 s doubling to 30 s, 10 minutes in total.
2. **A lost result became a false failure.** The worker had done the work and stored its idempotency key, so
   every timeout redispatch was skipped, and nothing re-sent the result. After 3 redispatches (40 minutes at the
   default 600 s) the subtask was marked `FAILED` and the task ended `PARTIAL_FAILED`, although all the work
   had succeeded. Now the key stores the output and a redelivery re-sends it; the server's compare-and-swap
   drops extras. Checked live: one redispatch settles the task and the work is not redone.
3. **Redis failing just after a result was sent ran the work twice.** The failed idempotency write was treated
   as a processing failure and went to the retry ladder (5 of 5 in-flight subtasks). Now it is logged: the
   result is already out, and a missing key can only cost a duplicate on a later redelivery.
4. **A task could be stranded forever.** If the server died between committing a task and dispatching it,
   task and subtasks stayed `PENDING`; the sweep only looked at `RUNNING` tasks. The random drills did not hit
   this window (a few milliseconds), so it was reproduced by planting such a row: untouched after two sweeps on
   the old build, `COMPLETED` after one on the new build.

### Limits

- One server. While it is down nothing can be submitted; accepted tasks are unaffected.
- Submit waits for the broker ack of each subtask, so it blocks while Kafka is down.
- A dead consumer is replaced only after the 45 s session timeout. Lowering `session.timeout.ms` trades faster
  failover for more false rebalances.
- An outage longer than the 10-minute retry budget still drops the message. The timeout sweep (600 s) then
  recovers it through redispatch and re-send.
- Redis is not persisted here. A kill loses the idempotency keys and agent checkpoints written since the last
  snapshot, so a later redelivery of those subtasks redoes the work. These runs did not trigger one.
- MongoDB acknowledges a trace write before it is journaled (the default write concern), so a kill can lose the
  last ~100 ms of events. On the Linux run, 1 task of 120 lost its first two trace events (`SUBMITTED`,
  `DECOMPOSED`) in each MongoDB drill; the laptop runs lost none. Task state lives in MySQL and was not
  affected. `?journal=true` on the Mongo URI closes the gap and makes every trace write slower.
- Duplicates are counted from the trace, so the MongoDB row is a lower bound.

    C() { docker compose -p agentflow-chaos -f docker-compose.yml -f docker/chaos.yml "$@"; }
    C up -d --wait mysql redis mongo kafka && C up -d --build --no-deps server worker-1 worker-2
    python3 scripts/fault_matrix.py --victim all        # prefix the `C up` line with SUBTASK_PARTITIONS=64 WORKER_CONCURRENCY=32 for the scaled run
    python3 scripts/fault_matrix.py --victim redis --down 180 --deadline 300
    python3 scripts/fault_matrix.py --victim mysql --down 330 --deadline 480

## 3. Overload (2026-11-12)

60 `LLM_BATCH` tasks × 5 calls submitted at once to 64 worker threads, mock model at 1.5 s per call. That is
about 43 calls/s attempted against a token bucket of 20 + 10/s, roughly four times the quota.

| No token available | Tasks completed | Subtasks dead-lettered | Wall time | Rate-limit failures |
|---|---|---|---|---|
| Before: fail, then the retry ladder (5 s / 30 s / 5 min), then DLQ | 22 / 60, the other 38 `PARTIAL_FAILED` | 52 / 300 | 344 s | 557 |
| After: wait for one, up to `llm.max-wait-ms` (60 s) | **60 / 60** | 0 | 31 s | 0 |

Before, every rate-limited call used up one of a subtask's three retries. Overload turned into failures, and
the tasks that did finish had waited for the 5-minute retry step. Now the call waits for a token. Its consumer
thread takes no new message meanwhile, so the excess stays queued in Kafka, and 300 calls at 10/s take the
expected 30 s. Waiting calls are not served in order, and one large task can still fill the queue ahead of a
small one.

    python3 scripts/load_test.py --base http://127.0.0.1:18080/api/v1 --type LLM_BATCH --tasks 60 --items 5 --concurrency 60 --deadline 480

### The same runs on a Linux server (2026-11-12)

Sections 1 (ceiling), 2 and 3 repeated on a 24-core Linux machine (WSL2, Docker 29.1, Compose 2.40), from a
fresh clone of the fixed build.

| | Laptop (M5) | Linux server |
|---|---|---|
| Throughput, 8 partitions | 25.1 /s | 25.0 /s |
| Throughput, 64 partitions | 195–201 /s | 193–203 /s |
| Six faults, 30 s outage, both configurations | all accepted tasks completed, none run twice | same |
| Redis down 180 s | 120 / 120 | 120 / 120 |
| MySQL down 330 s | 21 / 21 | 21 / 21, 5 redeliveries answered from the stored result |
| Overload, 4× the quota | 60 / 60 in 31 s | 60 / 60 in 30.5 s |
| Messages dropped / results ignored / dead-lettered | 0 / 0 / 0 | 0 / 0 / 0 |
| Trace complete after the MongoDB kill | 100% | 99.2% (see Limits) |

Throughput is the same on both because the ceiling is set by the 300 ms simulated work per thread, not by the
hardware.

## 4. Agent crash recovery (real model: Claude Haiku + Tavily)

`scripts/chaos-agent.sh "<question>"` waits for the first agent step, kills the worker running it, and
checks that another worker resumes from the Redis checkpoint.

| Question | Killed | Resumed | Result | Wall time |
|---|---|---|---|---|
| How does Kafka consumer group rebalancing work? | worker-2 | 2 agents on worker-1, from step 2 | COMPLETED | 71 s |
| What is the difference between Raft and Paxos? | worker-1 | 2 agents on worker-2, from step 2 | COMPLETED | 56 s |
| How does Redis Cluster shard keys? | worker-2 | 2 agents on worker-1, from step 2 | COMPLETED | 59 s |

3/3 recovered; completed steps (LLM decisions + tool results) were replayed from the checkpoint, not re-run.
Most of the wall time is the same 45 s session timeout as in the fault matrix.

## 5. Research quality

See the evaluation table in the README and `eval/results/`.

## 6. Retrieval quality (BEIR)

`agentflow-rag/retrieval_eval.py`, test splits, the service's own BM25 / embedding / RRF code; dense search is
exact cosine (what pgvector `<=>` does without an ANN index). Raw output: `eval/results/retrieval-v1.{md,json}`.

| Method | SciFact nDCG@10 | SciFact Recall@3 | NFCorpus nDCG@10 | NFCorpus Recall@10 |
|---|---|---|---|---|
| Hash embedding (before) | 0.211 | 0.204 | 0.074 | 0.037 |
| BM25 (k1 0.9, b 0.4) | 0.681 | 0.696 | 0.322 | 0.153 |
| Dense `BAAI/bge-small-en-v1.5` | 0.720 | 0.710 | 0.338 | 0.158 |
| **Hybrid, RRF k=60 over top 100 each** | **0.733** | **0.720** | **0.365** | **0.174** |
| *Published: BM25 (BEIR paper, Anserini)* | *0.665* | | *0.325* | |
| *Published: bge-small model card (MTEB)* | *0.713* | | *0.343* | |

Paired bootstrap 95% CI of the nDCG@10 gain: hybrid − BM25 = [+0.028, +0.076] (SciFact), [+0.030, +0.057]
(NFCorpus); hybrid − dense = [−0.012, +0.036] (SciFact, not significant), [+0.014, +0.041] (NFCorpus).
Encoding time on the M5 CPU: 539 s (SciFact), 360 s (NFCorpus).

## 7. Lecture video: transcription and slide frames (2026-11-13)

Environment: the 24-core Linux machine (WSL2) with one RTX 5080 Laptop GPU (16 GB). faster-whisper 1.2.1
(ctranslate2 4.9.0), `large-v3`, fp16. The service is `agentflow-media`; design in
`docs/superpowers/specs/2026-11-13-course-video-qa-design.md`.

### How to run transcription on one GPU (spike)

One 47.7-minute lecture (MIT 18.06, lecture 2), same audio for every row. Model load time is excluded.
The scripts were throwaway and are not in the repo.

| Plan | ASR workers | Wall time | Speed (× realtime) | Peak VRAM |
|---|---|---|---|---|
| 60 s chunks, one at a time | 1 | 315 s | 9.1 | 5.8 GB |
| same | 2 | 310 s | 9.2 | 11.2 GB |
| same | 3 | 327 s | 8.8 | 15.8 GB |
| same (control, rerun at the end) | 1 | 334 s | 8.6 | 5.8 GB |
| 5 min chunks, batched inside the worker (batch 8) | 1 | 39 s | 72.6 | 7.3 GB |
| same | 2 | 43 s | 67.0 | 13.7 GB |
| same | 3 | 74 s | 38.9 | 15.9 GB |
| **whole file, batched (batch 16)** | **1** | **30 s** | **95.1** | **10.0 GB** |

1. **More ASR workers on one GPU buy nothing.** Throughput with 1, 2 and 3 workers is the same (one worker
   already saturates the GPU) while VRAM grows linearly; near the 16 GB limit it gets slower (5-minute plan,
   3 workers: almost half the speed).
2. **The speed-up comes from batching inside one worker**: 8 to 10 times faster than chunk-by-chunk.
3. **So a lecture is not split:** one subtask per lecture, the whole file batched, one transcription at a time
   on the GPU. The earlier idea (60 s chunks fanned out to several workers, retried per chunk) was dropped.
4. After half an hour of continuous load the same configuration was about 6% slower (315 s → 334 s).

The two ways do not produce the same text. In the first 10 minutes the chunked run had about 10% more words
(1400 vs 1265): partly repetitions and hallucinations at the hard 60 s cuts ("Thank you very much. Thank you."
where nobody said it), partly short phrases that the batched pipeline's silence detection dropped. There is no
reference transcript, so neither is shown to be more accurate; batching was chosen because invented text would
later be quoted as evidence.

### End to end on a real lecture (smoke)

`agentflow-media/smoke.py` against the running service, one lecture of the evaluation course (Berkeley Agentic
AI MOOC, "LLM Agents Overview", 1 h 58 min 21 s, 1280×720). Everything is done twice; the second pass must be
all cache hits.

| Step | First pass | Result |
|---|---|---|
| Import (audio + 720p video, two downloads, no ffmpeg) | 38.6 s | 242 MB on disk including frames |
| Transcribe, as shipped (timestamps inside each chunk) | 151.5 s = **46.9 × realtime**, including the one-time model load | 1,298 utterances, 18,240 words, English |
| Transcribe, first version (pipeline default: no timestamps) | 96.1 s = 73.9 × realtime, same | 264 utterances, 18,546 words |
| Frames (key frames only, OCR on change) | 806.4 s | 1,288 key frames, 974 OCRed, 896 kept |
| Second pass | all three steps `cached`, `/stats` unchanged (1 download, 1 ASR run, 1 OCR run) | |

The lecture was transcribed twice because the first version turned out to give one utterance per ~30 s chunk
(next section). Import and frames were not repeated for the second run; they were cache hits.

The model stays loaded after the first transcription: about 4.4 GB of VRAM while idle.

### What the spot check found

- **Transcript:** reads correctly at the start, middle and end; start times never go backwards. "DeepSeek"
  is heard as "EAP-seq" in both runs.
- **Utterance length was wrong in the first version and is fixed.** The batched pipeline does not ask Whisper
  for timestamps by default, so one silence-detection chunk came back as one utterance: median 27.8 s, starting
  and ending mid-sentence, useless for "jump to this sentence". With `without_timestamps=False` each chunk is
  split at Whisper's own timestamps: 1,298 utterances, median 4.9 s, 90% under 7.8 s, longest 24.4 s. It costs
  58% more time (96 s → 152 s). These are phrases rather than sentences (497 of 1,298 end with ". ? !"), 5 of
  them start up to 1 s before the previous one ends (chunk boundaries), and the largest gap is 11.8 s;
  segmentation has to deal with all three.
- The two runs agree on 90–94% of the words, in order, in three 10-minute windows; the timestamped run has
  1.6% fewer words overall. There is no reference transcript, so neither is shown to be more accurate.
- **Frames: the same slide is captured many times.** The recording has the speaker's webcam in a corner
  (about 2% of the picture), so the picture "changes" on almost every key frame: the median change between
  consecutive key frames is 0.48% of pixels, above the 0.3% threshold that was tuned on a synthetic video
  without a webcam. (The service compares each key frame with the last one it OCRed, not with its neighbour;
  the outcome is the same.) 76% of key frames were OCRed (974 of 1,288), which is where the 13 minutes go. OCR output
  for an unchanged slide differs in spacing ("LLM AgentsMOOC" / "LLM Agents MOOC"), so the exact-text check
  does not remove the repeats; slides that build up line by line add more. Comparing texts with whitespace
  removed, 896 frames collapse to 359 at similarity 0.9 and to 268 at 0.8.
- Not fixed here. The frame list is usable (the text is the slide text, not noise) but about three times too
  long, and the OCR step is the slowest part of ingesting a lecture by a wide margin.
