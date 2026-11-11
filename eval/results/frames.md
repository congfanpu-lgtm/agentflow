# Multi-hop evaluation (FRAMES)

2026-11-10 · model `claude-haiku-5-5` · grader `claude-sonnet-5-5` · script [`eval/frames_eval.py`](../frames_eval.py)

Questions come from [FRAMES](https://huggingface.co/datasets/google/frames-benchmark): 824 questions with
gold answers, each needing 2–15 Wikipedia articles. The file is shuffled with a fixed seed.
**dev** is questions 0–29 and was used to diagnose and tune. **holdout** is questions 30–59 and was only run
on finished designs. The grader compares the report with the gold answer; "cannot determine" counts as wrong.

| Configuration | dev (30) | holdout (30) | Tokens / question | p50 |
|---|---|---|---|---|
| Same model, no tools | 16 | 12 | — | — |
| Before (`d4dda12`): plan 2–4 sub-questions, research them in parallel | 13 | 12 | 27.8k / 25.9k | 12s / 18s |
| **After (default): route first, answer-type questions are not split** | **19** | **17** | 22.8k / 21.9k | 13s / 18s |
| After + `rounds=4` (a reviewer sends follow-up agents) | 22 | 19 | 45.4k / 44.4k | 15s / 21s |

Before → after, paired by question: dev fixed 8 and broke 2; holdout fixed 9 and broke 4.
Pooled it is 25/60 → 36/60 (exact McNemar p = 0.035). Holdout alone (12 → 17) is not significant at n = 30.

## What went wrong before

With tools, the old pipeline scored below the same model with no tools (13 vs 16 on dev).

- 13 of its 17 wrong answers ended with "cannot determine".
- The planner wrote sub-questions such as "In which city is *that* racecourse?" and sent them to parallel agents.
  Those agents could not know what "that" referred to.
- 27 of 91 sub-questions referred to an earlier answer. 16 of those agents gave up after one step and 17 returned
  no sources. Of the 64 self-contained sub-questions, 2 gave up and 3 returned no sources.

## What changed

- **Route first.** One short LLM call classifies the question. *Report* questions (explain, compare, what's new)
  go through the unchanged pipeline. *Answer* questions (one specific fact) are not split: the whole question
  goes to one agent, whose step limit went from 6 to 10, and the report must lead with the answer.
- **Router accuracy:** all 20 questions in `eval/questions.json` were routed to *report*; 59 of 60 FRAMES
  questions were routed to *answer*. The remaining one produced unparseable output, which falls back to *report*.

## What did not pay off

- **Follow-up rounds.** With routing, the reviewer asked for follow-ups on about 15% of questions and rescued
  1 of 9 (dev 1/4, holdout 0/5), while doubling tokens. The 2–3 question gap in the table is within
  run-to-run noise. It is off by default; `payload.rounds` (1–4) turns it on.
- **Splitting chain questions and finishing them with follow-up rounds.** An earlier design did this. It
  reached similar accuracy (dev 23, holdout 17) at 74k–99k tokens per question. On the 20 report questions
  it cost 2.9× as much and depth fell from 95% to 88%. That design was dropped and its code is not in the repo.

## Report questions did not regress

`v5-route` vs `v4-final` on the 20 questions in `eval/questions.json`:

| | v4-final | v5-route |
|---|---|---|
| Depth | 95% | 93% |
| Citation accuracy (strict / lenient) | 60% / 87% | 62% / 87% |
| Cost per question | $0.049 | $0.047 |
| Agents with no sources | 12% | 6% |
| p50 latency | 24s | 27s |

## Limits

- 30 questions per set gives roughly ±18 points at 95% for a single accuracy. Only the pooled paired
  comparison is significant.
- dev was used for tuning, so holdout is the number to quote.
- FRAMES dates from 2024, and the model alone answers 40–53% of these questions from memory.
- Of the 24 answers still wrong with the default settings, about 10 give no answer and about 14 commit to a
  wrong one: a misread question (the coaster's opening year instead of the resort's), arithmetic over several
  lookups, or a table the agent never reached. `web_fetch` still returns only the first 2000 characters of a page.
- "Gold articles seen" in the result files undercounts, because the trace keeps only the first 300 characters
  of each tool result.

## Re-run

    curl -L -o /tmp/frames.tsv https://huggingface.co/datasets/google/frames-benchmark/resolve/main/test.tsv
    python3 eval/frames_eval.py --tsv /tmp/frames.tsv --label dev --closed-book
    python3 eval/frames_eval.py --tsv /tmp/frames.tsv --label holdout --offset 30 --closed-book
    python3 eval/frames_eval.py --tsv /tmp/frames.tsv --label holdout-rounds4 --offset 30 --payload '{"rounds":4}'

Per-question records: `frames-{dev,holdout}{,-baseline,-rounds4}.json` in this directory.
The `-baseline` files were produced by running the same script against a build of `d4dda12`.
