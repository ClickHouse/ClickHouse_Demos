# Part 4: Knowledge Validation

**Estimated time:** 60–90 minutes
**No infrastructure changes** — this part is purely assessment.

## Overview

You've executed an Elasticsearch → ClickHouse migration end-to-end. Part 4 tests whether you understand **why** each architectural choice was made and whether you can apply the same reasoning to a customer scenario you haven't seen before.

The questions deliberately avoid lab-specific trivia ("what was the docker run command on Step 3b?") and focus on:

- **ClickHouse fundamentals** — MergeTree mechanics, sparse indexes, sort keys, materialized columns, dictionaries, AggregatingMergeTree
- **OpenTelemetry data model** — resource vs span vs log attributes, span kinds, signal correlation
- **Migration trade-offs** — Map vs structured columns, dual-write vs dual-read, ILM vs TTL, sampling, schema evolution
- **Production debugging** — diagnosing slow queries, diagnosing missing data, telling apart "wrong query" from "wrong data"

## Structure

| File | Purpose |
|---|---|
| [`exercises/mcq.md`](exercises/mcq.md) | 15 multiple-choice questions covering the conceptual material above |
| [`exercises/open-questions.md`](exercises/open-questions.md) | 5 open-ended exercises: schema design, migration plan, two debugging scenarios, one trade-off analysis |
| [`solutions/mcq-answers.md`](solutions/mcq-answers.md) | Answer key with explanations — **do not open until you've answered all 15 MCQs** |
| [`solutions/open-answers.md`](solutions/open-answers.md) | Model answers for the open-ended exercises |

## Pass Criteria

| Component | Weight | Pass threshold |
|---|---|---|
| Multiple-choice questions | 40% | **≥ 11 out of 15 correct** (~73%) |
| Open-ended exercises | 60% | Self-graded against the model answers; you should be able to articulate the same trade-offs and reach a similar (not identical) conclusion for at least 4 of 5 |

If you score below the threshold on the MCQs, re-read [Part 2's ADR template](../part2/exercises/adr-template.md) and the teaching-point callouts inside [Part 3's README](../part3/README.md) before retrying. If you struggle with the open-ended scenarios, the most useful re-read is the [Part 2 solutions](../part2/solutions/) — those show the kind of structured reasoning the open-ended questions reward.

## How to take the assessment

1. Block 60–90 minutes — these questions reward thinking, not lookup.
2. Open `exercises/mcq.md` and answer every question. Write your answer letter (A/B/C/D) directly in the file under each question.
3. Open `exercises/open-questions.md`. Each open-ended exercise has a recommended timebox (10–20 min). For schema design and migration plan, write actual SQL / actual prose — not bullet lists of buzzwords.
4. Once you've written all answers, open the corresponding `solutions/` file and self-grade. **Note where you disagree with the model answer** — those are the most valuable disagreements to discuss with a peer.
5. Run `bash ../common/cleanup.sh` to tear down the Part 1 / Part 3 infrastructure.

## What you should NOT need

- The lab repository checked out (the questions are self-contained)
- A running ClickHouse Cloud service
- Re-reading the OpenTelemetry spec end-to-end — relevant excerpts are quoted inside the questions where needed

---

**Done!** When you've finished both exercise files and reviewed the model answers, run `bash ../common/cleanup.sh` to tear down all provisioned resources and complete the lab.
