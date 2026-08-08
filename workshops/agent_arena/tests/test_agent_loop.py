from copy import deepcopy
from types import SimpleNamespace

from agents.llm import ConverseResult, Usage
from agents.loop import run_agent
from arena.config import ModelCfg, PromptCfg


def _model_cfg() -> ModelCfg:
    return ModelCfg(
        id="test/model",
        name="test-model",
        family="test",
        price_per_1m_in=0,
        price_per_1m_out=0,
    )


class _SequencedLLM:
    def __init__(self, responses: list[ConverseResult]):
        self.responses = iter(responses)
        self.calls = []

    def converse(self, model_id, system, messages, inference):
        self.calls.append((model_id, system, deepcopy(messages), deepcopy(inference)))
        return next(self.responses)


class _RecordingClickHouse:
    def __init__(self):
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        return SimpleNamespace(rows=[(1,)], cols=["answer"])


def test_empty_response_retries_original_request_without_self_correction():
    llm = _SequencedLLM([
        ConverseResult(text=" \n\t", usage=Usage(11, 0)),
        ConverseResult(text="```sql\nSELECT 1\n```", usage=Usage(13, 4)),
    ])
    ch = _RecordingClickHouse()

    result = run_agent(
        question="Return one",
        model_cfg=_model_cfg(),
        prompt_cfg=PromptCfg(name="P1_zeroshot", self_correct=False),
        schema_ctx="CREATE VIEW v_numbers AS SELECT 1 AS answer",
        ch=ch,
        llm=llm,
        inference={"temperature": 0},
        max_retries=1,
    )

    assert result.sql == "SELECT 1"
    assert result.rows == [(1,)]
    assert result.cols == ["answer"]
    assert result.error is None
    assert result.outcome_hint == "ok"
    assert result.attempts == 2
    assert result.usage == Usage(24, 4)
    assert ch.queries == ["SELECT 1"]
    assert len(llm.calls) == 2
    assert llm.calls[1] == llm.calls[0]
    assert [turn["role"] for turn in result.transcript] == [
        "system", "user", "assistant", "assistant",
    ]
    assert [turn["content"] for turn in result.transcript[2:]] == [
        " \n\t", "```sql\nSELECT 1\n```",
    ]


def test_all_empty_responses_exhaust_bounded_retry_budget():
    responses = [
        ConverseResult(text="", usage=Usage(5, 0)),
        ConverseResult(text="   ", usage=Usage(7, 0)),
        ConverseResult(text="\n", usage=Usage(9, 0)),
    ]
    llm = _SequencedLLM(responses)
    ch = _RecordingClickHouse()

    result = run_agent(
        question="Return one",
        model_cfg=_model_cfg(),
        prompt_cfg=PromptCfg(name="P1_zeroshot", self_correct=False),
        schema_ctx="CREATE VIEW v_numbers AS SELECT 1 AS answer",
        ch=ch,
        llm=llm,
        inference={},
        max_retries=2,
    )

    assert result.sql == ""
    assert result.rows is None
    assert result.cols is None
    assert result.error == "policy: empty SQL"
    assert result.outcome_hint == "sql_policy_rejected"
    assert result.attempts == 3
    assert result.usage == Usage(21, 0)
    assert ch.queries == []
    assert len(llm.calls) == 3
    assert llm.calls[1] == llm.calls[0]
    assert llm.calls[2] == llm.calls[0]
    assert [turn["role"] for turn in result.transcript] == [
        "system", "user", "assistant", "assistant", "assistant",
    ]
    assert [turn["content"] for turn in result.transcript[2:]] == [
        "", "   ", "\n",
    ]


def test_non_empty_policy_rejection_does_not_retry_without_self_correction():
    llm = _SequencedLLM([
        ConverseResult(text="DROP TABLE v_numbers", usage=Usage(5, 3)),
        ConverseResult(text="SELECT 1", usage=Usage(7, 2)),
    ])
    ch = _RecordingClickHouse()

    result = run_agent(
        question="Return one",
        model_cfg=_model_cfg(),
        prompt_cfg=PromptCfg(name="P1_zeroshot", self_correct=False),
        schema_ctx="CREATE VIEW v_numbers AS SELECT 1 AS answer",
        ch=ch,
        llm=llm,
        inference={},
        max_retries=1,
    )

    assert result.sql == "DROP TABLE v_numbers"
    assert result.outcome_hint == "sql_policy_rejected"
    assert result.attempts == 1
    assert result.usage == Usage(5, 3)
    assert len(llm.calls) == 1
    assert ch.queries == []
    assert [turn["role"] for turn in result.transcript] == [
        "system", "user", "assistant",
    ]
