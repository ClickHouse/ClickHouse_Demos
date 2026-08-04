from scripts.provision_langfuse_evaluators import judge_prompt


def test_judge_prompt_extracts_runtime_prompt_only():
    prompt = judge_prompt()
    assert "You are a senior ClickHouse SQL reviewer" in prompt
    assert "{{question}}" in prompt
    assert "Set this up in" not in prompt
