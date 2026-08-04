from eval.serialize import result_payload


def test_result_payload_persists_exact_leaderboard_measurements():
    """Dropping locally measured cost or latency must make a result incomplete."""
    payload = result_payload(
        sql="SELECT 1", rows=[(1,)], cols=["x"], error=None, outcome_hint="ok",
        transcript=[{"role": "user", "content": "one?"}],
        cost_usd=0.012345, latency_ms=1875, retries=1, tier=3,
    )
    assert payload == {
        "sql": "SELECT 1",
        "columns": ["x"],
        "rows": [[1]],
        "error": None,
        "outcome_hint": "ok",
        "transcript": [{"role": "user", "content": "one?"}],
        "cost_usd": 0.012345,
        "latency_ms": 1875,
        "retries": 1,
        "tier": 3,
    }
