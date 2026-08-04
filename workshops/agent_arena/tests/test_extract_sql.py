from agents.loop import extract_sql_block


def test_extracts_fenced_sql():
    text = "Here:\n```sql\nSELECT 1\n```\nDone."
    assert extract_sql_block(text) == "SELECT 1"


def test_takes_last_block_for_cot():
    text = "```sql\nSELECT bad\n```\nfinal:\n```sql\nSELECT good\n```"
    assert extract_sql_block(text) == "SELECT good"


def test_falls_back_to_raw_when_no_fence():
    text = "SELECT 1 FROM v_orders"
    assert extract_sql_block(text) == "SELECT 1 FROM v_orders"


def test_handles_bare_triple_backticks():
    text = "```\nSELECT 2\n```"
    assert extract_sql_block(text) == "SELECT 2"
