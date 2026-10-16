import pytest
from generator.retail import generate_retail
from pricing_lab.retail_history import RetailHistory
from pricing_lab.retail_schema import install_retail_schema

pytestmark = pytest.mark.integration


def test_load_resume_and_console_parity(analytics, ledger, tmp_path):
    path = generate_retail(tmp_path / 'data', 100)
    history = RetailHistory(analytics, ledger)
    result = history.load(path.as_uri())
    assert result['rows'] == 100
    assert history.load(path.as_uri())['rows'] == 100
    assert history.register_console(path.as_uri())['rows'] == 100
    assert analytics.query(f'SELECT count() n FROM {analytics.database}.retail_sales')[0]['n'] == 100
    assert not analytics.query('SELECT name FROM system.tables WHERE database={db:String} AND name=\'retail_sales_minute\'', {'db': analytics.database})


def test_lost_ack_does_not_duplicate_shard(analytics, ledger, tmp_path, monkeypatch):
    path = generate_retail(tmp_path / 'data', 100)
    history = RetailHistory(analytics, ledger)
    original = history._insert_shard
    def lost(*args, **kwargs):
        original(*args, **kwargs)
        raise ConnectionError('lost acknowledgment')
    monkeypatch.setattr(history, '_insert_shard', lost)
    with pytest.raises(ConnectionError):
        history.load(path.as_uri())
    monkeypatch.setattr(history, '_insert_shard', original)
    assert history.load(path.as_uri())['rows'] == 100


def test_wrong_console_totals_rejected(analytics, ledger, tmp_path):
    path = generate_retail(tmp_path / 'data', 100)
    history = RetailHistory(analytics, ledger)
    install_retail_schema(analytics)
    with pytest.raises(ValueError, match='totals'):
        history.register_console(path.as_uri())


def test_manual_insert_registers_without_reinserting(analytics, ledger, tmp_path):
    import json
    from uuid import UUID
    path = generate_retail(tmp_path / 'data', 100)
    manifest = json.loads(path.read_text())
    history = RetailHistory(analytics, ledger)
    install_retail_schema(analytics)
    for file in manifest['files']:
        history._insert_shard('retail_sales_console', path.parent, file, UUID(file['sha256'][:32]))
    assert history.register_console(path.as_uri())['rows'] == 100
    assert history.load(path.as_uri())['rows'] == 100


def test_registration_rejects_wrong_subject_with_matching_totals(analytics, ledger, tmp_path):
    path = generate_retail(tmp_path / 'data', 100)
    history = RetailHistory(analytics, ledger)
    history.load(path.as_uri())
    db = analytics.database
    analytics.command(f'CREATE TABLE {db}.bad AS {db}.retail_sales')
    analytics.command(f'INSERT INTO {db}.bad SELECT * REPLACE(toUInt16(6000) AS store_id) FROM {db}.retail_sales')
    analytics.command(f'EXCHANGE TABLES {db}.bad AND {db}.retail_sales')
    with pytest.raises(ValueError, match='facts'):
        history.register_console(path.as_uri())


def test_manual_activation_recovers_lost_exchange_ack(analytics, ledger, tmp_path, monkeypatch):
    import json
    from uuid import UUID
    path = generate_retail(tmp_path / 'data', 100)
    history = RetailHistory(analytics, ledger)
    install_retail_schema(analytics)
    for file in json.loads(path.read_text())['files']:
        history._insert_shard('retail_sales_console', path.parent, file, UUID(file['sha256'][:32]))
    original = analytics.command
    def lost(sql, *args, **kwargs):
        result = original(sql, *args, **kwargs)
        if sql.startswith('EXCHANGE TABLES') and 'retail_sales_console' in sql:
            raise ConnectionError('lost exchange acknowledgment')
        return result
    monkeypatch.setattr(analytics, 'command', lost)
    with pytest.raises(ConnectionError):
        history.register_console(path.as_uri())
    monkeypatch.setattr(analytics, 'command', original)
    assert history.register_console(path.as_uri())['rows'] == 100
