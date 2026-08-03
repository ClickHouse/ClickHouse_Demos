import pytest

from collector.config import Settings


def test_settings_load_cloud_defaults(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_HOST", "abc.aws.clickhouse.cloud")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "secret")

    settings = Settings.from_env()

    assert settings.clickhouse_port == 8443
    assert settings.clickhouse_secure is True
    assert settings.mode == "live"
    assert settings.queue_capacity == 10_000


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("POLYMARKET_MODE", "paper", "must be live or fixture"),
        ("CLICKHOUSE_SECURE", "maybe", "must be true or false"),
        ("POLYMARKET_MARKET_COUNT", "0", "must be greater than zero"),
    ],
)
def test_settings_reject_invalid_values(monkeypatch, name, value, message):
    monkeypatch.setenv("CLICKHOUSE_HOST", "abc.aws.clickhouse.cloud")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "secret")
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match=message):
        Settings.from_env()


def test_settings_require_cloud_credentials(monkeypatch):
    monkeypatch.delenv("CLICKHOUSE_HOST", raising=False)
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "secret")

    with pytest.raises(ValueError, match="CLICKHOUSE_HOST is required"):
        Settings.from_env()
