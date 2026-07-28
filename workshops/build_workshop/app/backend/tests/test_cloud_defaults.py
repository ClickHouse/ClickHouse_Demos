import pytest

from app.settings import Settings


@pytest.fixture(scope="session", autouse=True)
def wait_for_api() -> None:
    # This is a settings unit test; it must not poll a running API.
    return None


def test_clickhouse_defaults_cannot_target_a_local_server(monkeypatch):
    for name in (
        "CLICKHOUSE_HOST",
        "CLICKHOUSE_PORT",
        "CLICKHOUSE_SECURE",
    ):
        monkeypatch.delenv(name, raising=False)

    settings = Settings(_env_file=None)

    assert settings.clickhouse_host == ""
    assert settings.clickhouse_port == 8443
    assert settings.clickhouse_secure_effective is True
