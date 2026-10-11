from datetime import UTC, datetime
import pytest


def test_lease_uses_wall_clock(golden_snapshot, clock):
    before = clock.wall_now()
    clock.restore_scenario(datetime(2020, 1, 1, tzinfo=UTC))
    assert clock.wall_now() >= before
    assert golden_snapshot.expires_at > clock.wall_now()
    assert clock.scenario_now() == datetime(2020, 1, 1, tzinfo=UTC)


def test_naive_restore_cannot_make_clock_ambiguous(clock):
    with pytest.raises(ValueError):
        clock.restore_scenario(datetime(2020, 1, 1))
