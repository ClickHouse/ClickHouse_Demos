from datetime import UTC,datetime,timedelta
import pytest
from app.loadgen import retail_writer

START=datetime(2026,1,30,2,tzinfo=UTC)

@pytest.mark.parametrize('seconds,expected',[(0,100000),(119.999,100000),(120,10000),(3600,10000),(10800,10000)])
def test_burst_uses_business_clock_not_process_start(seconds,expected):
    rate,end=retail_writer.sales_rate(START+timedelta(seconds=seconds),10000,100000,2)
    assert rate==expected
    assert end==(START+timedelta(minutes=2) if seconds<120 else None)


def test_no_burst_preserves_explicit_single_rate():
    assert retail_writer.sales_rate(START,300,None,0)==(300,None)


def test_three_hour_duration_is_supported(monkeypatch):
    class ReachedSettings(Exception):pass
    monkeypatch.setattr(retail_writer.Settings,'from_env',lambda **kw:(_ for _ in ()).throw(ReachedSettings()))
    with pytest.raises(ReachedSettings):
        retail_writer.main(['--minutes','180','--burst-transactions-per-minute','100000','--burst-minutes','2'])

@pytest.mark.parametrize('flags',[
 ['--minutes','181'],['--burst-minutes','-1'],['--burst-minutes','2'],
 ['--burst-transactions-per-minute','100000'],
 ['--burst-transactions-per-minute','1000001','--burst-minutes','2'],
 ['--burst-transactions-per-minute','100000','--burst-minutes','nan'],
])
def test_invalid_pacing_is_rejected_before_connecting(flags):
    with pytest.raises(SystemExit):retail_writer.main(flags)
