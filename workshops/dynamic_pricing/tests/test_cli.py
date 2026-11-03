from pricing_lab.cli import main,build_preflight,preflight_layers
from pricing_lab.settings import Settings


def test_offline_preflight_labels_recorded_mode(capsys):
    assert main(['preflight','--participant','learner-1','--module','setup','--offline'])==0
    assert 'recorded' in capsys.readouterr().out


def test_telemetry_preflight_is_a_supported_module(capsys):
    assert main(['preflight','--module','telemetry','--offline'])==0
    assert '"module": "telemetry"' in capsys.readouterr().out
    assert preflight_layers('telemetry') == {
        'history': True, 'cdc': True, 'clickstack': True, 'agent': False,
    }


def test_cli_rejects_other_participant_without_connecting(capsys):
    assert main(['preflight','--participant','other','--offline'])==2
    assert 'participant_mismatch' in capsys.readouterr().err


def test_setup_does_not_require_later_module_credentials():
    result=build_preflight(Settings(), 'setup')
    assert not any('clickstack' in check['name'] or 'cdc' in check['name'] for check in result['checks'])

def test_installed_workshop_command_runs_outside_repository_cwd(tmp_path):
    import os,subprocess,sys
    from pathlib import Path
    environment={**os.environ,'PYTHONPATH':''}
    result=subprocess.run([str(Path(sys.executable).parent/'workshop'),'preflight','--participant','learner-1','--offline'],cwd=tmp_path,env=environment,capture_output=True,text=True)
    assert result.returncode==0,result.stderr
    assert 'recorded' in result.stdout

def test_application_preflight_does_not_probe_host_toolchain():
    result=build_preflight(Settings(),'setup',check_toolchain=False)
    assert result['toolchain_scope']=='application'
    assert not {'docker','node'} & {c['name'] for c in result['checks']}


def test_tool_versions_reject_wrong_major_and_cli_release(monkeypatch):
    import subprocess
    from pricing_lab.cli import tool_version
    output='v24.0.0'
    monkeypatch.setattr(subprocess,'run',lambda argv,**kwargs:subprocess.CompletedProcess(argv,0,stdout=output))
    assert not tool_version(['node','--version'],22,minimum=(22,12,0))
    output='v22.11.0'
    assert not tool_version(['node','--version'],22,minimum=(22,12,0))
    output='v22.12.0'
    assert tool_version(['node','--version'],22,minimum=(22,12,0))
    output='clickhousectl 0.4.3'
    assert not tool_version(['clickhousectl','--version'],0,exact=(0,4,2))
    output='clickhousectl 0.4.2'
    assert tool_version(['clickhousectl','--version'],0,exact=(0,4,2))


def test_register_history_parses_without_a_manifest_override(capsys):
    assert main(['register-history','--offline'])==2
    assert 'offline_command_unavailable' in capsys.readouterr().err


def test_retail_import_requires_explicit_dataset(capsys):
    assert main(['retail-import']) == 2
    assert 'PRICING_RETAIL_DATASET_MANIFEST' in capsys.readouterr().err


def test_retail_rollup_requires_retail_mode_before_validation(monkeypatch,capsys):
    from types import SimpleNamespace
    import pricing_lab.cli as cli
    import pricing_lab.retail_history as history
    calls=[]
    monkeypatch.setattr(cli,'AnalyticsClient',lambda settings:SimpleNamespace(database='pricing',close=lambda:None))
    monkeypatch.setattr(cli,'Ledger',lambda dsn:object())
    monkeypatch.setattr(history,'RetailHistory',lambda *args:SimpleNamespace(register_rollup=lambda:calls.append('validate')))
    assert main(['retail-register-rollup'])==2
    assert calls==[]
    assert 'retail_mode_required' in capsys.readouterr().err


def test_missing_retail_cdc_is_reported_before_long_rollup_validation(monkeypatch,capsys):
    from types import SimpleNamespace
    import pricing_lab.cli as cli
    import pricing_lab.retail_history as history
    from pricing_lab.retail_cdc import RETAIL_TABLES
    monkeypatch.setenv('PRICING_RETAIL_MODE','true')
    calls=[]
    names=[{'name':name+'_cdc'} for name in RETAIL_TABLES if name!='retail_batches']
    monkeypatch.setattr(cli,'AnalyticsClient',lambda settings:SimpleNamespace(database='pricing',query=lambda *args:names,close=lambda:None))
    monkeypatch.setattr(cli,'Ledger',lambda dsn:object())
    monkeypatch.setattr(history,'RetailHistory',lambda *args:SimpleNamespace(register_rollup=lambda:calls.append('validate')))
    assert main(['retail-register-rollup'])==2
    assert calls==[]
    error=capsys.readouterr().err
    assert 'retail_cdc_missing' in error and 'retail_batches_cdc' in error


def test_rollup_reports_post_validation_stage_without_exposing_raw_error(monkeypatch,capsys):
    from types import SimpleNamespace
    import pricing_lab.cli as cli
    import pricing_lab.retail_history as history
    import pricing_lab.retail_initialize as initialize
    from pricing_lab.retail_cdc import RETAIL_TABLES
    monkeypatch.setenv('PRICING_RETAIL_MODE','true')
    names=[{'name':name+'_cdc'} for name in RETAIL_TABLES]
    monkeypatch.setattr(cli,'AnalyticsClient',lambda settings:SimpleNamespace(database='pricing',query=lambda *args:names,close=lambda:None))
    monkeypatch.setattr(cli,'Ledger',lambda dsn:object())
    monkeypatch.setattr(history,'RetailHistory',lambda *args:SimpleNamespace(register_rollup=lambda **kwargs:{'ready':True}))
    def fail(*args):raise ValueError('PRIVATE_MODEL_ERROR_DO_NOT_PRINT')
    monkeypatch.setattr(initialize,'install_retail_model',fail)
    assert main(['retail-register-rollup'])==2
    captured=capsys.readouterr()
    assert 'Installing the prepared pricing model' in captured.out
    assert 'PRIVATE_MODEL_ERROR_DO_NOT_PRINT' not in captured.err


def test_retail_register_rollup_full_check_is_opt_in(monkeypatch,capsys):
    from types import SimpleNamespace
    import pricing_lab.cli as cli
    import pricing_lab.retail_history as history
    import pricing_lab.retail_cdc as cdc
    import pricing_lab.retail_initialize as initialize
    import pricing_lab.retail_schema as schema
    monkeypatch.setenv('PRICING_RETAIL_MODE','true')
    monkeypatch.setattr(cli,'AnalyticsClient',lambda settings:SimpleNamespace(database='pricing',close=lambda:None))
    monkeypatch.setattr(cli,'Ledger',lambda dsn:object())
    calls=[]
    monkeypatch.setattr(history,'RetailHistory',lambda *args:SimpleNamespace(register_rollup=lambda **kwargs:calls.append(kwargs) or {'ready':True}))
    monkeypatch.setattr(cdc,'require_retail_cdc',lambda *args:None)
    monkeypatch.setattr(initialize,'install_retail_model',lambda *args:None)
    monkeypatch.setattr(schema,'install_retail_analysis',lambda *args:None)
    assert main(['retail-register-rollup'])==0
    assert calls==[{'full_check':False}]
    assert main(['retail-register-rollup','--full-check'])==0
    assert calls[-1]=={'full_check':True}
