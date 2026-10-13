from pricing_lab.cli import main,build_preflight
from pricing_lab.settings import Settings


def test_offline_preflight_labels_recorded_mode(capsys):
    assert main(['preflight','--participant','learner-1','--module','setup','--offline'])==0
    assert 'recorded' in capsys.readouterr().out


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
