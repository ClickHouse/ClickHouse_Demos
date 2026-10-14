from pathlib import Path
import json
import subprocess

ROOT=Path(__file__).resolve().parents[1]


def test_primary_compose_is_local_edge_without_databases_or_frontend_secrets():
    result=subprocess.run(['docker','compose','--env-file',str(ROOT/'app/.env.workshop.example'),'-f',str(ROOT/'app/docker-compose.workshop.yml'),'config','--format','json'],capture_output=True,text=True,check=True)
    config=json.loads(result.stdout)
    assert set(config['services'])=={'backend','frontend'} # Optional profiles are explicitly started.
    frontend=config['services']['frontend']
    assert all(p['host_ip']=='127.0.0.1' for p in frontend['ports'])
    assert 'environment' not in frontend
    assert 'ports' not in config['services']['backend']
    assert not frontend['build'].get('args')
    assert Path(config['services']['backend']['build']['context'])==ROOT


def test_generated_admin_and_replication_secrets_stay_outside_runtime(tmp_path):
    app=tmp_path/'app';app.mkdir()
    compose=app/'docker-compose.yml';compose.write_text((ROOT/'app/docker-compose.workshop.yml').read_text())
    env=app/'.env.workshop'
    env.write_text('PRICING_POSTGRES_ADMIN_DSN=admin-only-secret\nPRICING_POSTGRES_APP_PASSWORD=app-password-secret\nPRICING_POSTGRES_CDC_PASSWORD=cdc-only-secret\nPRICING_POSTGRES_DSN=app-dsn\n')
    result=subprocess.run(['docker','compose','--env-file',str(env),'-f',str(compose),'--profile','writer','config','--format','json'],capture_output=True,text=True,check=True)
    config=json.loads(result.stdout)
    for name in ('backend','writer'):
        runtime=config['services'][name]['environment']
        assert runtime['PRICING_POSTGRES_DSN']=='app-dsn'
        assert not {'PRICING_POSTGRES_ADMIN_DSN','PRICING_POSTGRES_APP_PASSWORD','PRICING_POSTGRES_CDC_PASSWORD'} & runtime.keys()
