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
