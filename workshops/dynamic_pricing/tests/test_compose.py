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


def test_telemetry_collector_writes_to_the_workshop_clickhouse_service(tmp_path):
    env=tmp_path/'collector.env'
    env.write_text('PRICING_CLICKHOUSE_HOST=workshop.clickhouse.cloud\nPRICING_CLICKHOUSE_PORT=8443\nPRICING_CLICKHOUSE_USERNAME=default\nPRICING_CLICKHOUSE_PASSWORD=collector-test-password\nPRICING_CLICKSTACK_DATABASE=otel\n')
    result=subprocess.run(['docker','compose','--env-file',str(env),'-f',str(ROOT/'app/docker-compose.workshop.yml'),'--profile','telemetry','config','--format','json'],capture_output=True,text=True,check=True)
    collector=json.loads(result.stdout)['services']['collector']
    assert collector['image'].startswith('clickhouse/clickstack-otel-collector@sha256:')
    runtime=collector['environment']
    assert runtime['CLICKHOUSE_ENDPOINT']=='https://workshop.clickhouse.cloud:8443'
    assert runtime['CLICKHOUSE_USER']=='default'
    assert runtime['CLICKHOUSE_PASSWORD']=='collector-test-password'
    assert runtime['HYPERDX_OTEL_EXPORTER_CLICKHOUSE_DATABASE']=='otel'
    assert not {'PRICING_CLICKSTACK_ENDPOINT','PRICING_CLICKSTACK_API_KEY'} & runtime.keys()
    assert collector.get('command') is None # Keep the image's schema migrations and pipelines.
    config=(ROOT/'obs/collector.yaml').read_text()
    assert 'otlphttp/clickstack' not in config
    example=(ROOT/'app/.env.workshop.example').read_text()
    assert 'PRICING_CLICKSTACK_ENDPOINT=' not in example
    assert 'PRICING_CLICKSTACK_API_KEY=' not in example
