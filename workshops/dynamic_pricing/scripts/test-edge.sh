#!/bin/bash
set -euo pipefail
lab_root=$(cd "$(dirname "$0")/.." && pwd)
cd "$lab_root"
project="pricing-edge-$$"
compose=(docker compose -p "$project" --env-file app/.env.workshop.example --profile developer -f app/docker-compose.workshop.yml -f app/docker-compose.developer.yml)
trap '"${compose[@]}" down --volumes >/dev/null 2>&1' EXIT
export FRONTEND_HOST_PORT=$(.venv/bin/python -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
export PRICING_ALLOWED_ORIGIN="http://localhost:$FRONTEND_HOST_PORT" PRICING_OWNER_ID=learner-1 PRICING_SCENARIO_ID=grocery PRICING_CLICKHOUSE_DATABASE=pricing PRICING_MODEL_PATH=/opt/pricing/pricing/artifacts/development/model.json
"${compose[@]}" up --build --detach --wait --wait-timeout 120
pg=$("${compose[@]}" port postgres 5432);ch=$("${compose[@]}" port clickhouse 8123)
export PRICING_TEST_POSTGRES_DSN="postgresql://pricing_test:test-only-password@127.0.0.1:${pg##*:}/postgres"
export PRICING_TEST_CLICKHOUSE_PORT="${ch##*:}"
PYTHONPATH="$PWD:$PWD/tests" .venv/bin/python tests/prepare_developer.py
.venv/bin/python - <<'PY'
import httpx,uuid,os
origin=os.environ["PRICING_ALLOWED_ORIGIN"]
with httpx.Client(base_url=origin,timeout=90) as client:
 assert client.get('/api/health').status_code==200
 session=client.post('/api/session',json={},headers={'Origin':origin})
 assert session.status_code==200,session.text
 assert session.json()['data']['ingestion_mode']=='local_replay'
 headers={'Origin':origin,'X-Pricing-Generation':'1'}
 proposal=client.post('/api/candidates',json={'subject':{'sku':101,'store_id':1,'channel':'store'}},headers=headers)
 assert proposal.status_code==200,proposal.text
 candidate=next(c for c in proposal.json()['candidates'] if c['candidate']['price_minor']==650)
 approved=client.post('/api/candidates/'+candidate['candidate']['candidate_id']+'/approve',json={'idempotency_key':str(uuid.uuid4())},headers=headers)
 assert approved.status_code==200,approved.text
 receipt=approved.json()['receipt']
 sale=client.post('/api/sales',json={'event_id':str(uuid.uuid4()),'subject':{'sku':101,'store_id':1,'channel':'store'},'quantity':1,'occurred_at':'2026-01-30T12:00:00Z','expected_price_version':receipt['applied_price_version']},headers=headers)
 assert sale.status_code==200 and sale.json()['data']['unit_price_minor']==650,sale.text
 page=client.get('/');assert page.status_code==200 and 'Dynamic pricing workbench' in page.text
 print('Container edge rehearsal passed: local test replay, session, proposal, learner approval, receipt, subsequent sale.')
PY

"${compose[@]}" --profile writer run --build --rm --no-deps writer python writer.py --count 1 --interval 0.1
