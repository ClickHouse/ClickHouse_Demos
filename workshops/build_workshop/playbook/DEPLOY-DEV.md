# Deploying the playbook to dev.demohouse.cloud/workshop

This serves the playbook under the team's existing **dev** DemoHouse stack
(`https://dev.demohouse.cloud/workshop`), behind the same ALB + nginx. The
docs are served without the magic-link gate, so the URL is shareable directly.

The playbook image is built here (this repo) and pushed to the DemoHouse ECR
registry; the runtime wiring (compose service + nginx route) lives in the
DemoHouse repo on branch `feat/dev-workshop-route`.

## Prerequisites (operator machine)

1. **AWS SSO** for the dev-stack account (`959934561610`):
   ```bash
   aws sso login --profile sa
   ```
   The local `164313782301_AccountAdministrators` profile is a *different*
   account and cannot reach the dev stack, ECR, or the EC2 instance.

2. **Docker running** (OrbStack). The build uses `buildx --platform=linux/arm64`
   because the dev EC2 is Graviton (`t4g.xlarge`).

3. The DemoHouse repo checked out at `~/casa/projects/posthouse` on branch
   `feat/dev-workshop-route` (has the compose `workshop` service + nginx route).

## One-time: create the ECR repo

```bash
aws ecr create-repository \
  --repository-name posthouse-demo-workshop \
  --region ap-southeast-1 --profile sa
```

## Build + push the workshop image

```bash
cd <this repo>/workshops/build_workshop/playbook

REG=959934561610.dkr.ecr.ap-southeast-1.amazonaws.com
aws ecr get-login-password --region ap-southeast-1 --profile sa \
  | docker login --username AWS --password-stdin "$REG"

docker buildx build --platform=linux/arm64 \
  --build-arg NEXT_PUBLIC_BASE_PATH=/workshop \
  --build-arg NEXT_PUBLIC_SITE_URL=https://dev.demohouse.cloud \
  -t "$REG/posthouse-demo-workshop:dev" \
  --push .
```

## Ship the compose + nginx changes onto the dev EC2

From the DemoHouse repo (branch `feat/dev-workshop-route`) this re-uploads
`docker-compose.yml` + `infra/nginx/` to S3 and SSM-syncs them onto the box:

```bash
cd ~/casa/projects/posthouse
./scripts/deploy-dev.sh --skip-build --skip-terraform
```

Then bring up the workshop container and refresh nginx's upstream cache. The
dev instance id comes from `terraform -chdir=infra/terraform-dev output -raw instance_id`:

```bash
INST=$(terraform -chdir=infra/terraform-dev output -raw instance_id)
aws ssm send-command --profile sa --region ap-southeast-1 \
  --instance-ids "$INST" --document-name AWS-RunShellScript \
  --parameters 'commands=[
    "cd /opt/demohouse",
    "aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 959934561610.dkr.ecr.ap-southeast-1.amazonaws.com",
    "TAG=dev docker compose pull workshop",
    "TAG=dev docker compose up -d --no-deps --no-build workshop",
    "docker compose up -d --no-deps --force-recreate nginx"
  ]'
```

## Verify

```bash
for p in /workshop /workshop/docs/learner/00-setup /workshop/docs/instructor/00-setup; do
  echo "$p -> $(curl -s -o /dev/null -w '%{http_code}' https://dev.demohouse.cloud$p)"
done
```

All three should return `200`. (Locally verified against the standalone build:
`/` -> 404, `/workshop` -> 200, learner + instructor setup pages -> 200.)
