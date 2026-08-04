# Deploy the Langfuse control-loops demo to App Runner

Runbook for a human with AWS credentials. Every step has a verification you can run
before moving on; if a verification does not pass, stop there rather than continuing and
debugging two things at once.

Target: the demo in `demos/lf_control_loops` running on AWS App Runner in
`ap-southeast-1`, eventually reachable at `https://langfuse.demohouse.cloud`, with
`https://dev-langfuse.demohouse.cloud` in front of the same service for pre-demo checks.

| | |
|---|---|
| Region | `ap-southeast-1` |
| Account | `959934561610` |
| ECR repository | `posthouse-demo-langfuse-loops` |
| Container port | `8000` (Dockerfile `EXPOSE`/`CMD`, and `Port` in `apprunner-service.json`) |
| Health check | `GET /api/healthz`, HTTP, expect exactly 200. Confirmed present at `app/main.py:277` |
| App Runner service | `langfuse-control-loops` |
| Prod host | `langfuse.demohouse.cloud` |
| Dev host | `dev-langfuse.demohouse.cloud`, matching the existing `dev-labs` pattern |
| Autoscaling | `MaxSize` 2, not the App Runner default of 25. Step 3b, and read why |
| Secrets to provision | none, see below |

**Prerequisite, and it is now satisfied.** This runbook used to open by saying the
bring-your-own-key backend had not landed, that `/api/healthz` returned 404, and that a
loop endpoint without credential headers answered 200 with an error body. That is no
longer true. Verified on 2026-08-04 against an image built from this tree:
`/api/healthz` answers `200 {"ok":true}`, `/api/status` answers 200 with `"byok":true`,
and `POST /api/workflow/run` with no credential headers answers `400`. `app/main.py`
declares `/api/healthz` at line 277, before the catch-all static mount at line 356.

Run the check anyway rather than trusting this paragraph, because `app/` is under active
development and the health check path is the one thing that turns a code change into a
`CREATE_FAILED` that looks like an infrastructure problem:

```bash
docker build -t lf-control-loops:check demos/lf_control_loops
docker run --rm -d --name lf-check -p 8000:8000 lf-control-loops:check
sleep 5
curl -s -o /dev/null -w 'healthz %{http_code}\n' http://127.0.0.1:8000/api/healthz
curl -s -o /dev/null -w 'status  %{http_code}\n' http://127.0.0.1:8000/api/status
curl -s -o /dev/null -w 'workflow(no creds) %{http_code}\n' -X POST \
  -H 'Content-Type: application/json' -d '{"ticket":"probe"}' \
  http://127.0.0.1:8000/api/workflow/run
docker rm -f lf-check
```

Expect `200`, `200`, `400`. A `404` on healthz means the route moved or something was
declared after the static mount; fix that before creating the service, and change
`HealthCheckConfiguration.Path` in `apprunner-service.json` to match if the route was
renamed deliberately. A `200` on the third line is the serious one: it means the server
found credentials somewhere, and step 4 explains why that stops the deploy.

## Read this first: there are no secrets to provision

This demo is bring-your-own-key. Visitors paste their own OpenAI and Langfuse keys into
the Setup tab; the browser keeps them in `sessionStorage` and sends them per request as
`X-OpenAI-Key` / `X-Langfuse-Public-Key` / `X-Langfuse-Secret-Key` / `X-Langfuse-Host` /
`X-Openai-Model`. The server uses them for that request and stores them nowhere.

Operationally that removes a surprising amount of work: no Secrets Manager entries, no
App Runner `RuntimeEnvironmentSecrets`, no instance role (App Runner needs only an
*access* role to pull the image), no rotation schedule, no "who has the demo key"
question, and no incident if the image or a task is compromised. It is also why the
service can be public with no login. Keep it that way; a single fallback server key
would reintroduce every one of those items.

**One thing bring-your-own-key does not buy: it does not make the service safe to leave
unmetered.** Nobody can spend our model quota, but anyone can spend our App Runner
compute and data transfer, and a review took an instance down in about two minutes with
an unauthenticated flood. That is why step 3b sets an autoscaling ceiling and step 5a
recommends a rate limit. Do not read "no secrets to provision" as "nothing to control".

## Dangerous name collision, read before you touch any App Runner service

The App Runner services named **`workshop-site`** and **`dev-workshop-site`** do **not**
serve the workshop. They serve `labs.demohouse.cloud` and `dev-labs.demohouse.cloud`
through CloudFront. The workshop is the EC2/nginx/SSM stack driven by
`.github/workflows/workshop.yml` and has nothing to do with them.

So if you are cleaning up "the old workshop services", you are about to delete the labs
site. The names are the hazard. Confirm what a service actually serves before acting:

```bash
aws apprunner list-services --region ap-southeast-1 \
  --query 'ServiceSummaryList[].[ServiceName,ServiceUrl,Status]' --output table
aws apprunner list-operations --region ap-southeast-1 --service-arn <ARN> --max-results 5
```

The service this runbook creates is `langfuse-control-loops`. That is the only one these
steps ever modify.

---

## 1. Create the ECR repository

```bash
aws ecr create-repository \
  --region ap-southeast-1 \
  --repository-name posthouse-demo-langfuse-loops \
  --image-scanning-configuration scanOnPush=true \
  --image-tag-mutability MUTABLE \
  --tags Key=Project,Value=demohouse Key=Component,Value=langfuse-control-loops-demo
```

`MUTABLE` because CI moves a `:prod` tag alongside the immutable `:<sha>` tag, the same
shape `posthouse-demo-workshop` uses. Deployments are pinned by digest regardless, so the
moving tag is a convenience, not the deploy contract.

Verify:

```bash
aws ecr describe-repositories --region ap-southeast-1 \
  --repository-names posthouse-demo-langfuse-loops \
  --query 'repositories[0].[repositoryUri,imageTagMutability]' --output text
```

Expect a URI of
`959934561610.dkr.ecr.ap-southeast-1.amazonaws.com/posthouse-demo-langfuse-loops`.

Optional but recommended, so untagged layers do not accumulate forever:

```bash
aws ecr put-lifecycle-policy \
  --region ap-southeast-1 \
  --repository-name posthouse-demo-langfuse-loops \
  --lifecycle-policy-text '{"rules":[{"rulePriority":1,"description":"expire untagged after 14 days","selection":{"tagStatus":"untagged","countType":"sinceImagePushed","countUnit":"days","countNumber":14},"action":{"type":"expire"}}]}'
```

## 2. Build and push the first image

App Runner cannot be created against an empty repository, so the image comes first.

Preferred path, let CI do it: push a branch named `lf-control-loops-v1` with changes
under `demos/lf_control_loops/**`. That runs `.github/workflows/lf-control-loops.yml`,
which builds, asserts no `.env` made it into the image, smoke tests the container, and
pushes `:<sha>` plus `:prod`. The workflow's own deploy job will fail until step 3 has
created the service and the `LF_LOOPS_SERVICE_ARN` repo variable is set, which is
expected on the first run.

Manual path, for the bootstrap and for break-glass only. It exists because of a
chicken-and-egg: CI needs the `LF_LOOPS_AWS_ROLE_ARN` OIDC role to push, and on a
brand-new setup that role may not exist yet, so there would otherwise be no way to
get the first image into an empty repository. Use CI for everything after that.

**The `.env` assertion below is a required step, not a nicety.** An earlier version
of this runbook went straight to `docker buildx build --push`, which builds and
publishes in one atomic action: there is no moment at which a human can inspect the
image before it is in ECR, so the manual path structurally could not be checked, and
CI's assertion was the only credential guard anywhere. `.dockerignore` had a real
recursive-pattern bug at the time (a nested `app/.env` shipped; see the header comment
in `demos/lf_control_loops/.dockerignore`), so this was not a theoretical gap. Build,
assert, then push, in three separate commands.

```bash
export REGISTRY=959934561610.dkr.ecr.ap-southeast-1.amazonaws.com
export REPOSITORY=posthouse-demo-langfuse-loops
export TAG=$(git rev-parse HEAD)

# 1. Build into the LOCAL daemon, not to ECR. --load is what makes the assertion
# below possible: the image exists and is inspectable while still unpublished.
#
# linux/amd64 is not optional. App Runner runs x86_64 only and will not deploy an
# arm64 manifest. Do not copy the --platform linux/arm64 from workshop.yml, which
# targets a Graviton EC2 host. On an Apple Silicon laptop this build is emulated
# and slow, roughly 4-6 minutes; that is normal.
#
# --load with a --platform that is not the host's needs a builder that can hold a
# foreign-arch image. Verified working on an arm64 Mac under OrbStack with the
# `docker` driver (`docker buildx ls` showed linux/amd64 in the node's platform
# list, and `docker image inspect` on the result reported amd64/linux). On stock
# Docker Desktop with the classic image store this can fail with a "docker
# exporter does not currently support exporting manifest lists" style error; if it
# does, either enable the containerd image store or run
# `docker buildx create --driver docker-container --use` first. Do NOT respond to
# that error by switching back to --push: that removes the assertion window, which
# is the entire reason this step is shaped this way.
docker buildx build \
  --platform linux/amd64 \
  --provenance false \
  --tag "$REGISTRY/$REPOSITORY:$TAG" \
  --tag "$REGISTRY/$REPOSITORY:prod" \
  --load \
  demos/lf_control_loops
```

```bash
# 2. REQUIRED. Same assertions the CI workflow runs, on the same image, before
# anything leaves this laptop. Do not skip this because "the .dockerignore handles
# it": the whole point is that a broken .dockerignore is exactly what this catches,
# and it has been broken before. --platform is needed to run an amd64 image on an
# arm64 host; Docker Desktop emulates it.
set -euo pipefail

found=$(docker run --rm --platform linux/amd64 --entrypoint sh \
  "$REGISTRY/$REPOSITORY:$TAG" -c \
  'find / -xdev \( -name ".env" -o -name ".env.*" -o -name "*.env" \) -print 2>/dev/null' \
  || true)
[ -z "$found" ] || { echo "ABORT: env files in the image:"; echo "$found"; exit 1; }
echo "ok no .env in the image"

leaked=$(docker run --rm --platform linux/amd64 --entrypoint sh \
  "$REGISTRY/$REPOSITORY:$TAG" -c \
  'find /app \( -name "*.pyc" -o -name "__pycache__" -o -name "*.pem" -o -name "*.key" \
     -o -path "/app/app/sandbox" \) -print 2>/dev/null' || true)
[ -z "$leaked" ] || { echo "ABORT: local state in the image:"; echo "$leaked"; exit 1; }
echo "ok no local state in the image"

if docker history --no-trunc --format '{{.CreatedBy}}' "$REGISTRY/$REPOSITORY:$TAG" \
   | grep -Ei 'sk-proj-|sk-lf-|pk-lf-|OPENAI_API_KEY|LANGFUSE_SECRET'; then
  echo "ABORT: credential-shaped string in image history"; exit 1
fi
echo "ok no credential-shaped strings in image history"
```

```bash
# 3. Only now publish. The tags already point at the asserted image, so this
# pushes exactly what step 2 inspected rather than rebuilding it.
aws ecr get-login-password --region ap-southeast-1 \
  | docker login --username AWS --password-stdin "$REGISTRY"

docker push "$REGISTRY/$REPOSITORY:$TAG"
docker push "$REGISTRY/$REPOSITORY:prod"
```

If you would rather not carry three steps by hand, that is the argument for using the
CI path, which does all of this and the container smoke test as well.

Verify the image is there and is the right architecture:

```bash
aws ecr describe-images --region ap-southeast-1 \
  --repository-name posthouse-demo-langfuse-loops \
  --image-ids imageTag=prod \
  --query 'imageDetails[0].[imageDigest,imagePushedAt,imageSizeInBytes]' --output text

docker manifest inspect "$REGISTRY/$REPOSITORY:prod" \
  | grep -E '"architecture"|"os"'
```

Expect `"architecture": "amd64"` and `"os": "linux"`. If you see `arm64`, rebuild with
`--platform linux/amd64`; App Runner will otherwise fail the deployment with a message
that does not mention architecture at all.

## 3. Create the IAM access role and the autoscaling ceiling, then the service

### 3a. The access role

App Runner pulls from ECR as itself, not as you, so it needs a role it can assume.
**Do not assume one exists.** The labs services use one, but reusing another service's
role couples two unrelated deploys, and if it is a console-generated
`AppRunnerECRAccessRole` you cannot tell from the name what depends on it. Check, then
create a dedicated one:

```bash
aws iam list-roles --query "Roles[?contains(RoleName,'AppRunner')].RoleName" --output text
```

Create it:

```bash
cat > /tmp/apprunner-trust.json <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "build.apprunner.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
JSON

aws iam create-role \
  --role-name AppRunnerECRAccessRole-langfuse-loops \
  --assume-role-policy-document file:///tmp/apprunner-trust.json \
  --description "Lets App Runner pull the langfuse-control-loops image from ECR"

aws iam attach-role-policy \
  --role-name AppRunnerECRAccessRole-langfuse-loops \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSAppRunnerServicePolicyForECRAccess

export ACCESS_ROLE_ARN=$(aws iam get-role \
  --role-name AppRunnerECRAccessRole-langfuse-loops \
  --query 'Role.Arn' --output text)
echo "$ACCESS_ROLE_ARN"
```

The trust principal is `build.apprunner.amazonaws.com`, not `tasks.apprunner.amazonaws.com`.
That is the single most common mistake here: the tasks principal is for the *instance*
role, which this service does not need at all, and getting them backwards produces a
create-service failure that reads like a permissions problem on ECR.

Verify:

```bash
aws iam list-attached-role-policies \
  --role-name AppRunnerECRAccessRole-langfuse-loops \
  --query 'AttachedPolicies[].PolicyName' --output text
aws iam get-role --role-name AppRunnerECRAccessRole-langfuse-loops \
  --query 'Role.AssumeRolePolicyDocument.Statement[0].Principal.Service' --output text
```

Expect `AWSAppRunnerServicePolicyForECRAccess` and `build.apprunner.amazonaws.com`.

### 3b. The autoscaling ceiling, before the service

`apprunner-service.json` sets `AutoScalingConfigurationArn` to a configuration named
`langfuse-loops-capped`, so that configuration has to exist first or `create-service`
fails. Create it now:

```bash
aws apprunner create-auto-scaling-configuration \
  --region ap-southeast-1 \
  --auto-scaling-configuration-name langfuse-loops-capped \
  --min-size 1 \
  --max-size 1 \
  --max-concurrency 25 \
  --tags Key=Project,Value=demohouse Key=Component,Value=langfuse-control-loops-demo
```

Verify, and note the ARN it returns has a `/1` revision suffix that the service config
deliberately omits:

```bash
aws apprunner list-auto-scaling-configurations --region ap-southeast-1 \
  --auto-scaling-configuration-name langfuse-loops-capped \
  --query 'AutoScalingConfigurationSummaryList[].[AutoScalingConfigurationArn,AutoScalingConfigurationRevision]' \
  --output table
```

#### Why MaxSize is 1 and not 2

Loop 3 signs its proposal with `PROPOSAL_SIGNING_SECRET`, and when that variable is
unset each process generates its own random secret at boot. `apprunner-service.json`
deliberately carries no `RuntimeEnvironmentVariables`, because needing no server-side
secrets is one of the real wins of the bring-your-own-key design. Those two facts
together mean that with more than one instance, a propose served by instance A and an
apply routed to instance B fails with "This proposal was signed by a different copy of
this demo" -- roughly half the time once `MaxConcurrency` 25 brings the second instance
up. Loop 3's approval gate is the centrepiece of the talk, so that is the worst possible
thing to have fail intermittently in front of an audience.

`MaxSize` 1 makes the correctness property STRUCTURAL rather than dependent on remembering
an environment variable: with one instance, propose and apply always meet. The cost is
that a flood degrades the demo sooner, which is the same trade the ceiling below already
makes deliberately.

If you ever raise `MaxSize`, you MUST set `PROPOSAL_SIGNING_SECRET` to the same value on
every instance, or loop 3 breaks. The code already warns at boot when the variable is
absent and the runtime error names the cause, so the failure is self-diagnosing rather
than mysterious. It is still an outage in front of people.

#### Why this is not optional, and which numbers to trust

Skipping this step does not leave autoscaling unset. App Runner attaches its default
configuration, whose `MaxSize` is **25**. That is the actual defect this step closes:
the service is public and unauthenticated, the app has no rate limit and no concurrency
cap of its own, and a review demonstrated one unauthenticated caller saturating an
instance in about two minutes. Bring-your-own-key means that caller cannot spend our
OpenAI or Langfuse quota, and that half was verified by planting fake server-side
credentials and confirming no code path reaches them. It says nothing at all about our
App Runner compute and data-transfer bill, and a default ceiling of 25 is what turns a
flood into a line item.

Be honest about what the ceiling does and does not buy. It bounds cost. It does **not**
stop the denial of service: capping at 2 instances means a flood degrades the demo
sooner than it would at 25. That is the right trade for a conference demo, where an
unbounded bill is worse than a demo that is briefly slow, but it is a trade, not a fix.
The fix for abuse is the rate limit in step 5.

Confidence, separated on purpose, because a previous pass asserted AWS defaults it had
not checked:

| Claim | Confidence | Basis |
|---|---|---|
| `CreateService` accepts only `AutoScalingConfigurationArn`, with no inline min/max | high | `CreateServiceRequest` members in the bundled botocore model, aws-cli 2.27.38 |
| Default is `MaxSize` 25, `MinSize` 1, `MaxConcurrency` 100 | high | same model's documented defaults |
| Valid ranges: `MaxConcurrency` 1-200, `MinSize` 1-25, `MaxSize` minimum 1 | high | shape constraints in the same model. The model declares no `MaxSize` maximum, so a per-account quota may bite instead |
| ARN without a revision suffix tracks the latest revision | high | documented on the `AutoScalingConfigurationArn` member |
| App Runner temporarily doubles provisioned instances during a deployment | high | documented on `MinSize`. With `MinSize` 1 that is 2 during a deploy |
| `MaxSize` 2 is the right ceiling | judgement, medium | Mine, not AWS's. Two 1 vCPU / 2 GB instances is the smallest ceiling that still survives one instance recycling. Raise it if the demo ever has a real audience, and only then |
| `MaxConcurrency` 25 is the right trigger | judgement, low | This is a scale-up **trigger**, not a throttle: App Runner does not reject request 26, it adds an instance. Lowering it makes scaling more eager, so it reaches the `MaxSize` 2 ceiling sooner and has headroom before collapse; the default 100 keeps everything on one instance right up to the point the review showed it falling over. Worth revisiting with a real load test rather than reasoning |
| Cost per instance-hour | unchecked | Deliberately not stated here. Look it up before quoting a number to anyone |

### 3c. The service

`demos/lf_control_loops/apprunner-service.json` carries a `_comment` key for humans, and
the AWS CLI rejects unknown members in `--cli-input-json`. Strip it and inject the role
ARN in one step:

```bash
cd demos/lf_control_loops

jq 'del(._comment)
    | .SourceConfiguration.AuthenticationConfiguration.AccessRoleArn = $arn' \
   --arg arn "$ACCESS_ROLE_ARN" \
   apprunner-service.json > /tmp/apprunner-input.json

# Read it before you run it. This is the only place the port, the image tag, the
# health check path and the autoscaling ceiling are asserted together.
jq '{ServiceName, image: .SourceConfiguration.ImageRepository.ImageIdentifier,
      port: .SourceConfiguration.ImageRepository.ImageConfiguration.Port,
      auto: .SourceConfiguration.AutoDeploymentsEnabled,
      health: .HealthCheckConfiguration.Path,
      cpu: .InstanceConfiguration.Cpu, mem: .InstanceConfiguration.Memory,
      scaling: .AutoScalingConfigurationArn}' \
   /tmp/apprunner-input.json

# scaling must be non-null. A null here means the service will be created against
# App Runner's default configuration and scale to 25 instances under a flood.
test "$(jq -r '.AutoScalingConfigurationArn // "null"' /tmp/apprunner-input.json)" != "null" \
  || { echo "ABORT: no AutoScalingConfigurationArn, see step 3b"; }

aws apprunner create-service --region ap-southeast-1 \
  --cli-input-json file:///tmp/apprunner-input.json
```

Capture the ARN and wait for it to settle:

```bash
export SERVICE_ARN=$(aws apprunner list-services --region ap-southeast-1 \
  --query "ServiceSummaryList[?ServiceName=='langfuse-control-loops'].ServiceArn" \
  --output text)

while :; do
  s=$(aws apprunner describe-service --region ap-southeast-1 \
        --service-arn "$SERVICE_ARN" --query 'Service.Status' --output text)
  echo "$s"; [ "$s" = "RUNNING" ] && break
  [ "$s" = "CREATE_FAILED" ] && { echo "failed"; break; }
  sleep 15
done
```

`RUNNING` means App Runner's own health check on `/api/healthz` passed, which already
proves the container binds `0.0.0.0:8000`. `CREATE_FAILED`, or an outright API error
before the service is even created, is almost always one of: an arm64 image (step 2), a
missing `langfuse-loops-capped` autoscaling configuration (step 3b, and this one fails
as a validation error rather than `CREATE_FAILED`), the wrong trust principal (step 3a),
or a port mismatch between the Dockerfile and `Port`. Read the logs before changing
anything:

```bash
aws logs tail /aws/apprunner/langfuse-control-loops --region ap-southeast-1 --since 30m
```

Record the ARN and URL as repo variables so CI can deploy:

```bash
aws apprunner describe-service --region ap-southeast-1 --service-arn "$SERVICE_ARN" \
  --query 'Service.[ServiceArn,ServiceUrl]' --output text
```

Set `LF_LOOPS_SERVICE_ARN` to the ARN and `LF_LOOPS_URL` to `https://<ServiceUrl>` in
GitHub repo variables, along with `LF_LOOPS_AWS_ROLE_ARN` for the OIDC role. Until the
custom domain exists, `LF_LOOPS_URL` should be the `*.awsapprunner.com` URL.

## 4. Verify on the generated URL before attaching any domain

Do this before DNS. It separates "the app works" from "the routing works", and DNS and
certificate propagation make the two impossible to tell apart later.

```bash
export APP_URL="https://$(aws apprunner describe-service --region ap-southeast-1 \
  --service-arn "$SERVICE_ARN" --query 'Service.ServiceUrl' --output text)"

# Exact statuses, no --fail, no --location. --fail passes on a 302, and following
# redirects hides exactly the problem you are looking for.
for p in /api/healthz /api/status /; do
  printf '%s -> %s\n' "$p" \
    "$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 "$APP_URL$p")"
done
```

Expect `200` for all three. Then check the payload, because a 200 from an error page
looks identical to a 200 from the app:

```bash
curl -s --max-time 20 "$APP_URL/api/status" | jq .
```

Expect `byok: true`, an `allowed_langfuse_hosts` array, a `default_model` and
`model_choices`. It must **not** contain anything reporting whether a server-side key
exists; there are none, and reporting on them would be a lie that invites someone to add
one.

Last, confirm the bring-your-own-key contract holds in production, not just in CI:

```bash
curl -s -o /dev/null -w '%{http_code}\n' --max-time 20 \
  -X POST -H 'Content-Type: application/json' \
  -d '{"ticket":"deploy check"}' "$APP_URL/api/workflow/run"
```

Expect `400`. A `200` here would mean the server found credentials somewhere, which is
the one outcome that makes the whole public-with-no-login design unsafe. Stop and
investigate before going further.

Then open `$APP_URL` in a browser, paste your own keys into the Setup tab, and run one
loop end to end. Automation cannot tell you the UI is usable.

Last, confirm the autoscaling ceiling actually attached. `create-service` succeeding is
not proof: if the ARN had been dropped from the input, the service would be `RUNNING` and
happily scale to 25.

```bash
asc=$(aws apprunner describe-service --region ap-southeast-1 \
  --service-arn "$SERVICE_ARN" \
  --query 'Service.AutoScalingConfigurationSummary.AutoScalingConfigurationArn' --output text)
echo "$asc"

aws apprunner describe-auto-scaling-configuration --region ap-southeast-1 \
  --auto-scaling-configuration-arn "$asc" \
  --query 'AutoScalingConfiguration.[AutoScalingConfigurationName,MinSize,MaxSize,MaxConcurrency]' \
  --output text
```

Expect `langfuse-loops-capped 1 2 25`. A `MaxSize` of `25` means you are on App Runner's
default configuration and step 3b did not take effect.

## 5. Attach langfuse.demohouse.cloud and dev-langfuse.demohouse.cloud

### Verified facts, do not re-research these

- `langfuse.demohouse.cloud` does **not** exist in Route53 today. Nothing to move, no
  cutover, no TTL to wait out. This is a greenfield record.
- The hosted zone for `demohouse.cloud` is **`Z05349683FG0M5SRMBNK1`**.
- A wildcard ACM certificate covering `demohouse.cloud` and `*.demohouse.cloud` already
  exists and is attached to the prod ALB listener.

### Both hosts are wanted, and the dev one is not optional

Two hostnames, now confirmed:

| Host | Purpose |
|---|---|
| `langfuse.demohouse.cloud` | what you put on a slide |
| `dev-langfuse.demohouse.cloud` | pre-demo checks, and the URL you hand a colleague to try |

The `dev-` prefix is the established pattern in this account, not a new convention:
`labs.demohouse.cloud` and `dev-labs.demohouse.cloud` are already wired that way, so
`dev-langfuse` is what the next person will guess. Do not invent
`langfuse-dev.demohouse.cloud` or a `staging.` prefix.

One decision to make explicitly, because it is cheap now and annoying later: whether
`dev-langfuse` points at the same App Runner service as `langfuse`, or at a second
service. Same service is the recommendation. There are no server-side credentials to
differ between environments, no database, and no per-environment configuration at all,
so a second service would cost a second bill and a second deploy path to serve byte
identical content. `associate-custom-domain` can be called twice against one service, so
both hosts land on the same task. If you later need `dev-langfuse` to run an unreleased
image, create `langfuse-control-loops-dev` then, and give it its own autoscaling
configuration rather than sharing `langfuse-loops-capped`.

### What follows from the wildcard fact

The wildcard does not help here. App Runner custom domains issue and validate their
**own** certificate through DNS validation records that App Runner gives you; there is no
way to hand App Runner an existing ACM certificate. So "we already have a wildcard" saves
zero steps on the direct path, and it saves zero steps twice, once per host. It does still
help on the CloudFront path, where the wildcard is a usable viewer certificate for both
names.

Confirm the zone and the absence of both records before you start:

```bash
aws route53 list-resource-record-sets --hosted-zone-id Z05349683FG0M5SRMBNK1 \
  --query "ResourceRecordSets[?contains(Name,'langfuse')]" --output json
```

Expect `[]`. Anything non-empty means someone got here first; stop and find out who.

### Option A: App Runner custom domain directly

Do the prod host first, get it to `ACTIVE`, then repeat the identical sequence for
`dev-langfuse.demohouse.cloud`. Both associations can live on the same service. Doing
them one at a time rather than in parallel matters: each association returns its own
certificate validation records, and two pending sets of `_<hash>` CNAMEs in flight at
once is how you end up creating one host's validation record under the other host's name
and waiting out a timeout for no reason.

```bash
aws apprunner associate-custom-domain --region ap-southeast-1 \
  --service-arn "$SERVICE_ARN" \
  --domain-name langfuse.demohouse.cloud \
  --no-enable-www-subdomain

# ...and, after the first is ACTIVE, the dev host on the same service:
aws apprunner associate-custom-domain --region ap-southeast-1 \
  --service-arn "$SERVICE_ARN" \
  --domain-name dev-langfuse.demohouse.cloud \
  --no-enable-www-subdomain

# App Runner returns the records to create: one CNAME for the domain itself plus
# certificate validation records. Read them, then create them in Route53.
aws apprunner describe-custom-domains --region ap-southeast-1 \
  --service-arn "$SERVICE_ARN" \
  --query 'CustomDomains[].[DomainName,Status,CertificateValidationRecords]' --output json
```

DNS records required, per host:

| Type | Name | Value | Why |
|---|---|---|---|
| CNAME | `langfuse.demohouse.cloud` | the `*.awsapprunner.com` service URL | routes traffic |
| CNAME | the `_<hash>.langfuse.demohouse.cloud` names App Runner returns | the values it returns | ACM DNS validation, usually two or three records |
| CNAME | `dev-langfuse.demohouse.cloud` | the same `*.awsapprunner.com` service URL | same service, second name |
| CNAME | the `_<hash>.dev-langfuse.demohouse.cloud` names App Runner returns | the values it returns | a separate validation set, do not reuse the prod host's |

Create them from the API output rather than by hand. Select by `DomainName` rather than
by index: with two associations `CustomDomains[0]` is whichever one the API felt like
returning first, and copying the wrong host's validation records is the failure this
avoids.

```bash
aws apprunner describe-custom-domains --region ap-southeast-1 \
  --service-arn "$SERVICE_ARN" > /tmp/cd.json

# DNSTarget is top level and shared: both hosts CNAME to the same service target.
jq -r '.DNSTarget' /tmp/cd.json

for h in langfuse.demohouse.cloud dev-langfuse.demohouse.cloud; do
  echo "--- $h"
  jq -r --arg h "$h" \
    '.CustomDomains[] | select(.DomainName==$h)
     | .CertificateValidationRecords[]? | [.Name,.Value] | @tsv' /tmp/cd.json
done
```

Then a single change batch with the service CNAME plus each validation record, and wait:

```bash
while :; do
  aws apprunner describe-custom-domains --region ap-southeast-1 \
    --service-arn "$SERVICE_ARN" \
    --query 'CustomDomains[].[DomainName,Status]' --output text
  sleep 30
done   # ACTIVE when done; PENDING_CERTIFICATE_DNS_VALIDATION until the records resolve
```

Verification, both hosts:

```bash
for h in langfuse.demohouse.cloud dev-langfuse.demohouse.cloud; do
  printf '%s CNAME -> %s\n' "$h" "$(dig +short "$h" CNAME)"
  printf '%s /api/healthz -> %s\n' "$h" \
    "$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 "https://$h/api/healthz")"
done
```

Expect `200` for both. Anything in the 3xx range means something is in front of App Runner
that should not be.

Trade-offs: two moving parts instead of four, one certificate lifecycle managed entirely
by App Runner, and no cache to invalidate when the demo changes. Against it: every
request pays the round trip to `ap-southeast-1`, there is no edge caching seam, and you
diverge from how the neighbouring site is wired. Note that "no WAF seam" is **not** on
that list, contrary to what an earlier version of this document said: AWS WAF associates
directly with an App Runner service. See the abuse section below.

### Option B: CloudFront in front, matching the labs precedent

Labs is already built this way: `labs.demohouse.cloud` -> CloudFront distribution
`E32KAQBKR0HHHL` -> the App Runner service URL as a custom origin. A parallel
distribution for `langfuse.demohouse.cloud` would use the existing wildcard ACM
certificate as its viewer certificate (in `us-east-1`, which is where CloudFront requires
it, so check the wildcard exists there and not only in `ap-southeast-1`), an
`Aliases` entry of `langfuse.demohouse.cloud`, the App Runner URL as an
`https-only` custom origin, and an alias A record in `Z05349683FG0M5SRMBNK1` pointing at
the distribution.

Caching needs care for this app. `/api/*` must be no-cache with all five credential
headers forwarded, or CloudFront will strip them and every request will look
credential-less and return 400. The static bundle can cache freely.

Trade-offs: consistent with labs, faster static delivery for a global audience, and a
place to terminate TLS with the wildcard you already have. Against it: a second service to
reason about, a cache invalidation step on every UI change, an origin request policy that
must forward `X-OpenAI-Key` and friends or the app silently breaks, and a `us-east-1`
certificate dependency for a service that otherwise lives in one region. "Somewhere to
attach WAF" used to be listed as a benefit here and has been removed, because it is not
one: WAF attaches to App Runner directly too.

### Recommendation: Option A, the App Runner custom domain, directly

For this demo the direct path is the right call:

1. The failure mode of Option B is subtle and expensive. If the origin request policy does
   not forward the credential headers, the app answers 400 for everyone while looking
   perfectly healthy from every check in this runbook. That is a bad trade for a demo.
2. There is nothing to cache that matters. One small HTML/CSS/JS bundle, and every
   interesting request is a POST to an LLM where a network round trip is noise next to
   multi-second model latency.
3. Rate limiting does not require CloudFront. This is the one reason that used to point
   at Option B and no longer does; see below.
4. Fewer parts means a faster teardown when the demo's moment passes, which for a
   conference demo is a real consideration.

Match the labs pattern later if this graduates into something durable or picks up a
non-APAC audience. Adding CloudFront later is straightforward: point the distribution at
the same origin and swap the Route53 record. Nothing in Option A blocks it.

## 5a. Abuse and cost, corrected

An earlier version of this document ruled out any edge control with this argument:
*there is nothing to protect at the edge, no secrets, no login, no spend of ours, and the
usual reason to want a WAF in front of an LLM endpoint is abuse of our credentials, of
which there are none.* Half of that is right and half of it is wrong, and the wrong half
was load-bearing.

**The credential half holds, and was verified.** There is no code path that reaches
OpenAI or Langfuse with anything but the caller's own keys. A review planted fake
server-side environment credentials and confirmed nothing picked them up; a loop endpoint
called without headers answers 400 rather than falling back. Nobody can burn our model
quota through this service, and step 4 asserts that property in production on every
deploy. Keep it.

**The cost and availability half does not hold.** Bring-your-own-key says nothing about
the resources *we* pay for. A review took the instance down in about two minutes with an
unauthenticated request flood and generated free outbound data transfer while doing it.
There is no rate limit, there is no concurrency cap, and until step 3b there was no
autoscaling ceiling either, so the flood's only bound was App Runner's default `MaxSize`
of 25. "The visitor brings their own key" was never an answer to "who pays for the 25
instances".

Two controls, in order of what to do first.

**Already done, step 3b: the autoscaling ceiling.** `MaxSize` 2 bounds the bill. It does
not stop the abuse and it makes the service easier to saturate, which is the correct trade
for a demo but should be understood as a cost control, not a security control.

**Recommended, and cheap: an AWS WAF rate-based rule attached to the App Runner service
directly.** No CloudFront needed.

```bash
# REGIONAL scope, in ap-southeast-1 alongside the service. Not CLOUDFRONT scope,
# which lives in us-east-1 and is for distributions.
aws wafv2 create-web-acl --region ap-southeast-1 --scope REGIONAL \
  --name langfuse-loops-ratelimit \
  --default-action Allow={} \
  --visibility-config \
     SampledRequestsEnabled=true,CloudWatchMetricsEnabled=true,MetricName=langfuseLoops \
  --rules file:///tmp/waf-rules.json

# ARN form for the association is
# arn:aws:apprunner:ap-southeast-1:959934561610:service/<name>/<id>, which is the
# SERVICE_ARN you already have.
aws wafv2 associate-web-acl --region ap-southeast-1 \
  --web-acl-arn "$WEB_ACL_ARN" \
  --resource-arn "$SERVICE_ARN"
```

The rule to put in `/tmp/waf-rules.json` is a `RateBasedStatement` aggregating on IP,
scoped down to the expensive routes only:

```json
[{
  "Name": "limit-loop-posts-per-ip",
  "Priority": 0,
  "Statement": {
    "RateBasedStatement": {
      "Limit": 1000,
      "EvaluationWindowSec": 300,
      "AggregateKeyType": "IP",
      "ScopeDownStatement": {
        "AndStatement": { "Statements": [
          { "ByteMatchStatement": {
              "SearchString": "/api/",
              "FieldToMatch": { "UriPath": {} },
              "TextTransformations": [{ "Priority": 0, "Type": "NONE" }],
              "PositionalConstraint": "STARTS_WITH" } },
          { "ByteMatchStatement": {
              "SearchString": "POST",
              "FieldToMatch": { "Method": {} },
              "TextTransformations": [{ "Priority": 0, "Type": "NONE" }],
              "PositionalConstraint": "EXACTLY" } }
        ]}
      }
    }
  },
  "Action": { "Count": {} },
  "VisibilityConfig": {
    "SampledRequestsEnabled": true,
    "CloudWatchMetricsEnabled": true,
    "MetricName": "limitLoopPostsPerIp"
  }
}]
```

Three deliberate choices in there, and a trap:

- **Scoped down to `POST /api/*`.** Under Option A the same service serves the static
  bundle, so an unscoped IP rule counts `index.html`, `app.js` and `styles.css` against
  every visitor. Only the loop POSTs cost anything, so only they should be counted.
- **`Action: Count`, not `Block`, on first deploy.** Ship it in count mode, watch the
  CloudWatch metric through one real demo, then switch to `Block`. A rate rule that
  blocks the audience mid-talk is a worse outcome than the flood it was guarding against.
- **The conference NAT trap.** `AggregateKeyType: IP` at a conference means the entire
  room shares one aggregation key. This is the single most likely way for this control to
  break the demo it protects. The scope-down to POSTs is most of the mitigation; the
  `Limit` is the rest, and 1000 per 5 minutes is a guess sized for "a few dozen people
  behind one NAT doing tens of runs each", not a measured number.

Confidence, again separated:

| Claim | Confidence | Basis |
|---|---|---|
| WAF associates directly with an App Runner service, no CloudFront required | high | `APP_RUNNER_SERVICE` is in both the `ResourceType` and `AssociatedResourceType` enums of the bundled wafv2 model, and `AssociateWebACL` documents the `arn:partition:apprunner:region:account-id:service/name/id` ARN form |
| The web ACL must be `REGIONAL` scope in `ap-southeast-1` | high | `CLOUDFRONT` scope web ACLs are `us-east-1` only and apply to distributions |
| `EvaluationWindowSec` accepts only 60, 120, 300, 600, default 300 | high | documented on the member in the same model |
| `Limit` range is 10 to 2,000,000,000 | high | shape constraints in the same model |
| `Limit` 1000 per 5 minutes is the right number | judgement, low | Mine, and unmeasured. Run in `Count` mode and set it from the metric |
| WAF monthly cost | unchecked | roughly a few dollars per web ACL plus per-rule plus per-million-requests, but do not quote these from here. Check the pricing page |
| Whether a web ACL already exists in this account to reuse | unchecked | needs an `aws wafv2 list-web-acls --scope REGIONAL` against the account. Do not assume either way |

What this does **not** need: a per-visitor quota in the app, a login, or a server-side key.
The point of the rate limit is to protect our compute bill, and it is the only thing here
that does. Adding application-level auth to solve it would give up the property that makes
this demo simple.

## 6. Rollback

Deployments are pinned by digest, so rollback is redeploying the previous digest. It is
not a rebuild and it does not need CI.

List what has been deployed, newest first:

```bash
aws ecr describe-images --region ap-southeast-1 \
  --repository-name posthouse-demo-langfuse-loops \
  --query 'reverse(sort_by(imageDetails,&imagePushedAt))[:5].[imagePushedAt,imageDigest,imageTags]' \
  --output table
```

Confirm what is serving right now, so you know what you are rolling back from:

```bash
aws apprunner describe-service --region ap-southeast-1 --service-arn "$SERVICE_ARN" \
  --query 'Service.SourceConfiguration.ImageRepository.ImageIdentifier' --output text
```

Roll back to a known-good digest:

```bash
export GOOD_DIGEST=sha256:....

aws apprunner update-service --region ap-southeast-1 \
  --service-arn "$SERVICE_ARN" \
  --source-configuration "$(jq -n --arg image \
    "959934561610.dkr.ecr.ap-southeast-1.amazonaws.com/posthouse-demo-langfuse-loops@$GOOD_DIGEST" \
    '{ImageRepository:{ImageIdentifier:$image,ImageRepositoryType:"ECR",
       ImageConfiguration:{Port:"8000"}}}')"
```

Then wait for `RUNNING` and re-run the step 4 verifications against the live URL. The
rollback is not finished until `/api/healthz` returns 200 and `/api/workflow/run` without
credential headers returns 400.

If a deployment is stuck rather than bad:

```bash
aws apprunner list-operations --region ap-southeast-1 --service-arn "$SERVICE_ARN" --max-results 5
aws logs tail /aws/apprunner/langfuse-control-loops --region ap-southeast-1 --since 15m
```

Emergency stop, which keeps the service and its custom domain but stops serving:

```bash
aws apprunner pause-service  --region ap-southeast-1 --service-arn "$SERVICE_ARN"
aws apprunner resume-service --region ap-southeast-1 --service-arn "$SERVICE_ARN"
```

Prefer pause over delete. Deleting the service releases the `*.awsapprunner.com` hostname
and forces the whole custom domain and certificate validation dance again, now twice over
because there are two custom domains on it. And before you delete anything named like a
workshop, re-read the collision warning at the top of this document.

`pause-service` is also the right lever if the service is being flooded rather than
deployed badly. It is blunt and it takes the demo down, but with `MaxSize` 2 a flood
degrades the service anyway, so pausing costs availability you had already lost and stops
the bill immediately. If you find yourself reaching for it, that is the signal to move the
step 5a rate rule from `Count` to `Block`.
