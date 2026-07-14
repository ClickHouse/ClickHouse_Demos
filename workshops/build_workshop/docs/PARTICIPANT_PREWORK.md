# ClickHouse BUILD Workshop — Participant Prework

Welcome. This is a hands-on, build-along workshop: you will use an AI coding agent to
build an Agentic SRE setup and a GenBI app on ClickHouse Cloud, live, in 3 hours. That
only works if your laptop arrives ready. Everything below is MANDATORY unless marked
optional, takes about 45-60 minutes total, and must be done BEFORE the day.

If anything fails and you cannot fix it: arrive 30 minutes early — there is a staffed
setup clinic before doors.

Platform note: macOS and Linux are supported natively. Windows works via WSL2 only
(install Docker with the WSL2 backend and run all commands inside WSL2).

---

## Quick checklist

| # | Item | Done when |
|---|---|---|
| 1 | Laptop: 16 GB+ RAM, 10 GB free disk | — |
| 2 | Docker installed, 6 GB+ allocated | `docker run hello-world` succeeds |
| 3 | Workshop repo cloned, `prework.sh` run | Script prints PASS |
| 4 | ClickHouse Cloud account | You can see the $300 trial credits in the console |
| 5 | clickhousectl installed | `clickhousectl --version` prints a version |
| 6 | Coding agent working, MCP-capable | The 60-second MCP smoke test passes |
| 7 | LLM API key (Anthropic or OpenAI) | Key created AND credits loaded |
| 8 | Node.js 20+ (or Python 3.11+ if told Streamlit) | `node --version` |
| 9 | Telegram bot created | Your bot replied to your test curl |
| 10 | Email you can access during the session | — |

---

## Step 1 — Laptop and Docker

- 16 GB+ total RAM (the demo stack uses 3-4 GB inside Docker, plus your IDE, coding
  agent, browser, and a dev server), 10 GB free disk.
- Install Docker Desktop (https://docker.com), OR — if your company requires a paid
  Docker Desktop license (250+ employees) — OrbStack, Colima, or Rancher Desktop all
  work with the standard compose file.
- Allocate at least 6 GB RAM to Docker: Docker Desktop -> Settings -> Resources ->
  Memory -> 6 GB+ -> Apply and restart. (OrbStack and Colima size dynamically.)
- Windows: enable WSL2, install Docker Desktop with the WSL2 backend, and do everything
  else in this guide inside your WSL2 shell.

Check: `docker run hello-world`

## Step 2 — Clone the workshop repo and run the verifier

```bash
git clone <WORKSHOP_REPO_URL>          # sent in the same email as this guide
cd <WORKSHOP_REPO_DIR>
./prework.sh
```

`prework.sh` pre-pulls all Docker images (~1.8 GB — do this at home, not on venue
wifi), installs the GenBI app dependencies, and verifies Docker, RAM allocation, port
8080 availability, and tool versions. It must end with PASS. If it prints FAIL, follow
its hints; if stuck, arrive early for the clinic.

Keep this directory: the whole workshop runs from it, and some tools (clickhousectl)
store login state relative to the directory you run them from.

## Step 3 — ClickHouse Cloud account

You get your own ClickHouse Cloud trial: $300 in credits for 30 days, no credit card.
The database service itself is created together during the workshop — for now, only the
account.

1. Go to https://console.clickhouse.cloud/signUp
2. Sign up with email (or Google/Microsoft SSO).
   - Use a PERSONAL email if your work email might already be attached to an existing
     company ClickHouse organization — existing-org emails may not get a fresh trial.
3. Verify your email via the link (within 24 hours of signing up).
4. Log in. If an onboarding wizard pushes you to create a service now, you can stop at
   the account stage — we create services together in the session. (If you do end up
   creating one, that is fine too; just note the password it shows you.)
5. CHECKPOINT: confirm you can see your trial credits — click your organization name in
   the console (lower left), open **Billing**, and look for the Credits section showing
   the $300 trial. If you cannot see trial credits, contact the instructor BEFORE the
   workshop — loaner services exist but are limited.

## Step 4 — Install clickhousectl (the ClickHouse CLI)

```bash
curl https://clickhouse.com/cli | sh
```

The installer puts the binary at `~/.local/bin/clickhousectl` (alias `chctl`) but does
NOT edit your shell profile. On macOS (and most Linux) add it to PATH:

```bash
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc   # or ~/.bashrc
exec $SHELL
clickhousectl --version
```

Two things to know for the workshop:
- You will log in day-of with `clickhousectl cloud auth login` (browser flow). Run all
  `clickhousectl cloud ...` commands FROM THE WORKSHOP REPO DIRECTORY — the CLI stores
  its login tokens in a `.clickhouse/` folder inside the directory where you logged in
  (a known beta quirk). Log in once, stay in that directory.
- The CLI's browser login is read-only by design: you can list and query, not create or
  delete. That is all the workshop needs from it.

## Step 5 — Your coding agent, verified MCP-capable

Bring ONE of: Claude Code, Cursor, Codex CLI, or Windsurf — installed, signed in, and
on an ACTIVE plan or credits. A 3-hour agentic session is a heavy session; free tiers
will stall:

- Claude Code: Claude Pro/Max subscription, or Console (API) billing.
- Cursor: a paid plan recommended (agent usage consumes quota).
- Codex CLI: ChatGPT Plus/Pro/Business/Enterprise, or an OpenAI API key. Update to the
  current build (`npm i -g @openai/codex` or `brew upgrade codex`) — MCP OAuth support
  is recent.
- Windsurf: signed in; if your company manages your account, confirm MCP servers are
  not admin-blocked.

The 60-second MCP smoke test (do this now — it surfaces corporate lockdowns a week
early, not on the day): add any public MCP server to your tool and call one tool. Two
public no-auth options:

- ClickHouse SQL playground MCP-style test is not available; instead use for example
  Context7 (`https://mcp.context7.com/mcp`) or the DeepWiki MCP
  (`https://mcp.deepwiki.com/mcp`) — both are public, read-only, no-auth HTTP servers.

Example with Claude Code:

```bash
claude mcp add --transport http deepwiki https://mcp.deepwiki.com/mcp
claude
# inside: ask "use deepwiki to look up facebook/react" — a tool call must fire
```

Equivalent config for your tool is in the appendices below. If your tool cannot add an
MCP server (enterprise policy), talk to your admin or bring a personal machine.

If you have skills support (all four tools do), also pre-install the official
ClickHouse skills — your agent gets ClickHouse best-practices knowledge:

```bash
cd <WORKSHOP_REPO_DIR>
clickhousectl skills --agent claude    # or: cursor / codex / windsurf (or --all)
```

## Step 6 — LLM API key (for the app YOU build in Lab 3)

Your GenBI app calls an LLM at runtime to turn questions into SQL. Your coding agent's
subscription does NOT cover this — you need one API key. Expect well under $5 of usage.

Recommended: Anthropic
1. Sign in at https://platform.claude.com (Google or email).
2. **Settings > Billing** -> **Buy credits** -> add a payment method -> load $5
   (minimum). Without credits, the key will error even though it creates fine.
3. Optional but smart — a hard spend cap: **Settings > Workspaces** -> **Create
   workspace** (name it `build-workshop`) -> open it -> **Limits** tab -> set spend
   limit to $5. Caps cannot be set on the Default workspace.
4. Create the key INSIDE that workspace: workspace -> **API Keys** -> **Create Key** ->
   name `build-workshop-genbi` -> copy the `sk-ant-...` value now (shown once).
5. Model to use in the lab: `claude-haiku-4-5` (cheap and fast; thousands of SQL
   generations per $5).

Alternative: OpenAI
1. https://platform.openai.com -> Settings > Billing -> load $5.
2. Create a project, then https://platform.openai.com/api-keys -> **Create new secret
   key** scoped to the project -> copy the `sk-proj-...` key.
3. Caveat: OpenAI project budgets are soft alerts, not hard caps — set the monthly
   budget and notification threshold anyway.

Bring the key with you (password manager, not a git repo). Forgot or blocked? The
instructor has a small pool of capped loaner keys.

## Step 7 — Node.js 20+

The GenBI app skeleton is Next.js. Install Node 20+ (https://nodejs.org or your
package manager); check `node --version`. (If the organizer email said the Streamlit
track instead: Python 3.11+ and `python3 --version`.)

## Step 8 — Telegram bot (5 minutes, do it now)

In Lab 2 your SRE agent will page you on your phone. Create the bot in advance:

1. Install Telegram on your phone (and optionally desktop).
2. Open https://t.me/botfather (verified account with a blue check) -> **Start**.
3. Send `/newbot`.
4. Name (display name): anything, e.g. `My BUILD Alerts`.
5. Username: must be globally unique and END in `bot`, e.g. `yourname_build_bot`.
6. BotFather replies with a token like `110201543:AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsaw`.
   Save it like a password — anyone with it controls the bot.
7. CRITICAL: open your new bot's chat (`https://t.me/<your_bot_username>`) and press
   **Start**. Bots cannot message you until you message them first — skipping this is
   the number one failure.
8. Get your chat id:

```bash
curl -s "https://api.telegram.org/bot<TOKEN>/getUpdates"
# read result[..].message.chat.id — a number like 123456789
```

9. Test the full loop:

```bash
curl -s -X POST "https://api.telegram.org/bot<TOKEN>/sendMessage" \
  -H "Content-Type: application/json" \
  -d '{"chat_id": <YOUR_CHAT_ID>, "text": "BUILD workshop test: it works"}'
```

Your phone should buzz. Save TOKEN and CHAT_ID where you can paste them in the session.
If your company network blocks Telegram: no problem — a shared Slack channel is the
fallback, provided on the day.

## Step 9 — Day-of preview (nothing to do now)

So you know what is coming, the first 35 minutes of the workshop are:
1. Create your ClickHouse Cloud service (Observability use case) and copy its password
   THE MOMENT IT IS SHOWN — it is displayed once.
2. Paste three values into `app/.env` and run `docker compose up -d`.
3. Enable MCP on your service: service -> **Connect** -> **Connect with MCP** -> toggle.
4. `clickhousectl cloud auth login` (from the repo directory).
5. Connect your coding agent to your service's MCP endpoint (your appendix below) and
   authorize via browser OAuth.

---

# Appendix — per-tool MCP configuration reference

You will run these DURING the workshop (they need your live service). Read your tool's
section now; pre-stage what you can. Two endpoints matter:

- `https://mcp.clickhouse.cloud/mcp` — general ClickHouse Cloud MCP (query, schema,
  service info). Added in Lab 0.
- `https://mcp.clickhouse.cloud/clickstack` — observability MCP (logs, traces, metrics
  investigation tools). Added at the start of Lab 2.

Both use browser OAuth with your ClickHouse Cloud login. Your trial org has exactly one
service, so no extra scoping is needed. (Only if you are using a LOANER service from
the instructor: add the header `x-service-id: <service-uuid from your slip>` to the
clickstack entry — syntax shown per tool.)

## A. Claude Code

```bash
# Lab 0
claude mcp add --transport http clickhouse-cloud https://mcp.clickhouse.cloud/mcp
# Lab 2
claude mcp add --transport http clickstack https://mcp.clickhouse.cloud/clickstack
# loaner-service variant:
#   claude mcp add --transport http clickstack https://mcp.clickhouse.cloud/clickstack \
#     --header "x-service-id: YOUR_SERVICE_ID"
```

Authorize: start `claude`, run `/mcp`, select the server, complete the browser flow
(or from the shell: `claude mcp login clickhouse-cloud`).

Stop permission prompts during the investigation — add to `.claude/settings.json` in
the workshop repo (or `~/.claude/settings.json`):

```json
{
  "permissions": {
    "allow": [
      "mcp__clickhouse-cloud",
      "mcp__clickstack"
    ]
  }
}
```

Skills: the workshop repo ships the SRE investigator skill at
`.claude/skills/sre-investigator/` — nothing to do, Claude Code picks it up from the
repo automatically (accept the workspace trust prompt on first run).

## B. Cursor

Create or edit `.cursor/mcp.json` in the workshop repo (or `~/.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "clickhouse-cloud": { "url": "https://mcp.clickhouse.cloud/mcp" },
    "clickstack": {
      "url": "https://mcp.clickhouse.cloud/clickstack"
    }
  }
}
```

(Loaner service: add `"headers": { "x-service-id": "YOUR_SERVICE_ID" }` inside the
clickstack object.) Then Cursor Settings -> MCP: the servers appear with a login/needs-
auth action — click it and complete the browser OAuth. Alternative: search "ClickHouse"
in the Cursor MCP marketplace and click Add to Cursor.

Stop prompts — create `.cursor/permissions.json` in the repo:

```json
{
  "mcpAllowlist": [
    "clickhouse-cloud:*",
    "clickstack:*"
  ]
}
```

Skills: Cursor reads `.agents/skills/` and `.claude/skills/` in the repo — the workshop
skill is picked up automatically.

## C. Codex CLI

```bash
codex mcp add clickhouse-cloud --url https://mcp.clickhouse.cloud/mcp
codex mcp login clickhouse-cloud
# Lab 2:
codex mcp add clickstack --url https://mcp.clickhouse.cloud/clickstack
codex mcp login clickstack
```

Loaner service (header not supported on the add command) — edit `~/.codex/config.toml`:

```toml
[mcp_servers.clickstack]
url = "https://mcp.clickhouse.cloud/clickstack"
http_headers = { "x-service-id" = "YOUR_SERVICE_ID" }
```

Stop prompts — in `~/.codex/config.toml`, per server:

```toml
[mcp_servers.clickhouse-cloud]
url = "https://mcp.clickhouse.cloud/mcp"
default_tools_approval_mode = "auto"

[mcp_servers.clickstack]
url = "https://mcp.clickhouse.cloud/clickstack"
default_tools_approval_mode = "auto"
```

Skills: Codex reads `.agents/skills/` from the repo — automatic.

## D. Windsurf

Edit `~/.codeium/windsurf/mcp_config.json` (or via the MCP panel in Cascade):

```json
{
  "mcpServers": {
    "clickhouse-cloud": { "serverUrl": "https://mcp.clickhouse.cloud/mcp" },
    "clickstack": { "serverUrl": "https://mcp.clickhouse.cloud/clickstack" }
  }
}
```

(Loaner service: add `"headers": { "x-service-id": "YOUR_SERVICE_ID" }` to clickstack.)
Refresh the MCP panel and complete the OAuth prompt. If the native OAuth flow fails on
your build, use the fallback shim (requires Node, which you have from Step 7):

```json
{
  "mcpServers": {
    "clickhouse-cloud": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "https://mcp.clickhouse.cloud/mcp"]
    }
  }
}
```

Reduce prompts: set auto-execution to Allowlist (or Turbo for the session) in the
settings panel; instructors will help tune this in Lab 0.

Skills: Windsurf reads `.windsurf/skills/` and `.agents/skills/` from the repo —
automatic.

## E. No Docker? (corporate laptop path)

You can still do everything except run the demo app locally. During Lab 2 you connect
to the instructor's shared incident service instead, using the official local
`mcp-clickhouse` server (no OAuth, works everywhere Python does):

```bash
pip install mcp-clickhouse   # or: uv tool install mcp-clickhouse
```

Tool config (Claude Code example; instructor gives you HOST and PASSWORD on the day):

```json
{
  "mcpServers": {
    "clickhouse-shared": {
      "command": "uvx",
      "args": ["mcp-clickhouse"],
      "env": {
        "CLICKHOUSE_HOST": "<shared-service-host>",
        "CLICKHOUSE_PORT": "8443",
        "CLICKHOUSE_USER": "workshop_reader",
        "CLICKHOUSE_PASSWORD": "<given on the day>",
        "CLICKHOUSE_SECURE": "true"
      }
    }
  }
}
```

Tell the instructor you are on this path when you arrive so they can count you in.
