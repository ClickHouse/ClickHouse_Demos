# ClickHouse BUILD Workshop playbook

The published playbook for the ClickHouse BUILD Workshop ("Build AI with AI"): a
three-hour, hands-on session where participants use their own agentic coding tool to take
an NYC-taxi ride-hailing analytics app end to end on ClickHouse Cloud.

This directory is the documentation site only. The workshop app that participants clone
and build on lives alongside it at `workshops/build_workshop/app` in this repository, on
the `build-workshop-v1` branch. The site is built with [Next.js](https://nextjs.org)
and [Fumadocs](https://fumadocs.dev). Production is
[workshop.demohouse.cloud](https://workshop.demohouse.cloud); dev is
[dev-workshop.demohouse.cloud](https://dev-workshop.demohouse.cloud).

## Prerequisites

- Node.js `>= 22.12.0`. Earlier 22.x releases fail the build with
  `require() of ES Module ... not supported` because Fumadocs' MDX loader is ESM and
  require-of-ESM is only enabled by default from Node 22.12. An `.nvmrc` pins Node 22;
  run `nvm use` if you use nvm.
- npm (the lockfile is npm).

## Run locally

```bash
npm install       # first time only
npm run dev
```

Open http://localhost:3000. The site serves from the domain root in development (no base
path).

## Build

```bash
npm run build     # production build (Next.js, Turbopack)
npm run start     # serve the production build on http://localhost:3000
npm run types:check   # optional: regenerate types and run tsc
```

## Deployment

The site is built for `/` with an empty `NEXT_PUBLIC_BASE_PATH`.

```bash
NEXT_PUBLIC_BASE_PATH= \
NEXT_PUBLIC_SITE_URL=https://dev-workshop.demohouse.cloud \
npm run build -- --webpack
```

Promotion is branch-based:

1. Create a feature branch and PR it to protected `dev-build-workshop-v1`.
2. Required workshop CI passes; merging deploys that SHA to the dev hostname.
3. Test dev, then open a `dev-build-workshop-v1` to `build-workshop-v1` PR.
4. Merging deploys the same source revision to the production hostname.

The public workflow uses GitHub OIDC, ECR immutable SHA tags, SSM, container health, and
route probes. Old `/workshop` URLs redirect to the matching dedicated hostname.

Legacy module 07/08 lesson paths remain as unlisted compatibility pages for static exports;
the recommended Node deployment redirects those paths to their new module numbers.

## Content authoring

All content is MDX under `content/docs/`. The site is dual-track: every module has a
Learner page and an Instructor page.

```
content/docs/
  index.mdx                # the overview (hero, tracks, scope, modules table, ...)
  meta.json                # top-level ordering: index, learner, instructor
  learner/
    meta.json              # root:true -> Learner track tab; orders the modules
    index.mdx              # track landing
    00-setup.mdx ... 09-wrap-up.mdx
  instructor/
    meta.json              # root:true -> Instructor track tab; orders the modules
    index.mdx              # run of show + shared-resource checklist
    00-setup.mdx ... 09-wrap-up.mdx
```

- Ordering is controlled by the `pages` array in each `meta.json` (file basenames,
  without extension).
- The `root: true` flag on `learner/` and `instructor/` turns each into a switchable
  sidebar tab.
- The page title (rendered as the `<h1>`) comes from the frontmatter `title`; do not
  add a top-level `#` heading in the body.
- Available MDX components include `Cards`/`Card` and `Callout` (from Fumadocs). No
  imports are needed for these.
- No emojis anywhere, in content or code.

### Platform contract

The learner track supports macOS and Windows. Windows means Ubuntu on WSL 2 with Docker
Desktop WSL integration; it does not mean translating shared Bash blocks into PowerShell.

- Put Windows host bootstrap or repair commands in a `powershell` fence inside
  `<PlatformOnly platform="windows">`.
- Keep all workshop, Docker, Git, `clickhousectl`, ClickHouse client, and preflight commands
  in `bash` fences. Windows learners run them in Ubuntu.
- Put platform-specific macOS text inside `<PlatformOnly platform="macos">` when the
  Windows path differs.
- Never tell Windows learners to clone under `/mnt/c`; use their WSL home directory.
- Keep shell scripts LF-only. The Windows CI job enforces the platform contract and builds
  the complete playbook on a Windows runner.

### The per-module learner contract

Every learner module page follows this skeleton, in order:

1. Frontmatter `title` (`NN Module Name`) and `description`.
2. `## Outcome` or `## Starting point` — the result, time budget, and prerequisites.
3. `## Why` — the motivation for the step.
4. `## Goal` — the single concrete outcome.
5. `## Step N — verb phrase` — one section per step. Each step names the literal file
   paths it touches as `### `path`` subheadings, with code blocks (mark unfinished
   implementation details with `TODO`).
6. `## How to verify you are done` — a self-check.
7. `## Wrap-up` — what just happened.
8. `## End state` — the handoff into the next module.

### The instructor page contract

Every instructor module page has these four sections: `## Timing`, `## Talk track`,
`## Common failures`, `## Reset steps`. Content is filled in from rehearsal; unknowns are
marked `TODO`.

### Learner branch convention

Learners clone the repository's default branch, then Module 00 switches them to
`build-workshop-v1`, the complete workshop reference. They stay there except while
running a Module 07 `fault/*` scenario, then switch back to `build-workshop-v1`.

## Project layout

| Path | Description |
|---|---|
| `src/app/(home)` | Landing page. |
| `src/app/docs` | Docs layout and the catch-all page renderer. |
| `src/app/api/search/route.ts` | Orama search route. |
| `src/lib/shared.ts` | App name, routes, GitHub/app-repo config. |
| `src/lib/layout.shared.tsx` | Shared nav options (title, track links, GitHub). |
| `source.config.ts` | Fumadocs MDX collection config (frontmatter schema). |
| `next.config.mjs` | Next config; `basePath` wiring. |
