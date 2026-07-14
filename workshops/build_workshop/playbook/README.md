# ClickHouse BUILD Workshop playbook

The published playbook for the ClickHouse BUILD Workshop ("Build AI with AI"): a
three-hour, hands-on session where participants use their own agentic coding tool to take
an NYC-taxi ride-hailing analytics app end to end on ClickHouse Cloud.

This directory is the documentation site only. The workshop app that participants clone
and build on lives alongside it at `workshops/build_workshop/app` in this repository, on
the `build-workshop-v1` branch. The site is built with [Next.js](https://nextjs.org)
and [Fumadocs](https://fumadocs.dev), and is published at `demohouse.cloud/workshop`.

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

The site is served from a sub-path (`demohouse.cloud/workshop`), so `basePath` is the
key deployment lever. It is configurable via environment variables read in
`next.config.mjs`:

| Variable | Purpose | Example |
|---|---|---|
| `NEXT_PUBLIC_BASE_PATH` | Prefixes every route and asset. Set for sub-path hosting. | `/workshop` |
| `NEXT_PUBLIC_SITE_URL` | Absolute origin for Open Graph / social image URLs. | `https://demohouse.cloud` |

Both are empty/localhost by default so local development works with no configuration.

### Option A — Node hosting (recommended)

Run the Next.js server behind whatever fronts `demohouse.cloud`. This supports search and
the generated OG images with no caveats.

```bash
NEXT_PUBLIC_BASE_PATH=/workshop \
NEXT_PUBLIC_SITE_URL=https://demohouse.cloud \
npm run build

NEXT_PUBLIC_BASE_PATH=/workshop npm run start   # then reverse-proxy /workshop to it
```

### Option B — Static export

Fumadocs can be exported to static HTML, but the default Orama search route
(`/api/search`) and the OG image routes are server routes and do not run in a static
export. To go fully static you must switch search to Fumadocs' static/client search and
either drop or pre-render the OG images. If you take this path:

1. Add `output: 'export'` to `next.config.mjs`.
2. Switch search to the static build (see the Fumadocs search docs).
3. Build with the same `NEXT_PUBLIC_BASE_PATH=/workshop`, then serve the `out/`
   directory under `/workshop`.

Node hosting is recommended unless the target can only serve static files.

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

### The per-module learner contract

Every learner module page follows this skeleton, in order:

1. Frontmatter `title` (`NN Module Name`) and `description`.
2. `## Starting point` — the `git checkout checkpoint/NN-name` command and prerequisites.
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

### Checkpoint branch convention

The workshop app repository uses `checkpoint/NN-name` branches (for example
`checkpoint/03-realtime-cdc`) as the starting state for each module; `main` is the
complete reference. Learner pages reference these branches in their "Starting point".

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
