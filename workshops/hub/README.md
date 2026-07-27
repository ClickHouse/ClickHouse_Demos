# Workshops hub

The workshop-agnostic **portal**: a single landing page that lists the available
ClickHouse workshops and links out to each one. It is deliberately decoupled from
any individual workshop so new workshops can be added without touching the others.

## Architecture

Each workshop is an independent module under `workshops/<name>/`, in whatever
format suits it (the build workshop is a Fumadocs playbook; the RTA mini workshop
is a standalone HTML guide). The hub sits above them and points at each one:

```
workshops/
  hub/                 # this portal (workshop-agnostic)
  build_workshop/      # module — Fumadocs playbook + app
  RTA-mini-workshop/   # module — standalone HTML guide + dashboard
  <future>/            # module — any format
```

Intended deploy topology (behind the existing reverse proxy / path routing):

| Path | Serves |
|---|---|
| `/` (or `/workshops`) | this hub |
| `/workshop` | build workshop playbook |
| `/rta-mini` | RTA mini workshop guide |
| `/<future>` | each new workshop |

The hub is self-contained static HTML — no build step, no dependencies.

## Adding a workshop

1. Add the workshop module under `workshops/<name>/`.
2. Add an entry to [`workshops.json`](./workshops.json) (the source of truth).
3. Add a matching `<a class="card">` in [`index.html`](./index.html), and point
   its `href` at the workshop's served URL.
4. Wire the reverse proxy to serve the new workshop at its path.

## Links

Card `href`s reflect the deploy topology above and are easy to change:
- **Build AI with AI** → the live build-workshop playbook.
- **Real-Time Market Analytics** → the RTA mini guide (relative link within the
  repo; swap for the deployed path once routing is wired).
