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

## Wiring the reverse proxy

The site is served behind the demohouse reverse proxy (nginx / ALB). The hub is
static and the workshops are independent units; the proxy just maps origin paths
to each. Card `href`s are origin-relative (`/workshop`, `/rta-mini/`) to match.

Reference nginx routing:

```nginx
# / → the hub (this folder, served as static files)
location = / {
  root /srv/workshops/hub;
  try_files /index.html =404;
}

# /workshop → the build_workshop playbook (Next standalone container on :3000,
# already built with basePath=/workshop)
location /workshop/ {
  proxy_pass http://build-workshop:3000;
  proxy_set_header Host $host;
  proxy_set_header X-Forwarded-Proto $scheme;
}

# /rta-mini → the RTA mini guide (static: workshops/RTA-mini-workshop/)
location /rta-mini/ {
  alias /srv/workshops/RTA-mini-workshop/;
  try_files $uri $uri/ /index.html;
}
```

Each new workshop adds one `location` block + one manifest entry + one card.

> **Local preview:** because the links are origin-relative, open the hub through a
> static server rooted at `workshops/` (e.g. `python3 -m http.server` from there,
> then visit `/hub/`) rather than `file://`, so `/workshop` and `/rta-mini/`
> resolve.
