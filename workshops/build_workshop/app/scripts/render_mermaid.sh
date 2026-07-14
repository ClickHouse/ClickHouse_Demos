#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

IN="${ROOT_DIR}/docs/architecture.mmd"
OUT_SVG="${ROOT_DIR}/docs/architecture.svg"

mkdir -p "${ROOT_DIR}/docs"

if command -v docker >/dev/null 2>&1; then
  # Prefer Docker to avoid requiring Node/npm on the host.
  # If this image tag changes in the future, update it here.
  docker run --rm \
    -u "$(id -u):$(id -g)" \
    -v "${ROOT_DIR}:/work" \
    ghcr.io/mermaid-js/mermaid-cli/mermaid-cli:latest \
    -i /work/docs/architecture.mmd \
    -o /work/docs/architecture.svg \
    -b transparent
  echo "Wrote ${OUT_SVG}"
  exit 0
fi

if command -v npx >/dev/null 2>&1; then
  # Fallback: use mermaid-cli via npx.
  (cd "${ROOT_DIR}" && npx --yes @mermaid-js/mermaid-cli -i "${IN}" -o "${OUT_SVG}" -b transparent)
  echo "Wrote ${OUT_SVG}"
  exit 0
fi

echo "Error: need either 'docker' or 'npx' to render Mermaid diagrams." >&2
exit 1

