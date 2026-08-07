#!/usr/bin/env bash
# ============================================================
# render.sh — Render Mermaid (.mmd) diagrams to PNG via Docker
# ============================================================
# Uses the official `minlag/mermaid-cli` Docker image so no
# Node / npm install is required. Re-renders every .mmd file
# in this directory.
#
# Usage (from part3/diagrams/):
#   bash render.sh
#
# Output: <name>.png next to each <name>.mmd
# ============================================================

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

if ! command -v docker >/dev/null 2>&1; then
    echo "Error: docker not found in PATH. Install Docker Desktop (or your distro's docker package) first."
    exit 1
fi

# Pin the image tag so output stays reproducible across machines.
IMAGE="minlag/mermaid-cli:11.4.2"

shopt -s nullglob
mmd_files=( *.mmd )

if [[ ${#mmd_files[@]} -eq 0 ]]; then
    echo "No .mmd files found in $(pwd)."
    exit 0
fi

# Pull once so the per-file runs are quick.
docker image inspect "${IMAGE}" >/dev/null 2>&1 || docker pull "${IMAGE}"

for src in "${mmd_files[@]}"; do
    out="${src%.mmd}.png"
    echo "→ ${src} → ${out}"
    docker run --rm \
        -u "$(id -u):$(id -g)" \
        -v "$(pwd):/data" \
        "${IMAGE}" \
        --input "/data/${src}" \
        --output "/data/${out}" \
        --backgroundColor "white" \
        --width 1800 \
        --scale 2
done

echo
echo "✓ Rendered ${#mmd_files[@]} diagram(s)."
