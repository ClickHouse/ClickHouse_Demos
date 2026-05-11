#!/bin/bash
# Renders Mermaid diagram sources in part1/diagrams/ to PNG.
# Requires: npx (Node.js) with @mermaid-js/mermaid-cli available via npx.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIAGRAMS_DIR="$SCRIPT_DIR/../diagrams"

command -v npx >/dev/null 2>&1 || { echo "ERROR: npx not found. Install Node.js first."; exit 1; }

echo "Rendering Mermaid diagrams in $DIAGRAMS_DIR ..."

for mmd_file in "$DIAGRAMS_DIR"/*.mmd; do
  [ -f "$mmd_file" ] || continue
  base="$(basename "$mmd_file" .mmd)"
  out="$DIAGRAMS_DIR/${base}.png"
  echo "  $base.mmd → $base.png"
  npx --yes @mermaid-js/mermaid-cli \
    --input "$mmd_file" \
    --output "$out" \
    --backgroundColor white \
    --width 1400 \
    --quiet
done

echo "Done. PNG files written to $DIAGRAMS_DIR/"
