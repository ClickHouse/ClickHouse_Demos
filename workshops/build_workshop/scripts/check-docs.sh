#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CONTENT="${ROOT}/playbook/content/docs"

fail_if_found() {
  local description=$1
  local pattern=$2
  shift 2
  if rg -n --pcre2 "${pattern}" "$@"; then
    echo "ERROR: ${description}" >&2
    exit 1
  fi
}

fail_if_found \
  "active workshop docs must use the dev/main promotion model" \
  'build-workshop-v1' \
  "${ROOT}/README.md" "${CONTENT}"

fail_if_found \
  "spell the Cloud management CLI as clickhousectl" \
  '(^|[^[:alnum:]_-])chctl([^[:alnum:]_-]|$)' \
  "${ROOT}/README.md" "${CONTENT}"

fail_if_found \
  "learners must not be told to edit SQL comments or execute a hidden SQL file" \
  '(comment|uncomment).*(sql|variant)|(?:run|execute|open).*(?:db/cloud|\.sql file)' \
  "${CONTENT}/learner"

# MCP server definitions belong in setup. Later modules should use or link to
# that setup instead of asking learners to configure the same endpoint again.
if rg -n 'mcpServers|"clickhouse"\s*:\s*\{' "${CONTENT}/learner" --glob '!00-setup.mdx'; then
  echo "ERROR: learner MCP server configuration must appear only in 00-setup.mdx" >&2
  exit 1
fi

echo "Workshop documentation policy checks passed."
