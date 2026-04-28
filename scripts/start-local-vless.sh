#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUNDLED_NODE="/Users/chenzx/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node"

export VLESS_UUID="${VLESS_UUID:-30625464-78e5-4785-a466-5649b8a7b18f}"
export WS_PATH="${WS_PATH:-/ws-tuug99w001ckb21l}"
export HOST="${HOST:-127.0.0.1}"
export PORT="${PORT:-3000}"

if [ ! -x "$BUNDLED_NODE" ]; then
  echo "Bundled node not found: $BUNDLED_NODE" >&2
  exit 1
fi

cd "$PROJECT_DIR"
exec "$BUNDLED_NODE" src/server.js
