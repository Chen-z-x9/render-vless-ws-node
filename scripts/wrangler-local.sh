#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUNDLED_NODE="/Users/chenzx/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node"
WRANGLER_JS="$PROJECT_DIR/node_modules/wrangler/bin/wrangler.js"

if [ ! -x "$BUNDLED_NODE" ]; then
  echo "Bundled node not found: $BUNDLED_NODE" >&2
  exit 1
fi

if [ ! -f "$WRANGLER_JS" ]; then
  echo "Wrangler is not installed. Run npm install first." >&2
  exit 1
fi

exec "$BUNDLED_NODE" "$WRANGLER_JS" "$@"
