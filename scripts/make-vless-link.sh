#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
  echo "Usage: $0 <host> <uuid> <ws_path> [remark]" >&2
  exit 1
fi

host="$1"
uuid="$2"
ws_path="$3"
remark="${4:-cf-worker-vless}"

if [[ "$ws_path" != /* ]]; then
  ws_path="/$ws_path"
fi

printf 'vless://%s@%s:443?encryption=none&security=tls&sni=%s&type=ws&host=%s&path=%s&fp=chrome#%s\n' \
  "$uuid" \
  "$host" \
  "$host" \
  "$host" \
  "$(python3 - <<'PY' "$ws_path"
import sys, urllib.parse
print(urllib.parse.quote(sys.argv[1], safe=""))
PY
)" \
  "$remark"
