#!/usr/bin/env bash
set -euo pipefail

BASE_URL=${1:-http://localhost:8000}

if [[ -z "${ADMIN_TOKEN:-}" ]]; then
  echo "ADMIN_TOKEN must be set in the environment."
  exit 1
fi

curl -s -X POST "$BASE_URL/api/ingest" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"seeds": []}'
