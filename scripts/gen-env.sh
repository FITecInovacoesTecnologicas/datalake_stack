#!/usr/bin/env bash
# gen-env.sh — create an env file from env.example, filling every empty key with a random hex secret.
#
# Usage: bash scripts/gen-env.sh [<env-file>]     (default: .env)
#
# Shared by `make env` and `task env`. Never overwrites an existing file. The file is written
# with mode 0600 and the secrets are never printed. S3 credentials follow the limits RustFS
# enforces: access key 3-20 chars, secret key 8-40 chars. Everything else gets 48 hex chars.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

ENV_FILE="${1:-.env}"
if [[ -e "$ENV_FILE" ]]; then
  echo "$ENV_FILE ja existe; nada a fazer"
  exit 0
fi

umask 077
while IFS= read -r line; do
  if [[ "$line" =~ ^([A-Z0-9_]+)=$ ]]; then
    k="${BASH_REMATCH[1]}"
    case "$k" in
      LAKE_S3_ACCESS_KEY) v=$(openssl rand -hex 10) ;;   # 20 chars
      LAKE_S3_SECRET_KEY) v=$(openssl rand -hex 20) ;;   # 40 chars
      *) v=$(openssl rand -hex 24) ;;                    # 48 chars
    esac
    printf '%s=%s\n' "$k" "$v"
  else
    printf '%s\n' "$line"
  fi
done < env.example > "$ENV_FILE"
chmod 600 "$ENV_FILE"
echo "escrito $ENV_FILE (0600). Guarde LAKEKEEPER_ENCRYPTION_KEY fora do host: sem ela o catalogo nao e restauravel."
