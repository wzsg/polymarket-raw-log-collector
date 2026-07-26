#!/usr/bin/env bash
set -euo pipefail

required=(
  WORKER_NAME
  IMAGE
  CONFIG_PATH
  RPC_ENV_PATH
  DATA_DIR
  STATE_DIR
  TO_BLOCK
  HF_REPO_ID
)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "missing required variable: ${name}" >&2
    exit 2
  fi
done

HF_BIN="${HF_BIN:-$HOME/.local/bin/hf}"
UPLOAD_WORKERS="${UPLOAD_WORKERS:-8}"
CONTAINER_NAME="${CONTAINER_NAME:-polymarket-raw-${WORKER_NAME}}"
uid="$(id -u)"
gid="$(id -g)"

mkdir -p "$DATA_DIR" "$STATE_DIR"
test -r "$CONFIG_PATH"
test -r "$RPC_ENV_PATH"
test -x "$HF_BIN"

docker pull "$IMAGE"
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

cleanup() {
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run \
  --name "$CONTAINER_NAME" \
  --user "${uid}:${gid}" \
  --mount "type=bind,src=${CONFIG_PATH},dst=/config/config.toml,readonly" \
  --mount "type=bind,src=${RPC_ENV_PATH},dst=/run/secrets/polygon.env,readonly" \
  --mount "type=bind,src=${DATA_DIR},dst=/data" \
  --mount "type=bind,src=${STATE_DIR},dst=/state" \
  "$IMAGE" \
  collect \
  --config /config/config.toml \
  --env-file /run/secrets/polygon.env \
  --to-block "$TO_BLOCK"

docker rm "$CONTAINER_NAME" >/dev/null
trap - EXIT

docker run --rm \
  --user "${uid}:${gid}" \
  --mount "type=bind,src=${CONFIG_PATH},dst=/config/config.toml,readonly" \
  --mount "type=bind,src=${DATA_DIR},dst=/data" \
  --mount "type=bind,src=${STATE_DIR},dst=/state" \
  "$IMAGE" \
  manifest \
  --config /config/config.toml \
  --worker "$WORKER_NAME" \
  --expected-to-block "$TO_BLOCK" \
  --output "/data/manifests/${WORKER_NAME}.json"

"$HF_BIN" upload-large-folder \
  "$HF_REPO_ID" \
  "$DATA_DIR" \
  --repo-type dataset \
  --include "raw_chain_logs/**" \
  --include "manifests/**" \
  --num-workers "$UPLOAD_WORKERS"
