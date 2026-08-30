#!/usr/bin/env bash

set -euo pipefail

CONTEXT="quan@tandemn-control-plane.us-east-1.eksctl.io"
NAMESPACE="default"
SERVICE="chunk-manager-rw"
LOCAL_PORT="60001"

cleanup() {
  if [[ -n "${PORT_FORWARD_PID:-}" ]]; then
    kill "$PORT_FORWARD_PID" 2>/dev/null || true
    wait "$PORT_FORWARD_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

export PGPASSWORD="$(
  kubectl --context "$CONTEXT" get secret chunk-manager-app --namespace "$NAMESPACE" \
    --output jsonpath='{.data.password}' | base64 --decode
)"
export PGSSLMODE=disable

kubectl --context "$CONTEXT" port-forward "service/$SERVICE" "$LOCAL_PORT:5432" >/dev/null 2>&1 &
PORT_FORWARD_PID=$!

connected=false
for _ in {1..30}; do
  if ! kill -0 "$PORT_FORWARD_PID" 2>/dev/null; then
    wait "$PORT_FORWARD_PID"
  fi

  if psql --host 127.0.0.1 --port "$LOCAL_PORT" --username app --dbname app --command 'SELECT 1;' >/dev/null 2>&1; then
    connected=true
    break
  fi

  sleep 1
done

if [[ "$connected" != true ]]; then
  printf 'Timed out waiting for the database port-forward.\n' >&2
  exit 1
fi

psql --host 127.0.0.1 --port "$LOCAL_PORT" --username app --dbname app <<'SQL'
DELETE FROM chunks;
DELETE FROM job_chain_associations;
DELETE FROM jobs;
SQL
