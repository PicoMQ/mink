#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HARNESS="$ROOT/harness"
SCENARIOS=("$@")
if [ ${#SCENARIOS[@]} -eq 0 ]; then
  SCENARIOS=(lite single cluster load chaos sql)
fi

export MINK_IMAGE="${MINK_IMAGE:-mink:local}"
export MINK_RUNNER_IMAGE="${MINK_RUNNER_IMAGE:-mink-runner:local}"
export MINK_QUERY_IMAGE="${MINK_QUERY_IMAGE:-mink-query:local}"
export MINK_PROFILE="${MINK_PROFILE:-release}"

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

stack_files() {
  case "$1" in
    lite) echo "-f $HARNESS/compose.lite.yml" ;;
    single) echo "-f $HARNESS/compose.yml" ;;
    cluster | load | chaos) echo "-f $HARNESS/compose.yml -f $HARNESS/compose.cluster.yml" ;;
    sql) echo "-f $HARNESS/compose.yml -f $HARNESS/compose.cluster.yml -f $HARNESS/compose.query.yml" ;;
    *) echo "unknown scenario: $1" >&2; exit 2 ;;
  esac
}

nodes() {
  case "$1" in
    lite) echo "mink" ;;
    single) echo "mink1" ;;
    sql) echo "mink1 mink2 mink3 query" ;;
    *) echo "mink1 mink2 mink3" ;;
  esac
}

compose() {
  local scenario="$1"
  shift
  # shellcheck disable=SC2046
  docker compose -p "mink-e2e-$scenario" $(stack_files "$scenario") -f "$HARNESS/compose.runner.yml" --profile runner "$@"
}

runner_env() {
  local scenario="$1"
  export MINK_E2E_SCENARIO="$scenario"
  export COMPOSE_PROJECT_NAME="mink-e2e-$scenario"
  export MINK_E2E_QUERY=""
  case "$scenario" in
    lite)
      export MINK_E2E_NODES="grpc://mink:9123" MINK_E2E_KAFKA="mink:9092" MINK_E2E_LAKE=0
      export MINK_LITE_ADVERTISE="grpc://mink:9123" MINK_LITE_KAFKA_ADVERTISE="mink:9092"
      ;;
    single)
      export MINK_E2E_NODES="grpc://mink1:9123" MINK_E2E_KAFKA="mink1:9092" MINK_E2E_LAKE=1
      ;;
    sql)
      export MINK_E2E_NODES="grpc://mink1:9123,grpc://mink2:9123,grpc://mink3:9123"
      export MINK_E2E_KAFKA="mink1:9092,mink2:9092,mink3:9092" MINK_E2E_LAKE=1
      export MINK_E2E_QUERY="grpc://query:9130"
      ;;
    *)
      export MINK_E2E_NODES="grpc://mink1:9123,grpc://mink2:9123,grpc://mink3:9123"
      export MINK_E2E_KAFKA="mink1:9092,mink2:9092,mink3:9092" MINK_E2E_LAKE=1
      ;;
  esac
}

test_targets() {
  case "$1" in
    lite) echo "--test functional" ;;
    single) echo "--test functional" ;;
    cluster) echo "--test functional --test cluster" ;;
    *) echo "--test $1" ;;
  esac
}

build() {
  docker volume create mink-e2e-target >/dev/null
  docker volume create mink-e2e-cargo >/dev/null
  if [ "${SKIP_BUILD:-}" = 1 ]; then
    return
  fi
  log "building $MINK_IMAGE, $MINK_QUERY_IMAGE and $MINK_RUNNER_IMAGE"
  runner_env sql
  compose sql build
}

run() {
  local scenario="$1"
  local status=0
  runner_env "$scenario"

  log "$scenario: up"
  # shellcheck disable=SC2046
  if ! compose "$scenario" up -d --wait $(nodes "$scenario"); then
    status=1
    log "$scenario: stack did not come up"
    compose "$scenario" ps -a || true
  else
    log "$scenario: tests"
    # shellcheck disable=SC2046,SC2086
    compose "$scenario" run --rm --no-deps runner $(test_targets "$scenario") -- --test-threads=1 --nocapture ${FILTER:-} || status=$?
  fi

  if [ "$status" -ne 0 ]; then
    log "$scenario: FAILED ($status), node logs follow"
    # shellcheck disable=SC2046
    compose "$scenario" logs --no-color --tail=300 $(nodes "$scenario") || true
  fi
  if [ "${KEEP:-}" != 1 ]; then
    log "$scenario: down"
    compose "$scenario" down -v --remove-orphans
  fi
  return "$status"
}

build
failed=()
for scenario in "${SCENARIOS[@]}"; do
  if ! run "$scenario"; then
    failed+=("$scenario")
  fi
done

if [ ${#failed[@]} -ne 0 ]; then
  log "failed: ${failed[*]}"
  exit 1
fi
log "all scenarios passed: ${SCENARIOS[*]}"
