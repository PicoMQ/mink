#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HARNESS="$ROOT/harness"
PROJECT=mink-e2e

export MINK_RUNNER_IMAGE="${MINK_RUNNER_IMAGE:-mink-runner:local}"

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

compose() {
  docker compose -p "$PROJECT" -f "$HARNESS/compose.yml" -f "$HARNESS/compose.trino.yml" -f "$HARNESS/compose.spark.yml" \
    -f "$HARNESS/compose.runner.yml" --profile runner "$@"
}

docker volume create mink-e2e-target >/dev/null
docker volume create mink-e2e-cargo >/dev/null
docker volume create mink-e2e-node >/dev/null
docker volume create mink-e2e-npm >/dev/null

if [ "${SKIP_CONSOLE:-}" != 1 ]; then
  log "console"
  docker run --rm -v mink-e2e-node:/node_modules -v mink-e2e-npm:/npm node:24-bookworm-slim \
    chown -R "$(id -u):$(id -g)" /node_modules /npm
  docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp -e npm_config_cache=/npm \
    -v "$ROOT:/src" -v mink-e2e-node:/src/console/node_modules -v mink-e2e-npm:/npm -w /src/console \
    node:24-bookworm-slim \
    sh -c "npm ci --prefer-offline --no-audit --no-fund && npm run check && npm run lint && npm test && npm run build"
fi
if [ "${SKIP_BUILD:-}" != 1 ]; then
  log "building $MINK_RUNNER_IMAGE"
  compose build runner
fi

status=0
log "up"
if ! compose up -d --wait kafka postgres rustfs schema-registry trino spark || ! compose run --rm createbucket ||
  ! compose run --rm createusers; then
  status=1
  log "the stack did not come up"
  compose ps -a || true
else
  log "tests"
  # shellcheck disable=SC2086
  compose run --rm --no-deps --use-aliases runner -- --nocapture ${FILTER:-} || status=$?
fi
if [ "$status" -eq 0 ] && [ "${SKIP_CONSOLE:-}" != 1 ]; then
  log "console e2e"
  docker volume create mink-e2e-node >/dev/null
  docker rm -f "$PROJECT-console" >/dev/null 2>&1 || true
  if ! compose run --rm --no-deps --entrypoint cargo runner build --locked -p mink-cli ||
    ! compose run -d --no-deps --name "$PROJECT-console" --entrypoint /target/debug/mink runner \
      serve -c harness/console.toml >/dev/null ||
    ! docker run --rm -v mink-e2e-node:/node_modules node:24-bookworm-slim \
      chown -R "$(id -u):$(id -g)" /node_modules; then
    status=1
  else
    docker run --rm --network "container:$PROJECT-console" --user "$(id -u):$(id -g)" \
      -e HOME=/tmp -e npm_config_cache=/npm -e CI=1 \
      -e MINK_ADMIN=http://localhost:9180 -e MINK_KAFKA=kafka:9092 -e MINK_REGISTRY=http://schema-registry:8081 \
      -v "$ROOT:/src" -v mink-e2e-node:/src/console/node_modules \
      -v mink-e2e-npm:/npm -w /src/console \
      mcr.microsoft.com/playwright:v1.64.1-noble \
      sh -c "npm ci --prefer-offline --no-audit --no-fund && npm run e2e" || status=$?
  fi
  if [ "$status" -ne 0 ]; then
    docker logs --tail=300 "$PROJECT-console" || true
  fi
  docker rm -f "$PROJECT-console" >/dev/null 2>&1 || true
fi

if [ "$status" -ne 0 ]; then
  log "FAILED ($status), logs follow"
  compose logs --no-color --tail=300 kafka postgres rustfs schema-registry trino spark || true
fi
if [ "${KEEP:-}" != 1 ]; then
  log "down"
  compose down -v --remove-orphans
fi
exit "$status"
