#!/usr/bin/env bash
# Start / stop a LOCAL OSS redis-server with the RediSearch module loaded, for
# functional smoke-testing of the harness IN-RAM. This is NOT the disk path:
# plain OSS redis has no on-disk search (it rejects bigredis-* config). It only
# validates the harness logic (indexing, TTL de-indexing, no-stale-hits, queries,
# DEBUG RELOAD). For the real disk run, point the harness at a Flex/BigRedis
# server configured per deploy/flex-redis.conf.
set -euo pipefail

PORT="${PORT:-6399}"
REDIS_MODULE="${REDIS_MODULE:-/home/jonathan/CLionProjects/RediSearchDisk/build/redisearch.so}"
REDIS_SERVER="${REDIS_SERVER:-redis-server}"
WORKDIR="${WORKDIR:-$(mktemp -d)}"
PIDFILE="/tmp/chatstress-redis-$PORT.pid"

start() {
  if [[ ! -f "$REDIS_MODULE" ]]; then
    echo "ERROR: module not found: $REDIS_MODULE (set REDIS_MODULE=...)" >&2
    exit 1
  fi
  echo "starting redis-server on :$PORT with module $REDIS_MODULE (dir=$WORKDIR)"
  "$REDIS_SERVER" \
    --port "$PORT" \
    --dir "$WORKDIR" \
    --save '' \
    --enable-module-command yes \
    --enable-debug-command yes \
    --loadmodule "$REDIS_MODULE" \
    --daemonize no &
  echo $! > "$PIDFILE"
  for _ in $(seq 1 30); do
    if redis-cli -p "$PORT" ping >/dev/null 2>&1; then echo "ready"; return 0; fi
    sleep 0.2
  done
  echo "ERROR: redis did not become ready" >&2; exit 1
}

stop() {
  redis-cli -p "$PORT" shutdown nosave >/dev/null 2>&1 || true
  [[ -f "$PIDFILE" ]] && kill "$(cat "$PIDFILE")" >/dev/null 2>&1 || true
  rm -f "$PIDFILE"
  echo "stopped"
}

case "${1:-}" in
  start) start ;;
  stop)  stop ;;
  *) echo "usage: PORT=.. REDIS_MODULE=.. $0 {start|stop}" >&2; exit 2 ;;
esac
