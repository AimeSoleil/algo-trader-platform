#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME=""
WATCH_MODE=0
INTERVAL_SECONDS=5
LIGHT_MODE=0

while (( $# > 0 )); do
  case "$1" in
    -w|--watch)
      WATCH_MODE=1
      shift
      ;;
    -i|--interval)
      INTERVAL_SECONDS="${2:?--interval requires a value}"
      shift 2
      ;;
    --interval=*)
      INTERVAL_SECONDS="${1#--interval=}"
      shift
      ;;
    -l|--light)
      LIGHT_MODE=1
      shift
      ;;
    -h|--help)
      cat <<'EOF'
Usage: ./scripts/rabbitmq_diag.sh [container_name] [--watch] [--interval 5] [--light]

Options:
  -w, --watch         Refresh continuously
  -i, --interval N    Refresh interval in seconds (default: 5)
  -l, --light         Only show alarms / queues / connections
EOF
      exit 0
      ;;
    *)
      if [[ -z "$CONTAINER_NAME" ]]; then
        CONTAINER_NAME="$1"
        shift
      else
        echo "[rabbitmq_diag] Unknown argument: $1" >&2
        exit 1
      fi
      ;;
  esac
done

find_container() {
  if [[ -n "$CONTAINER_NAME" ]]; then
    printf '%s\n' "$CONTAINER_NAME"
    return
  fi

  local by_name
  by_name="$(docker ps --format '{{.Names}}' | grep -E '(^|_)algo_rabbitmq$' | head -n1 || true)"
  if [[ -n "$by_name" ]]; then
    printf '%s\n' "$by_name"
    return
  fi

  local by_image
  by_image="$(docker ps --filter ancestor=rabbitmq:3-management-alpine --format '{{.Names}}' | head -n1 || true)"
  if [[ -n "$by_image" ]]; then
    printf '%s\n' "$by_image"
    return
  fi

  echo "[rabbitmq_diag] No running RabbitMQ container found" >&2
  exit 1
}

CONTAINER="$(find_container)"

print_snapshot() {
  echo "== RabbitMQ Container =="
  echo "timestamp=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "container=${CONTAINER}"
  docker ps --filter "name=${CONTAINER}" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
  echo

  if (( LIGHT_MODE == 1 )); then
    echo "== RabbitMQ Alarms =="
    docker exec "$CONTAINER" rabbitmq-diagnostics alarms || true
    echo

    echo "== Connections =="
    docker exec "$CONTAINER" rabbitmqctl list_connections name peer_host peer_port state channels protocol auth_mechanism timeout 2>/dev/null || true
    echo

    echo "== Queues =="
    docker exec "$CONTAINER" rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers memory state 2>/dev/null || true
    return
  fi

  echo "== Container Memory Limit =="
  docker inspect --format 'memory_limit_bytes={{.HostConfig.Memory}}' "$CONTAINER"
  echo

  echo "== RabbitMQ Alarms =="
  docker exec "$CONTAINER" rabbitmq-diagnostics alarms || true
  echo

  echo "== RabbitMQ Summary =="
  docker exec "$CONTAINER" rabbitmq-diagnostics status | grep -E 'Memory alarm|File Descriptors|Free Disk Space|Low free disk space watermark|Total memory used|Memory high watermark|connection_max|channel_max' || true
  echo

  echo "== Memory Breakdown =="
  docker exec "$CONTAINER" rabbitmq-diagnostics memory_breakdown -q || true
  echo

  echo "== Connections =="
  docker exec "$CONTAINER" rabbitmqctl list_connections name peer_host peer_port state channels protocol auth_mechanism timeout 2>/dev/null || true
  echo

  echo "== Queues =="
  docker exec "$CONTAINER" rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers memory state 2>/dev/null || true
  echo

  echo "== Channels =="
  docker exec "$CONTAINER" rabbitmqctl list_channels connection number consumer_count messages_unacknowledged prefetch_count 2>/dev/null || true
}

if (( WATCH_MODE == 1 )); then
  while true; do
    if [[ -t 1 ]]; then
      clear
    fi
    print_snapshot
    sleep "$INTERVAL_SECONDS"
  done
else
  print_snapshot
fi