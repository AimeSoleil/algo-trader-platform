#!/usr/bin/env bash
set -euo pipefail

# ── Log directory ──────────────────────────────────────────
LOG_DIR="${LOG_DIR:-logs}"
mkdir -p "$LOG_DIR"

echo "[run_workers] 启动 Celery workers 与 beat...  (logs → $LOG_DIR/)"

uv run celery -A shared.celery_app.celery_app worker -Q data -n data@%h \
  --loglevel=INFO --logfile="$LOG_DIR/celery-data.log" &
PID_DATA=$!

uv run celery -A shared.celery_app.celery_app worker -Q backfill -n backfill@%h \
  --loglevel=INFO --logfile="$LOG_DIR/celery-backfill.log" &
PID_BACKFILL=$!

uv run celery -A shared.celery_app.celery_app worker -Q signal -n signal@%h \
  --loglevel=INFO --logfile="$LOG_DIR/celery-signal.log" &
PID_SIGNAL=$!

uv run celery -A shared.celery_app.celery_app worker -Q analysis -n analysis@%h \
  --loglevel=INFO --logfile="$LOG_DIR/celery-analysis.log" &
PID_ANALYSIS=$!

uv run celery -A shared.celery_app.celery_app beat \
  --loglevel=INFO --logfile="$LOG_DIR/celery-beat.log" &
PID_BEAT=$!

echo "[run_workers] data worker pid=${PID_DATA}"
echo "[run_workers] backfill worker pid=${PID_BACKFILL}"
echo "[run_workers] signal worker pid=${PID_SIGNAL}"
echo "[run_workers] analysis worker pid=${PID_ANALYSIS}"
echo "[run_workers] beat pid=${PID_BEAT}"
echo "[run_workers] Ctrl+C 停止全部进程"

cleanup() {
  echo "[run_workers] 停止所有 Celery 进程..."
  kill "$PID_DATA" "$PID_BACKFILL" "$PID_SIGNAL" "$PID_ANALYSIS" "$PID_BEAT" 2>/dev/null || true
}

trap cleanup INT TERM EXIT
wait
