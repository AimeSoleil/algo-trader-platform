#!/usr/bin/env bash
###############################################################################
# run_workers.sh — Enhanced Celery worker manager for local development
#
# Capabilities:
#   • Starts data / backfill / signal / analysis workers + beat (+ flower)
#   • Auto-restarts crashed processes with exponential backoff (2s → 60s)
#   • Background health-check watchdog (celery inspect ping)
#   • Graceful two-phase shutdown: SIGTERM → grace period → SIGKILL
#   • PID file management with stale-process cleanup on startup
#   • Coloured, timestamped log output
#
# Configuration (environment variables):
#   LOG_DIR                 Log / PID directory          (default: logs)
#   DAEMON_MODE             Start manager in background  (default: 1)
#   RESTART_MAX_BACKOFF     Max backoff seconds          (default: 60)
#   HEALTH_CHECK_INTERVAL   Seconds between pings        (default: 30)
#   HEALTH_CHECK_FAILURES   Consecutive fails before kill (default: 3)
#   SHUTDOWN_GRACE          Seconds before SIGKILL        (default: 30)
#   WORKERS                 Comma-separated worker list  (default: all)
#                           Choices: data,backfill,signal,analysis
#   ENABLE_FLOWER           Set to 1 to start Flower     (default: 0)
#   FLOWER_PORT             Flower listen port           (default: 5555)
#   LOG_LEVEL               Celery worker log level      (default: INFO)
#                           Choices: DEBUG,INFO,WARNING,ERROR,CRITICAL
#
# Usage:
#   ./scripts/run_workers.sh [--with-flower] [--workers data,signal] [--loglevel DEBUG]
#   ./scripts/run_workers.sh --foreground    # keep manager attached to current terminal
#   ./scripts/run_workers.sh --stop          # stop all workers, beat & flower
#   ./scripts/run_workers.sh --status        # show manager/workers status
#   ./scripts/run_workers.sh --list          # show available worker names
#   ENABLE_FLOWER=1 ./scripts/run_workers.sh
#   WORKERS=data,signal ./scripts/run_workers.sh
#   Ctrl+C to stop all processes.
#
# Available workers:
#   data       行情数据采集与盘后批量入库 (queue: data)
#   backfill   缺口检测与历史数据回填     (queue: backfill)
#   signal     盘后特征计算与信号生成     (queue: signal)
#   analysis   LLM 蓝图生成与分析        (queue: analysis)
###############################################################################
set -euo pipefail

# ── Configuration ──────────────────────────────────────────
LOG_DIR="${LOG_DIR:-logs}"
DAEMON_MODE="${DAEMON_MODE:-1}"
RESTART_MAX_BACKOFF="${RESTART_MAX_BACKOFF:-60}"
HEALTH_CHECK_INTERVAL="${HEALTH_CHECK_INTERVAL:-30}"
HEALTH_CHECK_FAILURES="${HEALTH_CHECK_FAILURES:-3}"
SHUTDOWN_GRACE="${SHUTDOWN_GRACE:-30}"
WORKERS="${WORKERS:-all}"
ENABLE_FLOWER="${ENABLE_FLOWER:-0}"
FLOWER_PORT="${FLOWER_PORT:-5555}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"

FOREGROUND=0

ALL_QUEUES=(data backfill signal analysis)

# Worker descriptions (used by --list and --help)
declare -A WORKER_DESC=(
  [data]="行情数据采集与盘后批量入库"
  [backfill]="缺口检测与历史数据回填"
  [signal]="盘后特征计算与信号生成"
  [analysis]="LLM 蓝图生成与分析"
)

show_workers() {
  echo "可用 workers:"
  echo ""
  printf "  %-12s %s\n" "NAME" "DESCRIPTION"
  printf "  %-12s %s\n" "────" "───────────"
  for q in "${ALL_QUEUES[@]}"; do
    printf "  %-12s %s\n" "$q" "${WORKER_DESC[$q]}"
  done
  echo ""
  echo "用法: ./scripts/run_workers.sh --workers=data,signal"
  echo "默认启动全部: ${ALL_QUEUES[*]}"
}

show_status() {
  mkdir -p "$LOG_DIR"
  local manager_pid_file="$LOG_DIR/run_workers-manager.pid"

  echo "[run_workers] Status"
  echo "  LOG_DIR=${LOG_DIR}"

  if [[ -f "$manager_pid_file" ]]; then
    local manager_pid
    manager_pid="$(cat "$manager_pid_file" 2>/dev/null || true)"
    if [[ -n "$manager_pid" ]] && kill -0 "$manager_pid" 2>/dev/null; then
      echo "  manager   : RUNNING (pid=${manager_pid})"
    else
      echo "  manager   : STALE_PID_FILE"
    fi
  else
    echo "  manager   : STOPPED"
  fi

  local names=("${ALL_QUEUES[@]}" beat flower watchdog)
  for name in "${names[@]}"; do
    local pidfile="$LOG_DIR/${name}.pid"
    if [[ -f "$pidfile" ]]; then
      local pid
      pid="$(cat "$pidfile" 2>/dev/null || true)"
      if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        echo "  ${name}$(printf '%*s' $((10-${#name})) ''): RUNNING (pid=${pid})"
      else
        echo "  ${name}$(printf '%*s' $((10-${#name})) ''): STALE_PID_FILE"
      fi
    else
      echo "  ${name}$(printf '%*s' $((10-${#name})) ''): STOPPED"
    fi
  done
}

# Accept CLI flags
for arg in "$@"; do
  case "$arg" in
    --with-flower) ENABLE_FLOWER=1 ;;
    --workers=*)   WORKERS="${arg#--workers=}" ;;
    --loglevel=*)  LOG_LEVEL="${arg#--loglevel=}" ;;
    --foreground)  FOREGROUND=1 ;;
    --stop)
      # ── Stop all managed processes ────────────────────────
      echo "[run_workers] Stopping all Celery processes..."
      mkdir -p "$LOG_DIR"
      MANAGER_PID_FILE="$LOG_DIR/run_workers-manager.pid"
      stopped=0

      # 0) Stop manager daemon first (if running)
      if [[ -f "$MANAGER_PID_FILE" ]]; then
        manager_pid="$(cat "$MANAGER_PID_FILE" 2>/dev/null || true)"
        if [[ -n "$manager_pid" ]] && kill -0 "$manager_pid" 2>/dev/null; then
          echo "  SIGTERM → manager (pid=${manager_pid})"
          kill "$manager_pid" 2>/dev/null || true
          stopped=$((stopped + 1))
        fi
        rm -f "$MANAGER_PID_FILE"
      fi

      # 1) Kill processes tracked by PID files
      for pidfile in "${LOG_DIR}"/*.pid; do
        [[ -f "$pidfile" ]] || continue
        [[ "$(basename "$pidfile")" == "run_workers-manager.pid" ]] && continue
        pid="$(cat "$pidfile" 2>/dev/null)" || continue
        name="$(basename "$pidfile" .pid)"
        if kill -0 "$pid" 2>/dev/null; then
          echo "  SIGTERM → ${name} (pid=${pid})"
          kill "$pid" 2>/dev/null || true
          stopped=$((stopped + 1))
        fi
        rm -f "$pidfile"
      done

      # 2) Also ask Celery to shut down any workers it knows about
      uv run celery -A shared.celery_app.celery_app control shutdown 2>/dev/null || true

      # 3) Kill any lingering celery processes owned by this user
      pkill -f "celery.*shared.celery_app" 2>/dev/null || true

      # Brief grace period then force-kill survivors
      sleep 2
      pkill -9 -f "celery.*shared.celery_app" 2>/dev/null || true

      if (( stopped > 0 )); then
        echo "[run_workers] Sent SIGTERM to ${stopped} process(es). All stopped."
      else
        echo "[run_workers] No tracked processes found; sent shutdown to any lingering celery processes."
      fi
      exit 0
      ;;
    --status)
      show_status
      exit 0
      ;;
    --list|--help|-h)
      show_workers
      exit 0
      ;;
  esac
done

# foreground mode overrides daemon setting
if (( FOREGROUND == 1 )); then
  DAEMON_MODE=0
fi

# Normalise and validate LOG_LEVEL
LOG_LEVEL="$(echo "$LOG_LEVEL" | tr '[:lower:]' '[:upper:]')"
case "$LOG_LEVEL" in
  DEBUG|INFO|WARNING|ERROR|CRITICAL) ;;
  *)
    echo "[run_workers] 无效的日志级别: '${LOG_LEVEL}'" >&2
    echo "  可选值: DEBUG, INFO, WARNING, ERROR, CRITICAL" >&2
    exit 1
    ;;
esac

# Resolve which workers to start
if [[ "$WORKERS" == "all" ]]; then
  ACTIVE_WORKERS=("${ALL_QUEUES[@]}")
else
  IFS=',' read -ra ACTIVE_WORKERS <<< "$WORKERS"
  # Validate worker names
  for w in "${ACTIVE_WORKERS[@]}"; do
    found=0
    for q in "${ALL_QUEUES[@]}"; do [[ "$w" == "$q" ]] && found=1 && break; done
    if (( found == 0 )); then
      echo "[run_workers] 未知 worker: '${w}'" >&2
      echo "" >&2
      show_workers >&2
      exit 1
    fi
  done
fi

CELERY_CMD="uv run celery -A shared.celery_app.celery_app"

mkdir -p "$LOG_DIR"
MANAGER_PID_FILE="$LOG_DIR/run_workers-manager.pid"

# ── Daemon launcher (default) ─────────────────────────────
if [[ "$DAEMON_MODE" == "1" ]]; then
  if [[ -f "$MANAGER_PID_FILE" ]]; then
    existing_pid="$(cat "$MANAGER_PID_FILE" 2>/dev/null || true)"
    if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" 2>/dev/null; then
      echo "[run_workers] manager 已在后台运行 (pid=${existing_pid})"
      echo "[run_workers] 使用 --stop 停止全部进程"
      exit 0
    fi
    rm -f "$MANAGER_PID_FILE"
  fi

  script_path="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
  LOG_DIR="$LOG_DIR" \
  RESTART_MAX_BACKOFF="$RESTART_MAX_BACKOFF" \
  HEALTH_CHECK_INTERVAL="$HEALTH_CHECK_INTERVAL" \
  HEALTH_CHECK_FAILURES="$HEALTH_CHECK_FAILURES" \
  SHUTDOWN_GRACE="$SHUTDOWN_GRACE" \
  WORKERS="$WORKERS" \
  ENABLE_FLOWER="$ENABLE_FLOWER" \
  FLOWER_PORT="$FLOWER_PORT" \
  LOG_LEVEL="$LOG_LEVEL" \
  DAEMON_MODE=0 \
  nohup "$script_path" --foreground >> "$LOG_DIR/run_workers-manager.out" 2>&1 &

  daemon_pid=$!
  echo "$daemon_pid" > "$MANAGER_PID_FILE"
  echo "[run_workers] manager 后台启动成功 (pid=${daemon_pid})"
  echo "[run_workers] 日志: ${LOG_DIR}/run_workers-manager.out"
  echo "[run_workers] 使用 --stop 停止全部进程"
  exit 0
fi

# ── Colour helpers (no-op when not a tty) ──────────────────
if [[ -t 1 ]]; then
  CLR_RESET='\033[0m'
  CLR_RED='\033[0;31m'
  CLR_GREEN='\033[0;32m'
  CLR_YELLOW='\033[0;33m'
  CLR_BLUE='\033[0;34m'
  CLR_MAGENTA='\033[0;35m'
  CLR_CYAN='\033[0;36m'
  CLR_WHITE='\033[0;37m'
else
  CLR_RESET='' CLR_RED='' CLR_GREEN='' CLR_YELLOW=''
  CLR_BLUE='' CLR_MAGENTA='' CLR_CYAN='' CLR_WHITE=''
fi

# Map process names → colours
declare -A NAME_CLR=(
  [data]="$CLR_CYAN"
  [backfill]="$CLR_MAGENTA"
  [signal]="$CLR_YELLOW"
  [analysis]="$CLR_GREEN"
  [beat]="$CLR_BLUE"
  [flower]="$CLR_RED"
  [watchdog]="$CLR_WHITE"
)

# ── Logging ────────────────────────────────────────────────
log() {
  # log <name> <message...>
  local name="$1"; shift
  local ts
  ts="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  local clr="${NAME_CLR[$name]:-$CLR_RESET}"
  printf "${clr}%s [run_workers/%s]${CLR_RESET} %s\n" "$ts" "$name" "$*"
}

log_to_file() {
  # log_to_file <name> <message...>
  local name="$1"; shift
  local ts
  ts="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  printf "%s [run_workers/%s] %s\n" "$ts" "$name" "$*" >> "$LOG_DIR/celery-${name}.log"
}

log_both() {
  log "$@"
  log_to_file "$@"
}

# ── PID tracking ───────────────────────────────────────────
# Associative arrays: name → current child PID, name → wrapper PID
declare -A CHILD_PIDS=()
declare -A WRAPPER_PIDS=()

write_pid_file() {
  local name="$1" pid="$2"
  echo "$pid" > "$LOG_DIR/${name}.pid"
}

remove_pid_file() {
  local name="$1"
  rm -f "$LOG_DIR/${name}.pid"
}

cleanup_stale_pids() {
  log main "检查并清理过期 PID 文件..."
  for pidfile in "$LOG_DIR"/*.pid; do
    [[ -f "$pidfile" ]] || continue
    [[ "$(basename "$pidfile")" == "run_workers-manager.pid" ]] && continue
    local old_pid
    old_pid="$(cat "$pidfile" 2>/dev/null)" || continue
    local base
    base="$(basename "$pidfile" .pid)"
    if kill -0 "$old_pid" 2>/dev/null; then
      log main "发现过期进程 ${base} (pid=${old_pid})，正在终止..."
      kill "$old_pid" 2>/dev/null || true
      sleep 1
      kill -0 "$old_pid" 2>/dev/null && kill -9 "$old_pid" 2>/dev/null || true
    fi
    rm -f "$pidfile"
  done
}

# ── Auto-restart wrapper ──────────────────────────────────
run_with_restart() {
  # run_with_restart <name> <command...>
  local name="$1"; shift
  local backoff=0
  local restart_count=0
  local last_start=0

  while true; do
    last_start="$(date +%s)"
    log_both "$name" "启动进程 (restart_count=${restart_count})..."

    # Launch the actual process
    "$@" &
    local child_pid=$!
    CHILD_PIDS[$name]=$child_pid
    write_pid_file "$name" "$child_pid"
    log_both "$name" "进程已启动 pid=${child_pid}"

    # Wait for it to exit
    set +e
    wait "$child_pid"
    local exit_code=$?
    set -e

    # If we are shutting down, break out of the loop
    if [[ "${SHUTTING_DOWN:-0}" == "1" ]]; then
      log_both "$name" "关闭中，不再重启"
      break
    fi

    local now
    now="$(date +%s)"
    local elapsed=$(( now - last_start ))

    restart_count=$(( restart_count + 1 ))
    log_both "$name" "进程退出 exit_code=${exit_code} (运行了 ${elapsed}s)，第 ${restart_count} 次重启"

    # Reset backoff if the process ran successfully for >=60s
    if (( elapsed >= 60 )); then
      backoff=0
    else
      if (( backoff == 0 )); then
        backoff=2
      else
        backoff=$(( backoff * 2 ))
      fi
      if (( backoff > RESTART_MAX_BACKOFF )); then
        backoff=$RESTART_MAX_BACKOFF
      fi
    fi

    if (( backoff > 0 )); then
      log_both "$name" "等待 ${backoff}s 后重启..."
      sleep "$backoff"
    fi
  done
}

# ── Health-check watchdog ──────────────────────────────────
declare -A HEALTH_FAIL_COUNT=()

health_watchdog() {
  # Initialise failure counters
  for w in "${ACTIVE_WORKERS[@]}"; do
    HEALTH_FAIL_COUNT[$w]=0
  done

  while true; do
    sleep "$HEALTH_CHECK_INTERVAL"

    # If shutting down, exit the loop
    [[ "${SHUTTING_DOWN:-0}" == "1" ]] && break

    log watchdog "执行健康检查..."

    local ping_output
    ping_output="$(${CELERY_CMD} inspect ping 2>&1)" || true

    for w in "${ACTIVE_WORKERS[@]}"; do
      local worker_id="${w}@"
      if echo "$ping_output" | grep -q "${worker_id}"; then
        # Worker responded
        if (( HEALTH_FAIL_COUNT[$w] > 0 )); then
          log_both watchdog "${w} worker 恢复响应"
        fi
        HEALTH_FAIL_COUNT[$w]=0
      else
        HEALTH_FAIL_COUNT[$w]=$(( HEALTH_FAIL_COUNT[$w] + 1 ))
        local fails=${HEALTH_FAIL_COUNT[$w]}
        log_both watchdog "${w} worker 未响应 (连续失败 ${fails}/${HEALTH_CHECK_FAILURES})"

        if (( fails >= HEALTH_CHECK_FAILURES )); then
          local pid="${CHILD_PIDS[$w]:-}"
          if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            log_both watchdog "${w} worker 连续 ${HEALTH_CHECK_FAILURES} 次未响应，强制终止 pid=${pid}"
            kill -9 "$pid" 2>/dev/null || true
          fi
          HEALTH_FAIL_COUNT[$w]=0
        fi
      fi
    done
  done
}

# ── Graceful shutdown ──────────────────────────────────────
SHUTTING_DOWN=0

cleanup() {
  # Guard against re-entry
  [[ "${SHUTTING_DOWN}" == "1" ]] && return
  SHUTTING_DOWN=1

  log main "────────────────────────────────────────"
  log main "开始优雅关闭..."

  # Phase 1: SIGTERM all child processes
  local all_pids=()
  for name in "${!CHILD_PIDS[@]}"; do
    local pid="${CHILD_PIDS[$name]}"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      log main "发送 SIGTERM → ${name} (pid=${pid})"
      kill "$pid" 2>/dev/null || true
      all_pids+=("$pid")
    fi
  done

  # Also terminate the watchdog
  if [[ -n "${WATCHDOG_PID:-}" ]] && kill -0 "$WATCHDOG_PID" 2>/dev/null; then
    kill "$WATCHDOG_PID" 2>/dev/null || true
  fi

  # Phase 2: Wait up to SHUTDOWN_GRACE seconds
  if (( ${#all_pids[@]} > 0 )); then
    log main "等待进程退出 (最多 ${SHUTDOWN_GRACE}s)..."
    local deadline=$(( $(date +%s) + SHUTDOWN_GRACE ))
    local remaining=("${all_pids[@]}")

    while (( ${#remaining[@]} > 0 )) && (( $(date +%s) < deadline )); do
      local still_alive=()
      for pid in "${remaining[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
          still_alive+=("$pid")
        fi
      done
      remaining=("${still_alive[@]}")
      if (( ${#remaining[@]} > 0 )); then
        sleep 1
      fi
    done

    # Phase 3: SIGKILL stragglers
    if (( ${#remaining[@]} > 0 )); then
      log main "强制终止剩余进程..."
      for pid in "${remaining[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
          log main "发送 SIGKILL → pid=${pid}"
          kill -9 "$pid" 2>/dev/null || true
        fi
      done
    fi
  fi

  # Terminate wrapper sub-shells
  for name in "${!WRAPPER_PIDS[@]}"; do
    local wpid="${WRAPPER_PIDS[$name]}"
    kill "$wpid" 2>/dev/null || true
  done

  # Clean up PID files
  for name in "${!CHILD_PIDS[@]}"; do
    remove_pid_file "$name"
  done
  remove_pid_file watchdog
  rm -f "$MANAGER_PID_FILE"

  log main "所有进程已停止，清理完成。"
}

trap cleanup INT TERM EXIT

# ── Main ───────────────────────────────────────────────────
log main "════════════════════════════════════════"
log main "  Celery Worker Manager — algo-trader"
log main "════════════════════════════════════════"
log main "LOG_DIR=${LOG_DIR}"
log main "RESTART_MAX_BACKOFF=${RESTART_MAX_BACKOFF}s"
log main "HEALTH_CHECK_INTERVAL=${HEALTH_CHECK_INTERVAL}s"
log main "HEALTH_CHECK_FAILURES=${HEALTH_CHECK_FAILURES}"
log main "SHUTDOWN_GRACE=${SHUTDOWN_GRACE}s"
log main "WORKERS=${ACTIVE_WORKERS[*]}"
log main "LOG_LEVEL=${LOG_LEVEL}"
log main "ENABLE_FLOWER=${ENABLE_FLOWER}"
log main "DAEMON_MODE=${DAEMON_MODE}"
[[ "$ENABLE_FLOWER" == "1" ]] && log main "FLOWER_PORT=${FLOWER_PORT}"
log main "────────────────────────────────────────"

# Clean up stale PID files from previous runs
cleanup_stale_pids

# Register current manager PID (foreground runtime)
echo "$$" > "$MANAGER_PID_FILE"

# ── Start workers (only those in ACTIVE_WORKERS) ──────────

for queue in "${ACTIVE_WORKERS[@]}"; do
  run_with_restart "$queue" \
    $CELERY_CMD worker -Q "$queue" -n "${queue}@%h" \
    --loglevel="$LOG_LEVEL" --logfile="$LOG_DIR/celery-${queue}.log" &
  WRAPPER_PIDS[$queue]=$!
done

run_with_restart beat \
  $CELERY_CMD beat \
  --loglevel="$LOG_LEVEL" --logfile="$LOG_DIR/celery-beat.log" &
WRAPPER_PIDS[beat]=$!

# ── Flower (optional) ─────────────────────────────────────
if [[ "$ENABLE_FLOWER" == "1" ]]; then
  # Delay Flower start so workers have time to register with the broker
  log main "等待 5s 让 workers 注册到 broker..."
  sleep 5
  run_with_restart flower \
    $CELERY_CMD flower --port="$FLOWER_PORT" \
    --inspect_timeout=10000 &
  WRAPPER_PIDS[flower]=$!
  log main "Flower 已启动: http://localhost:${FLOWER_PORT}"
fi

# ── Health-check watchdog ──────────────────────────────────
health_watchdog &
WATCHDOG_PID=$!
write_pid_file watchdog "$WATCHDOG_PID"
log main "健康检查 watchdog 已启动 pid=${WATCHDOG_PID}"

log main "────────────────────────────────────────"
log main "所有进程已启动，按 Ctrl+C 停止全部"
log main "────────────────────────────────────────"

# Wait for all background jobs (wrappers + watchdog)
wait
