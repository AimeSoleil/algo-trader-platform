#!/usr/bin/env bash
###############################################################################
# run_workers.sh — Enhanced Celery worker manager for local development
#
# Capabilities:
#   • Starts data / signal / analysis workers + beat (+ flower)
#   • Auto-restarts crashed processes with exponential backoff (2s → 60s)
#   • Background health-check watchdog (celery inspect ping)
#   • Graceful two-phase shutdown: SIGTERM → grace period → SIGKILL
#   • PID file management with stale-process cleanup on startup
#   • Coloured, timestamped log output
#
# Configuration (environment variables):
#   LOG_DIR                 Log / PID directory          (default: logs)
#   DAEMON_MODE             Start manager in background  (default: 1)
#   ENV_FILE                Env file for local dev       (default: .env.local -> .env)
#   RESTART_MAX_BACKOFF     Max backoff seconds          (default: 60)
#   HEALTH_CHECK_INTERVAL   Seconds between pings        (default: 60)
#   HEALTH_CHECK_TIMEOUT    Inspect ping timeout seconds (default: 25)
#   HEALTH_CHECK_FAILURES   Consecutive fails before kill (default: 3)
#   SHUTDOWN_GRACE          Seconds before SIGKILL        (default: 30)
#   WORKERS                 Comma-separated worker list  (default: all)
#                           Choices: data,signal,analysis
#   ENABLE_FLOWER           Set to 1 to start Flower     (default: 0)
#   FLOWER_PORT             Flower listen port           (default: 5555)
#   FLOWER_INSPECT_TIMEOUT  Flower inspect timeout ms    (default: 60000)
#   LOG_LEVEL               Celery worker log level      (default: INFO)
#                           Choices: DEBUG,INFO,WARNING,ERROR,CRITICAL
#
# Usage:
#   ./scripts/run_workers.sh [--with-flower] [--workers=data,signal] [--loglevel DEBUG]
#   ./scripts/run_workers.sh --env-file .env.local
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
#   signal     盘后特征计算与信号生成     (queue: signal)
#   analysis   LLM 蓝图生成与分析        (queue: analysis)
###############################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Configuration ──────────────────────────────────────────
LOG_DIR="${LOG_DIR:-logs}"
DAEMON_MODE="${DAEMON_MODE:-1}"
ENV_FILE="${ENV_FILE:-}"
RESTART_MAX_BACKOFF="${RESTART_MAX_BACKOFF:-60}"
HEALTH_CHECK_INTERVAL="${HEALTH_CHECK_INTERVAL:-60}"
HEALTH_CHECK_TIMEOUT="${HEALTH_CHECK_TIMEOUT:-25}"
HEALTH_CHECK_FAILURES="${HEALTH_CHECK_FAILURES:-3}"
SHUTDOWN_GRACE="${SHUTDOWN_GRACE:-30}"
WORKERS="${WORKERS:-all}"
ENABLE_FLOWER="${ENABLE_FLOWER:-0}"
FLOWER_PORT="${FLOWER_PORT:-5555}"
FLOWER_INSPECT_TIMEOUT="${FLOWER_INSPECT_TIMEOUT:-60000}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"
WORKER_HOST="${WORKER_HOST:-$(hostname -s 2>/dev/null || hostname)}"

FOREGROUND=0

ALL_QUEUES=(data signal analysis)

# Worker descriptions (used by --list and --help)
declare -A WORKER_DESC=(
  [data]="行情数据采集与盘后批量入库"
  [signal]="盘后特征计算与信号生成"
  [analysis]="LLM 蓝图生成与分析"
)

resolve_env_file() {
  if [[ -n "$ENV_FILE" ]]; then
    if [[ "$ENV_FILE" = /* ]]; then
      printf '%s\n' "$ENV_FILE"
    else
      printf '%s\n' "$REPO_ROOT/$ENV_FILE"
    fi
    return
  fi

  if [[ -f "$REPO_ROOT/.env.local" ]]; then
    printf '%s\n' "$REPO_ROOT/.env.local"
    return
  fi

  if [[ -f "$REPO_ROOT/.env" ]]; then
    printf '%s\n' "$REPO_ROOT/.env"
    return
  fi

  printf '\n'
}

load_env_file() {
  local env_path="$1"
  [[ -n "$env_path" ]] || return 0

  if [[ ! -f "$env_path" ]]; then
    echo "[run_workers] 环境文件不存在: $env_path" >&2
    exit 1
  fi

  set -a
  # shellcheck disable=SC1090
  . "$env_path"
  set +a
  ENV_FILE="$env_path"
}

read_pid_file() {
  local name="$1"
  local pidfile="$LOG_DIR/${name}.pid"
  [[ -f "$pidfile" ]] || return 1
  cat "$pidfile" 2>/dev/null || true
}

read_wrapper_pid_file() {
  local name="$1"
  local pidfile="$LOG_DIR/${name}.wrapper.pid"
  [[ -f "$pidfile" ]] || return 1
  cat "$pidfile" 2>/dev/null || true
}

is_pid_running() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

collect_descendant_pids() {
  local parent_pid="$1"
  local child_pid

  while IFS= read -r child_pid; do
    [[ -n "$child_pid" ]] || continue
    collect_descendant_pids "$child_pid"
    printf '%s\n' "$child_pid"
  done < <(pgrep -P "$parent_pid" 2>/dev/null || true)
}

worker_node_name() {
  local worker="$1"
  printf '%s@%s\n' "$worker" "$WORKER_HOST"
}

build_destinations() {
  local names=("$@")
  local destinations=""
  local name

  for name in "${names[@]}"; do
    [[ -n "$destinations" ]] && destinations+="," 
    destinations+="$(worker_node_name "$name")"
  done

  printf '%s\n' "$destinations"
}

run_inspect_ping() {
  local destinations="$1"
  "${CELERY_CMD[@]}" inspect ping -t "$HEALTH_CHECK_TIMEOUT" -d "$destinations" 2>&1 || true
}

ping_output_has_worker() {
  local ping_output="$1"
  local worker="$2"
  echo "$ping_output" | grep -q "${worker}@"
}

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
  echo ""

  # ── Process status ──
  if [[ -f "$manager_pid_file" ]]; then
    local manager_pid
    manager_pid="$(cat "$manager_pid_file" 2>/dev/null || true)"
    if is_pid_running "$manager_pid"; then
      echo "  manager   : RUNNING (pid=${manager_pid})"
    else
      echo "  manager   : STALE_PID_FILE"
    fi
  else
    echo "  manager   : STOPPED"
  fi

  local names=("${ALL_QUEUES[@]}" beat flower watchdog)
  local name pid state width pad
  for name in "${names[@]}"; do
    pid="$(read_pid_file "$name" || true)"
    state="STOPPED"
    if [[ -n "$pid" ]]; then
      if is_pid_running "$pid"; then
        state="RUNNING"
      else
        state="STALE_PID_FILE"
      fi
    fi
    width=$((10 - ${#name}))
    (( width < 1 )) && width=1
    pad="$(printf '%*s' "$width" '')"
    printf '  %s%s: %s' "$name" "$pad" "$state"
    [[ -n "$pid" ]] && printf ' (pid=%s)' "$pid"
    printf '\n'
  done

  # ── Log files ──
  echo ""
  echo "  日志文件:"
  printf '    %-10s → %s\n' manager "${LOG_DIR}/run_workers-manager.out"
  for q in "${ALL_QUEUES[@]}"; do
    printf '    %-10s → %s\n' "$q" "${LOG_DIR}/celery-${q}.log"
  done
  printf '    %-10s → %s\n' beat "${LOG_DIR}/celery-beat.log"
}

run_diag() {
  mkdir -p "$LOG_DIR"

  echo "[run_workers] Diag"
  echo "  LOG_DIR=${LOG_DIR}"
  echo "  WORKERS=${ACTIVE_WORKERS[*]}"
  echo "  WORKER_HOST=${WORKER_HOST}"
  echo "  HEALTH_CHECK_INTERVAL=${HEALTH_CHECK_INTERVAL}s"
  echo "  HEALTH_CHECK_TIMEOUT=${HEALTH_CHECK_TIMEOUT}s"
  echo "  HEALTH_CHECK_FAILURES=${HEALTH_CHECK_FAILURES}"
  echo ""

  echo "  worker nodes:"
  local pid state
  for w in "${ACTIVE_WORKERS[@]}"; do
    pid="$(read_pid_file "$w" || true)"
    state="STOPPED"
    if [[ -n "$pid" ]]; then
      if is_pid_running "$pid"; then
        state="RUNNING"
      else
        state="STALE_PID_FILE"
      fi
    fi
    printf '    %-24s %s' "$(worker_node_name "$w")" "$state"
    [[ -n "$pid" ]] && printf ' (pid=%s)' "$pid"
    printf '\n'
  done
  echo ""

  echo "  inspect ping:"
  local destinations
  destinations="$(build_destinations "${ACTIVE_WORKERS[@]}")"
  run_inspect_ping "$destinations"
  echo ""

  echo "  watchdog log tail:"
  local watchdog_log="$LOG_DIR/celery-watchdog.log"
  if [[ -f "$watchdog_log" ]]; then
    tail -n 20 "$watchdog_log"
  else
    echo "    (no watchdog log yet)"
  fi
}

# Accept CLI flags (supports both --param=value and --param value)
while (( $# > 0 )); do
  case "$1" in
    --with-flower)
      ENABLE_FLOWER=1; shift ;;
    --workers=*)
      WORKERS="${1#--workers=}"; shift ;;
    --workers)
      WORKERS="${2:?'--workers requires a value'}"; shift 2 ;;
    --env-file=*)
      ENV_FILE="${1#--env-file=}"; shift ;;
    --env-file)
      ENV_FILE="${2:?'--env-file requires a value'}"; shift 2 ;;
    --loglevel=*)
      LOG_LEVEL="${1#--loglevel=}"; shift ;;
    --loglevel)
      LOG_LEVEL="${2:?'--loglevel requires a value'}"; shift 2 ;;
    --flower-port=*)
      FLOWER_PORT="${1#--flower-port=}"; shift ;;
    --flower-port)
      FLOWER_PORT="${2:?'--flower-port requires a value'}"; shift 2 ;;
    --foreground)
      FOREGROUND=1; shift ;;
    --stop)
      # ── Stop all managed processes ────────────────────────
      echo "[run_workers] Stopping all Celery processes..."
      mkdir -p "$LOG_DIR"
      MANAGER_PID_FILE="$LOG_DIR/run_workers-manager.pid"
      stopped=0

      # 0) Stop manager daemon first (if running)
      if [[ -f "$MANAGER_PID_FILE" ]]; then
        manager_pid="$(cat "$MANAGER_PID_FILE" 2>/dev/null || true)"
        if is_pid_running "$manager_pid"; then
          mapfile -t manager_descendants < <(collect_descendant_pids "$manager_pid")
          if (( ${#manager_descendants[@]} > 0 )); then
            echo "  SIGTERM → manager descendants (${#manager_descendants[@]} processes)"
            kill "${manager_descendants[@]}" 2>/dev/null || true
            stopped=$((stopped + ${#manager_descendants[@]}))
          fi
          echo "  SIGTERM → manager (pid=${manager_pid})"
          kill "$manager_pid" 2>/dev/null || true
          stopped=$((stopped + 1))
        fi
        rm -f "$MANAGER_PID_FILE"
      fi

      # 1) Kill processes tracked by PID files
      while IFS= read -r name; do
        [[ -n "$name" ]] || continue
        wrapper_pid="$(read_wrapper_pid_file "$name" || true)"
        if is_pid_running "$wrapper_pid"; then
          echo "  SIGTERM → ${name} wrapper (pid=${wrapper_pid})"
          kill "$wrapper_pid" 2>/dev/null || true
          stopped=$((stopped + 1))
        fi
        pid="$(read_pid_file "$name" || true)"
        if is_pid_running "$pid"; then
          echo "  SIGTERM → ${name} (pid=${pid})"
          kill "$pid" 2>/dev/null || true
          stopped=$((stopped + 1))
        fi
        rm -f "$LOG_DIR/${name}.pid"
        rm -f "$LOG_DIR/${name}.wrapper.pid"
      done < <(printf '%s\n' "${ALL_QUEUES[@]}" beat flower watchdog)

      # 2) Also ask Celery to shut down any workers it knows about
      uv run celery -A shared.celery_app.celery_app control shutdown 2>/dev/null || true

      # 3) Kill any lingering celery processes owned by this user
      pkill -f "celery.*shared.celery_app" 2>/dev/null || true

      # Brief grace period then force-kill survivors
      sleep 2
      if [[ -n "${manager_pid:-}" ]] && is_pid_running "$manager_pid"; then
        mapfile -t manager_descendants < <(collect_descendant_pids "$manager_pid")
        if (( ${#manager_descendants[@]} > 0 )); then
          kill -9 "${manager_descendants[@]}" 2>/dev/null || true
        fi
        kill -9 "$manager_pid" 2>/dev/null || true
      fi
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
    --diag)
      # lightweight runtime diagnostics for worker node names / inspect / watchdog
      DIAG_ONLY=1
      shift
      ;;
    --list|--help|-h)
      show_workers
      exit 0
      ;;
    *)
      echo "[run_workers] 未知参数: $1" >&2
      echo "  用法: ./scripts/run_workers.sh [--workers=data,signal] [--env-file=.env.local] [--loglevel=DEBUG] [--with-flower] [--foreground] [--stop] [--status] [--diag]" >&2
      exit 1
      ;;
  esac
done

ENV_FILE="$(resolve_env_file)"
load_env_file "$ENV_FILE"

# Local dev defaults to the configured common logging level when LOG_LEVEL is not set.
LOG_LEVEL="${LOG_LEVEL:-${COMMON__LOGGING__LEVEL:-INFO}}"

# foreground mode overrides daemon setting
if (( FOREGROUND == 1 )); then
  DAEMON_MODE=0
fi

# Flower also polls inspect APIs frequently; avoid watchdog over-polling.
if [[ "$ENABLE_FLOWER" == "1" ]] && (( HEALTH_CHECK_INTERVAL < 60 )); then
  HEALTH_CHECK_INTERVAL=60
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

CELERY_CMD=(uv run celery -A shared.celery_app.celery_app)

if [[ "${DIAG_ONLY:-0}" == "1" ]]; then
  run_diag
  exit 0
fi

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

  script_path="$SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")"
  LOG_DIR="$LOG_DIR" \
  ENV_FILE="$ENV_FILE" \
  RESTART_MAX_BACKOFF="$RESTART_MAX_BACKOFF" \
  HEALTH_CHECK_INTERVAL="$HEALTH_CHECK_INTERVAL" \
  HEALTH_CHECK_TIMEOUT="$HEALTH_CHECK_TIMEOUT" \
  HEALTH_CHECK_FAILURES="$HEALTH_CHECK_FAILURES" \
  SHUTDOWN_GRACE="$SHUTDOWN_GRACE" \
  WORKERS="$WORKERS" \
  ENABLE_FLOWER="$ENABLE_FLOWER" \
  FLOWER_PORT="$FLOWER_PORT" \
  FLOWER_INSPECT_TIMEOUT="$FLOWER_INSPECT_TIMEOUT" \
  LOG_LEVEL="$LOG_LEVEL" \
  WORKER_HOST="$WORKER_HOST" \
  DAEMON_MODE=0 \
  nohup "$script_path" --foreground >> "$LOG_DIR/run_workers-manager.out" 2>&1 &

  daemon_pid=$!
  echo "$daemon_pid" > "$MANAGER_PID_FILE"
  echo "[run_workers] manager 后台启动成功 (pid=${daemon_pid})"
  echo ""
  echo "  Workers:  ${ACTIVE_WORKERS[*]}"
  echo "  LogLevel: ${LOG_LEVEL}"
  echo ""
  echo "  日志文件:"
  printf '    %-10s → %s\n' manager "${LOG_DIR}/run_workers-manager.out"
  for _q in "${ACTIVE_WORKERS[@]}"; do
    printf '    %-10s → %s\n' "$_q" "${LOG_DIR}/celery-${_q}.log"
  done
  printf '    %-10s → %s\n' beat "${LOG_DIR}/celery-beat.log"
  if [[ "$ENABLE_FLOWER" == "1" ]]; then
    printf '    %-10s → %s\n' flower "http://localhost:${FLOWER_PORT}"
  fi
  echo ""
  echo "[run_workers] 使用 --stop 停止全部进程"
  echo "[run_workers] 使用 --status 查看运行状态"
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

write_wrapper_pid_file() {
  local name="$1" pid="$2"
  echo "$pid" > "$LOG_DIR/${name}.wrapper.pid"
}

remove_pid_file() {
  local name="$1"
  rm -f "$LOG_DIR/${name}.pid"
}

remove_wrapper_pid_file() {
  local name="$1"
  rm -f "$LOG_DIR/${name}.wrapper.pid"
}

resolve_pid() {
  local name="$1"
  local pid="${CHILD_PIDS[$name]:-}"

  if is_pid_running "$pid"; then
    printf '%s\n' "$pid"
    return
  fi

  pid="$(read_pid_file "$name" || true)"
  if is_pid_running "$pid"; then
    printf '%s\n' "$pid"
    return
  fi

  printf '\n'
}

managed_process_names() {
  local names=("${ACTIVE_WORKERS[@]}" beat)
  if [[ "$ENABLE_FLOWER" == "1" ]]; then
    names+=(flower)
  fi
  printf '%s\n' "${names[@]}"
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

start_managed_process() {
  local name="$1"
  shift
  run_with_restart "$name" "$@" &
  WRAPPER_PIDS[$name]=$!
  write_wrapper_pid_file "$name" "${WRAPPER_PIDS[$name]}"
}

start_worker_process() {
  local queue="$1"
  local extra_args=()

  if [[ "$queue" == "analysis" ]]; then
    extra_args=(--prefetch-multiplier=1)
  fi

  start_managed_process \
    "$queue" \
    "${CELERY_CMD[@]}" worker -Q "$queue" -n "$(worker_node_name "$queue")" \
    "${extra_args[@]}" \
    --loglevel="$LOG_LEVEL" --logfile="$LOG_DIR/celery-${queue}.log"
}

start_beat_process() {
  start_managed_process \
    beat \
    "${CELERY_CMD[@]}" beat \
    --loglevel="$LOG_LEVEL" --logfile="$LOG_DIR/celery-beat.log"
}

start_flower_process() {
  start_managed_process \
    flower \
    env FLOWER_UNAUTHENTICATED_API=true "${CELERY_CMD[@]}" flower --port="$FLOWER_PORT" \
    --inspect_timeout="$FLOWER_INSPECT_TIMEOUT"
}

# ── Health-check watchdog ──────────────────────────────────
declare -A HEALTH_FAIL_COUNT=()
declare -A HEALTH_LAST_SIGNATURE=()
declare -A HEALTH_STUCK_COUNT=()

worker_progress_signature() {
  local name="$1"
  local pid
  pid="$(resolve_pid "$name")"

  if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
    printf 'dead\n'
    return
  fi

  local cpu_time=""
  cpu_time="$(ps -p "$pid" -o cputime= 2>/dev/null | tr -d '[:space:]' || true)"

  local log_path="$LOG_DIR/celery-${name}.log"
  local log_size="0"
  local log_mtime="0"
  if [[ -f "$log_path" ]]; then
    log_size="$(wc -c < "$log_path" 2>/dev/null | tr -d '[:space:]' || printf '0')"
    log_mtime="$(python - <<'PY' "$log_path"
from pathlib import Path
import sys
try:
    print(int(Path(sys.argv[1]).stat().st_mtime))
except FileNotFoundError:
    print(0)
PY
    )"
  fi

  printf '%s|%s|%s|%s\n' "$pid" "$cpu_time" "$log_size" "$log_mtime"
}

health_watchdog() {
  # Initialise failure counters
  for w in "${ACTIVE_WORKERS[@]}"; do
    HEALTH_FAIL_COUNT[$w]=0
    HEALTH_LAST_SIGNATURE[$w]=""
    HEALTH_STUCK_COUNT[$w]=0
  done

  while true; do
    sleep "$HEALTH_CHECK_INTERVAL"

    # If shutting down, exit the loop
    [[ "${SHUTTING_DOWN:-0}" == "1" ]] && break

    log watchdog "执行健康检查..."

    local destinations
    destinations="$(build_destinations "${ACTIVE_WORKERS[@]}")"

    local ping_output
    ping_output="$(run_inspect_ping "$destinations")"

    # Distinguish control-plane timeout from worker failure.
    # If inspect has no replies but all worker processes are still alive,
    # treat this as transient broker/inspect jitter and do not count failures.
    local replied_count=0
    local all_workers_running=1
    for w in "${ACTIVE_WORKERS[@]}"; do
      if ping_output_has_worker "$ping_output" "$w"; then
        replied_count=$(( replied_count + 1 ))
      fi

      local pid_probe
      pid_probe="$(resolve_pid "$w")"
      if [[ -z "$pid_probe" ]] || ! kill -0 "$pid_probe" 2>/dev/null; then
        all_workers_running=0
      fi
    done

    if (( replied_count == 0 )) && (( all_workers_running == 1 )); then
      log_both watchdog "inspect 无回复 (0/${#ACTIVE_WORKERS[@]}) 但 worker 进程均存活；判定为 control-plane 超时，本轮跳过失败计数"
      continue
    fi

    for w in "${ACTIVE_WORKERS[@]}"; do
      if ping_output_has_worker "$ping_output" "$w"; then
        # Worker responded
        if (( HEALTH_FAIL_COUNT[$w] > 0 )); then
          log_both watchdog "${w} worker 恢复响应"
        fi
        HEALTH_FAIL_COUNT[$w]=0
        HEALTH_LAST_SIGNATURE[$w]=""
        HEALTH_STUCK_COUNT[$w]=0
      else
        HEALTH_FAIL_COUNT[$w]=$(( HEALTH_FAIL_COUNT[$w] + 1 ))
        local fails=${HEALTH_FAIL_COUNT[$w]}
        local signature
        signature="$(worker_progress_signature "$w")"
        if [[ "$signature" == "${HEALTH_LAST_SIGNATURE[$w]}" ]]; then
          HEALTH_STUCK_COUNT[$w]=$(( HEALTH_STUCK_COUNT[$w] + 1 ))
        else
          HEALTH_LAST_SIGNATURE[$w]="$signature"
          HEALTH_STUCK_COUNT[$w]=1
        fi
        local stuck=${HEALTH_STUCK_COUNT[$w]}
        log_both watchdog "${w} worker 未响应 (连续失败 ${fails}/${HEALTH_CHECK_FAILURES}, 无进展 ${stuck}/${HEALTH_CHECK_FAILURES})"

        if (( fails >= HEALTH_CHECK_FAILURES )); then
          local pid
          pid="$(resolve_pid "$w")"
          if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            if (( stuck >= HEALTH_CHECK_FAILURES )); then
              log_both watchdog "${w} worker 连续 ${HEALTH_CHECK_FAILURES} 次未响应且 CPU/日志无变化，强制终止 pid=${pid}"
              kill -9 "$pid" 2>/dev/null || true
            else
              log_both watchdog "${w} worker 连续 ${HEALTH_CHECK_FAILURES} 次未响应，但进程仍有 CPU/日志进展，跳过强杀"
            fi
          fi
          HEALTH_FAIL_COUNT[$w]=0
          HEALTH_STUCK_COUNT[$w]=0
          HEALTH_LAST_SIGNATURE[$w]=""
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
  while IFS= read -r name; do
    [[ -n "$name" ]] || continue
    local pid
    pid="$(resolve_pid "$name")"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      log main "发送 SIGTERM → ${name} (pid=${pid})"
      kill "$pid" 2>/dev/null || true
      all_pids+=("$pid")
    fi
  done < <(managed_process_names)

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
  while IFS= read -r name; do
    [[ -n "$name" ]] || continue
    remove_pid_file "$name"
    remove_wrapper_pid_file "$name"
  done < <(managed_process_names)
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
log main "HEALTH_CHECK_TIMEOUT=${HEALTH_CHECK_TIMEOUT}s"
log main "HEALTH_CHECK_FAILURES=${HEALTH_CHECK_FAILURES}"
log main "SHUTDOWN_GRACE=${SHUTDOWN_GRACE}s"
log main "WORKERS=${ACTIVE_WORKERS[*]}"
log main "WORKER_HOST=${WORKER_HOST}"
log main "LOG_LEVEL=${LOG_LEVEL}"
[[ -n "$ENV_FILE" ]] && log main "ENV_FILE=${ENV_FILE}"
log main "ENABLE_FLOWER=${ENABLE_FLOWER}"
log main "DAEMON_MODE=${DAEMON_MODE}"
[[ "$ENABLE_FLOWER" == "1" ]] && log main "FLOWER_PORT=${FLOWER_PORT}"
[[ "$ENABLE_FLOWER" == "1" ]] && log main "FLOWER_INSPECT_TIMEOUT=${FLOWER_INSPECT_TIMEOUT}ms"
log main "────────────────────────────────────────"

# Clean up stale PID files from previous runs
cleanup_stale_pids

# Register current manager PID (foreground runtime)
echo "$$" > "$MANAGER_PID_FILE"

# ── Start workers (only those in ACTIVE_WORKERS) ──────────

for queue in "${ACTIVE_WORKERS[@]}"; do
  start_worker_process "$queue"
done

start_beat_process

# ── Flower (optional) ─────────────────────────────────────
if [[ "$ENABLE_FLOWER" == "1" ]]; then
  # Delay Flower start so workers have time to register with the broker
  log main "等待 5s 让 workers 注册到 broker..."
  sleep 5
  start_flower_process
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
