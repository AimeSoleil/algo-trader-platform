# Scripts Usage

Run commands from the repository root:

```bash
cd /Users/julian/GitHub/algo-trader-platform
```

## Prerequisites

- Activate the environment and install dependencies (recommended):

```bash
source .venv/bin/activate
uv sync --all-packages
```

- Ensure infrastructure is up first: TimescaleDB/PostgreSQL, Redis, RabbitMQ.

## 1) Initialize DB schemas + Timescale hypertables

```bash
uv run python -m scripts.init_db
```

Optional destructive modes:

```bash
uv run python -m scripts.init_db --truncate-all --yes
uv run python -m scripts.init_db --drop-all --yes
```

## 2) Seed/update `watchlist_symbols` from `settings.watchlist`

```bash
uv run python -m scripts.seed_watchlist
```

## 3) Start Celery workers and beat (local development)

```bash
# Basic: start all workers + beat
bash scripts/run_workers.sh

# With Flower monitoring UI
bash scripts/run_workers.sh --with-flower
# or
ENABLE_FLOWER=1 bash scripts/run_workers.sh
# or start beat only
uv run celery -A shared.celery_app.celery_app beat --loglevel=INFO
```

### Features

- **Auto-restart**: Crashed workers automatically restart with exponential backoff (2s → 60s)
- **Health watchdog**: Background process pings workers every 30s; kills unresponsive ones after 3 failures
- **Graceful shutdown**: SIGTERM → 30s grace → SIGKILL (respects `task_acks_late`)
- **PID management**: PID files in `logs/`, stale process cleanup on startup
- **Flower** (optional): Celery monitoring UI at `http://localhost:5555`

### Configuration (environment variables)

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_DIR` | `logs` | Log and PID file directory |
| `RESTART_MAX_BACKOFF` | `60` | Max backoff seconds between restarts |
| `HEALTH_CHECK_INTERVAL` | `30` | Seconds between health checks |
| `HEALTH_CHECK_FAILURES` | `3` | Consecutive failures before force kill |
| `SHUTDOWN_GRACE` | `30` | Seconds to wait before SIGKILL |
| `ENABLE_FLOWER` | `0` | Set to `1` to start Flower |
| `FLOWER_PORT` | `5555` | Flower listen port |

### Worker queues

| Queue | Worker name | Description |
|-------|-------------|-------------|
| `data` | `data@hostname` | 行情数据采集与入库 |
| `backfill` | `backfill@hostname` | 缺口检测与历史回填 |
| `signal` | `signal@hostname` | 特征计算与信号生成 |
| `analysis` | `analysis@hostname` | LLM 蓝图生成 |

## 4) Production: Docker-based Celery workers

For production deployment, use Docker Compose with the `worker` profile:

```bash
# Start all Celery workers + beat + Flower
docker compose --profile worker up -d

# Start full stack (infrastructure + API services + workers)
docker compose --profile app --profile worker up -d

# Check status
docker compose --profile worker ps

# View logs
docker compose logs -f celery-data celery-signal

# Scale a specific worker
docker compose --profile worker up -d --scale celery-data=2

# Restart a specific worker
docker compose restart celery-signal
```

- All workers use a single universal image (`services/celery_worker/Dockerfile`)
- Docker `restart: unless-stopped` handles automatic crash recovery
- Each worker has a dedicated healthcheck (`celery inspect ping -d ...`)
- Flower dashboard: `http://localhost:5555` (requires `FLOWER_USER`/`FLOWER_PASSWORD`)

## Notes

- `init_db` and `seed_watchlist` are idempotent and safe to run repeatedly.
- **Local dev**: Use `run_workers.sh` — no Docker needed for workers.
- **Production**: Use `docker compose --profile worker up -d` — automatic restart, healthchecks, log aggregation.
- Stop local workers with `Ctrl+C` (graceful two-phase shutdown).
- Recommended: configure `logrotate` for `logs/celery-*.log` in long-running local setups.

## 5) Get one specific option contract quote (OCC symbol)

```bash
uv run python -m scripts.get_option_contract META260323C00607500
```

- Input format: `UNDERLYING + YYMMDD + C/P + strike*1000(8 digits)`
- Example parse: `META260323C00607500` = `META` + `2026-03-23` + `CALL` + `607.5`
- Data source: `yfinance` live option chain (not historical chain replay)
- Output includes both `quote` (normalized) and `raw_quote` (raw yfinance row JSON)
