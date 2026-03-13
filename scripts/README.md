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

## 3) Start Celery workers and beat

```bash
bash scripts/run_workers.sh
```

- Internally uses `uv run celery ...` for each process.
- Current worker queues started by this script:
  - `data`
  - `backfill`
  - `signal`
  - `analysis`
  - `beat` scheduler

## Notes

- `init_db` and `seed_watchlist` are idempotent and safe to run repeatedly.
- Stop workers with `Ctrl+C` (the script traps and cleans up child processes).
