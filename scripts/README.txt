Scripts usage notes
===================

Run from repository root: /Users/julian/GitHub/algo-trader-platform

1) Initialize databases and Timescale hypertables
   python scripts/init_db.py

2) Seed/update watchlist_symbols table from settings.watchlist
   python scripts/seed_watchlist.py

3) Start Celery workers (data/backfill/signal/analysis) and beat
   bash scripts/run_workers.sh

Notes:
- Scripts are safe to run repeatedly.
- Ensure PostgreSQL/TimescaleDB, Redis, and RabbitMQ are running first.
- Stop workers with Ctrl+C.
