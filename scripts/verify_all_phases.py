"""Verify all Phase 1-7 files compile correctly."""
import ast
import sys

files = [
    "services/data_service/app/filters/__init__.py",
    "services/data_service/app/filters/base.py",
    "services/data_service/app/filters/option_filters.py",
    "services/data_service/app/filters/stock_filters.py",
    "services/data_service/app/fetchers/option_fetcher.py",
    "services/data_service/app/fetchers/greeks.py",
    "services/data_service/app/converters.py",
    "services/data_service/app/storage.py",
    "services/data_service/app/scheduler.py",
    "services/data_service/app/tasks.py",
    "services/signal_service/app/filters/__init__.py",
    "services/signal_service/app/filters/base.py",
    "services/signal_service/app/filters/option_filters.py",
    "services/signal_service/app/filters/stock_filters.py",
    "services/signal_service/app/data_loaders.py",
    "services/signal_service/app/indicators/option_indicators.py",
    "services/signal_service/app/tasks.py",
    "shared/models/option.py",
    "shared/models/signal.py",
    "shared/config/settings.py",
    "shared/db/tables.py",
    "scripts/init_db.py",
]

ok = 0
errors = []
for f in files:
    try:
        with open(f) as fh:
            ast.parse(fh.read())
        ok += 1
    except SyntaxError as e:
        errors.append(f"{f}: {e}")
        print(f"FAIL {f}: {e}")

print(f"\n{ok}/{len(files)} files OK")
if errors:
    sys.exit(1)
