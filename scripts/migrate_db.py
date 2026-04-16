"""Migrate all data/schema from source DBs to target DBs.

This script migrates both:
1) TimescaleDB database (time-series tables)
2) PostgreSQL business database

Implementation uses native PostgreSQL tools for fidelity:
- pg_dump (custom format)
- pg_restore (--clean --if-exists)

Examples:
    uv run python -m scripts.migrate_db \
      --target-timescale-url postgresql://trader:pwd@new-host:5432/algo_trader \
      --target-postgres-url postgresql://trader:pwd@new-host:5432/algo_trader_biz

    # Explicit source URLs (optional; defaults to current settings)
    uv run python -m scripts.migrate_db \
      --source-timescale-url postgresql://trader:pwd@old-host:5432/algo_trader \
      --source-postgres-url postgresql://trader:pwd@old-host:5432/algo_trader_biz \
      --target-timescale-url postgresql://trader:pwd@new-host:5432/algo_trader \
      --target-postgres-url postgresql://trader:pwd@new-host:5432/algo_trader_biz
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from shared.config import get_settings


def _to_pg_driver_url(url: str) -> str:
    """Convert SQLAlchemy async URL to plain PostgreSQL URL for CLI tools."""
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _mask_db_url(url: str) -> str:
    parsed = urlparse(url)
    user = parsed.username or "<user>"
    host = parsed.hostname or "<host>"
    port = parsed.port or 5432
    db = (parsed.path or "/").lstrip("/") or "<db>"
    return f"{user}@{host}:{port}/{db}"


def _require_command(cmd: str) -> None:
    if shutil.which(cmd) is None:
        raise RuntimeError(f"Missing required command: {cmd}")


def _run(cmd: list[str], *, label: str) -> None:
    print(f"[migrate_db] {label}")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}") from exc


def _migrate_one_db(name: str, source_url: str, target_url: str, dump_dir: Path) -> None:
    dump_file = dump_dir / f"{name}.dump"

    print(
        "[migrate_db] "
        f"Migrate {name}: {_mask_db_url(source_url)} -> {_mask_db_url(target_url)}"
    )

    _run(
        [
            "pg_dump",
            "--format=custom",
            "--no-owner",
            "--no-privileges",
            "--dbname",
            source_url,
            "--file",
            str(dump_file),
        ],
        label=f"Dumping {name} from source",
    )

    _run(
        [
            "pg_restore",
            "--clean",
            "--if-exists",
            "--no-owner",
            "--no-privileges",
            "--exit-on-error",
            "--single-transaction",
            "--dbname",
            target_url,
            str(dump_file),
        ],
        label=f"Restoring {name} into target",
    )


def parse_args() -> argparse.Namespace:
    settings = get_settings()

    parser = argparse.ArgumentParser(
        description="Migrate Timescale + PostgreSQL data/schema from source to target databases"
    )
    parser.add_argument(
        "--source-timescale-url",
        default=_to_pg_driver_url(settings.infra.database.timescale_url),
        help="Source Timescale DB URL (default: from current settings)",
    )
    parser.add_argument(
        "--source-postgres-url",
        default=_to_pg_driver_url(settings.infra.database.postgres_url),
        help="Source business PostgreSQL DB URL (default: from current settings)",
    )
    parser.add_argument(
        "--target-timescale-url",
        required=True,
        help="Target Timescale DB URL",
    )
    parser.add_argument(
        "--target-postgres-url",
        required=True,
        help="Target business PostgreSQL DB URL",
    )
    parser.add_argument(
        "--keep-dumps",
        action="store_true",
        help="Keep generated dump files in a temporary folder and print the path",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    _require_command("pg_dump")
    _require_command("pg_restore")

    target_timescale = _to_pg_driver_url(args.target_timescale_url)
    target_postgres = _to_pg_driver_url(args.target_postgres_url)

    # Timescale extension must already exist on the target Timescale database.
    with tempfile.TemporaryDirectory(prefix="algo_migrate_") as tmp:
        dump_dir = Path(tmp)

        _migrate_one_db(
            name="timescale",
            source_url=_to_pg_driver_url(args.source_timescale_url),
            target_url=target_timescale,
            dump_dir=dump_dir,
        )
        _migrate_one_db(
            name="postgres",
            source_url=_to_pg_driver_url(args.source_postgres_url),
            target_url=target_postgres,
            dump_dir=dump_dir,
        )

        if args.keep_dumps:
            keep_path = Path.cwd() / f"migrate_dump_{dump_dir.name}"
            keep_path.mkdir(parents=True, exist_ok=True)
            shutil.copy2(dump_dir / "timescale.dump", keep_path / "timescale.dump")
            shutil.copy2(dump_dir / "postgres.dump", keep_path / "postgres.dump")
            print(f"[migrate_db] dump files kept at: {keep_path}")

    print("[migrate_db] migration finished successfully")


if __name__ == "__main__":
    main()
