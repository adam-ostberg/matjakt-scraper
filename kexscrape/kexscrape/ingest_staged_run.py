from __future__ import annotations

import argparse
from pathlib import Path

from .ingestion import IngestionService, read_ndjson_records
from .postgres_repo import PostgresRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest staged ICA NDJSON files into PostgreSQL"
    )
    parser.add_argument("--run-id", required=True, help="Run identifier")
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Path to run directory containing stores/*.ndjson",
    )
    parser.add_argument("--db-dsn", required=True, help="PostgreSQL DSN")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_dir = Path(args.run_dir)
    store_files = sorted((run_dir / "stores").glob("*.ndjson"))

    repository = PostgresRepository(args.db_dsn)
    try:
        repository.apply_schema()
        ingestion = IngestionService(repository)
        records = read_ndjson_records(store_files)
        ingested = ingestion.ingest_records(records, run_id=args.run_id)
        repository.commit()
    except Exception:
        repository.rollback()
        raise
    finally:
        repository.close()

    print(f"Ingested {ingested} records for run {args.run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
