from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from .bulk_ingestion import BulkIngestionService
from .postgres_repo import PostgresRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest staged ICA NDJSON files into PostgreSQL or Supabase"
    )
    parser.add_argument("--run-id", required=True, help="Run identifier")
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Path to run directory containing stores/*.ndjson",
    )
    parser.add_argument("--db-dsn", required=True, help="PostgreSQL or Supabase DSN")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_dir = Path(args.run_dir)
    store_files = sorted((run_dir / "stores").glob("*.ndjson"))
    manifest = _load_manifest(run_dir / "manifest.json")

    repository = PostgresRepository(args.db_dsn)
    try:
        repository.apply_schema()
        repository.start_crawl_run(
            run_id=args.run_id,
            started_at=_manifest_started_at(manifest),
            total_stores=len(store_files),
            status="RUNNING",
            metadata={
                "run_dir": str(run_dir),
                "stage_root": str(run_dir.parent),
            },
        )
        ingestion = BulkIngestionService(repository)
        ingested = ingestion.ingest_paths(store_files, run_id=args.run_id)
        repository.finish_crawl_run(
            run_id=args.run_id,
            finished_at=datetime.now(timezone.utc),
            status="SUCCESS",
            succeeded_stores=len(store_files),
            failed_stores=0,
        )
        repository.commit()
    except Exception:
        repository.rollback()
        raise
    finally:
        repository.close()

    print(f"Ingested {ingested} records for run {args.run_id}")
    return 0

def _load_manifest(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _manifest_started_at(manifest: dict[str, object]) -> datetime:
    value = manifest.get("started_at_utc")
    if isinstance(value, str) and value.strip():
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return datetime.now(timezone.utc)


if __name__ == "__main__":
    raise SystemExit(main())
