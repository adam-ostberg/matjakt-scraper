from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from .ingestion import normalize_record, read_ndjson_records
from .postgres_repo import PostgresRepository

BATCH_SIZE = 20


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
        repository.commit()

        total_ingested = 0
        total_batches = (len(store_files) + BATCH_SIZE - 1) // BATCH_SIZE
        for i in range(0, len(store_files), BATCH_SIZE):
            batch_files = store_files[i : i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            print(
                f"[{batch_num}/{total_batches}] Staging {len(batch_files)} stores...",
                flush=True,
            )

            records = read_ndjson_records(batch_files)
            normalized = (normalize_record(r, run_id=args.run_id) for r in records)
            ingested = repository.replace_staged_run(run_id=args.run_id, records=normalized)
            repository.commit()

            print(
                f"[{batch_num}/{total_batches}] Merging {ingested} records...",
                flush=True,
            )
            repository.merge_staged_run(run_id=args.run_id)
            repository.cleanup_staged_run(run_id=args.run_id)
            repository.commit()

            total_ingested += ingested
            print(f"[{batch_num}/{total_batches}] Done.", flush=True)

        repository.finish_crawl_run(
            run_id=args.run_id,
            finished_at=datetime.now(timezone.utc),
            status="SUCCESS",
            succeeded_stores=len(store_files),
            failed_stores=0,
        )
        repository.commit()
    except Exception:
        try:
            repository.rollback()
        except Exception:
            pass
        raise
    finally:
        repository.close()

    print(f"Ingested {total_ingested} records for run {args.run_id}")
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
