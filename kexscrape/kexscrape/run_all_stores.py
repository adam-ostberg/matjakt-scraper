from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .bulk_ingestion import BulkIngestionService
from .postgres_repo import PostgresRepository
from .store_catalog import StoreCatalogEntry, fetch_store_catalog


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ICA product scrape for all online stores, stage NDJSON, and ingest into PostgreSQL or Supabase."
    )
    parser.add_argument("--run-id", default="", help="Optional explicit run id")
    parser.add_argument(
        "--resume-run-id",
        default="",
        help="Resume from existing run manifest without recreating store list",
    )
    parser.add_argument(
        "--stage-root",
        default="runs",
        help="Root directory for staged run outputs",
    )
    parser.add_argument("--customer-type", default="B2C", help="Store catalog customer type")
    parser.add_argument("--concurrency", type=int, default=3, help="Concurrent store scrapes")
    parser.add_argument(
        "--max-retries",
        type=int,
        default=2,
        help="Retry attempts per failed store after first attempt",
    )
    parser.add_argument("--max-stores", type=int, default=0, help="Limit number of stores")
    parser.add_argument(
        "--max-products",
        type=int,
        default=0,
        help="Limit number of products scraped per store (0 = no limit)",
    )
    parser.add_argument(
        "--scrapy-cmd",
        default="scrapy",
        help="Scrapy executable (default: scrapy)",
    )
    parser.add_argument(
        "--db-dsn",
        default="",
        help="PostgreSQL or Supabase DSN (optional)",
    )

    parser.add_argument("--category-depth", default="8")
    parser.add_argument("--max-pages-per-category", default="100")
    parser.add_argument("--max-categories", default="")
    parser.add_argument("--include-non-leaf", default="true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    manifest_path, manifest = _prepare_manifest(args)
    run_dir = Path(manifest["run_dir"])
    stores_dir = run_dir / "stores"
    logs_dir = run_dir / "logs"
    stores_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    manifest["status"] = "RUNNING"
    manifest["updated_at_utc"] = _utc_now_iso()
    _save_manifest(manifest_path, manifest)

    db_repo = PostgresRepository(args.db_dsn) if args.db_dsn else None
    if db_repo is not None:
        db_repo.apply_schema()
        db_repo.start_crawl_run(
            run_id=manifest["run_id"],
            started_at=_from_iso(manifest["started_at_utc"]),
            total_stores=len(manifest["stores"]),
            status="RUNNING",
            metadata={
                "customer_type": manifest.get("customer_type", "B2C"),
                "stage_root": manifest.get("stage_root", ""),
            },
        )
        _sync_store_statuses(db_repo, manifest)
        db_repo.commit()

    try:
        _execute_store_scrapes(
            args=args,
            manifest=manifest,
            manifest_path=manifest_path,
            stores_dir=stores_dir,
            logs_dir=logs_dir,
            db_repo=db_repo,
        )

        ingestion_summary = {"enabled": bool(db_repo), "records_ingested": 0, "error": ""}
        if db_repo is not None:
            staged_files = _successful_store_files(manifest)
            ingestion_service = BulkIngestionService(db_repo)
            ingested = ingestion_service.ingest_paths(
                staged_files,
                run_id=manifest["run_id"],
            )
            ingestion_summary["records_ingested"] = ingested
            db_repo.commit()

        final_status = _derive_final_status(manifest)
        manifest["status"] = final_status
        manifest["ingestion"] = ingestion_summary
        manifest["finished_at_utc"] = _utc_now_iso()
        manifest["updated_at_utc"] = manifest["finished_at_utc"]
        _save_manifest(manifest_path, manifest)

        if db_repo is not None:
            success_count = _count_status(manifest, "SUCCESS")
            failed_count = _count_status(manifest, "FAILED")
            _sync_store_statuses(db_repo, manifest)
            db_repo.finish_crawl_run(
                run_id=manifest["run_id"],
                finished_at=_from_iso(manifest["finished_at_utc"]),
                status=final_status,
                succeeded_stores=success_count,
                failed_stores=failed_count,
            )
            db_repo.commit()

        print(
            json.dumps(
                {
                    "run_id": manifest["run_id"],
                    "status": final_status,
                    "successful_stores": _count_status(manifest, "SUCCESS"),
                    "failed_stores": _count_status(manifest, "FAILED"),
                    "records_ingested": ingestion_summary["records_ingested"],
                    "manifest_path": str(manifest_path),
                }
            )
        )

        return 0 if final_status in {"SUCCESS", "PARTIAL_SUCCESS"} else 1
    except Exception as error:
        manifest["status"] = "FAILED"
        manifest["error"] = str(error)
        manifest["finished_at_utc"] = _utc_now_iso()
        manifest["updated_at_utc"] = manifest["finished_at_utc"]
        _save_manifest(manifest_path, manifest)

        if db_repo is not None:
            _sync_store_statuses(db_repo, manifest)
            db_repo.finish_crawl_run(
                run_id=manifest["run_id"],
                finished_at=_from_iso(manifest["finished_at_utc"]),
                status="FAILED",
                succeeded_stores=_count_status(manifest, "SUCCESS"),
                failed_stores=_count_status(manifest, "FAILED"),
            )
            db_repo.commit()

        raise
    finally:
        if db_repo is not None:
            db_repo.close()


def _prepare_manifest(args: argparse.Namespace) -> tuple[Path, dict[str, Any]]:
    stage_root = Path(args.stage_root)
    stage_root.mkdir(parents=True, exist_ok=True)

    if args.resume_run_id:
        run_id = args.resume_run_id.strip()
        if not run_id:
            raise ValueError("resume-run-id cannot be empty")
        run_dir = stage_root / run_id
        manifest_path = run_dir / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(f"Missing manifest for run_id={run_id}: {manifest_path}")
        manifest = _load_manifest(manifest_path)
        _reset_running_stores_to_pending(manifest)
        return manifest_path, manifest

    run_id = args.run_id.strip() or _generate_run_id()
    run_dir = stage_root / run_id
    manifest_path = run_dir / "manifest.json"

    stores = fetch_store_catalog(customer_type=args.customer_type)
    if args.max_stores and args.max_stores > 0:
        stores = stores[: args.max_stores]

    manifest = {
        "run_id": run_id,
        "customer_type": args.customer_type,
        "stage_root": str(stage_root),
        "run_dir": str(run_dir),
        "status": "PENDING",
        "started_at_utc": _utc_now_iso(),
        "updated_at_utc": _utc_now_iso(),
        "stores": _initialize_store_states(stores, run_dir=run_dir),
    }

    run_dir.mkdir(parents=True, exist_ok=True)
    _save_manifest(manifest_path, manifest)
    return manifest_path, manifest


def _initialize_store_states(
    stores: list[StoreCatalogEntry],
    *,
    run_dir: Path,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for store in stores:
        output_path = run_dir / "stores" / f"{store.store_id}.ndjson"
        result[store.store_id] = {
            **store.to_manifest_dict(),
            "status": "PENDING",
            "attempts": 0,
            "last_error": "",
            "output_path": str(output_path),
            "last_attempt_started_at_utc": "",
            "last_attempt_finished_at_utc": "",
        }
    return result


def _execute_store_scrapes(
    *,
    args: argparse.Namespace,
    manifest: dict[str, Any],
    manifest_path: Path,
    stores_dir: Path,
    logs_dir: Path,
    db_repo: PostgresRepository | None,
) -> None:
    max_attempts = max(1, args.max_retries + 1)
    project_dir = Path(__file__).resolve().parents[1]

    while True:
        pending_store_ids = [
            store_id
            for store_id, store in manifest["stores"].items()
            if store["status"] in {"PENDING", "RUNNING"}
            and int(store["attempts"]) < max_attempts
        ]
        if not pending_store_ids:
            break

        with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as executor:
            futures = {}
            for store_id in pending_store_ids:
                store = manifest["stores"][store_id]
                store["status"] = "RUNNING"
                store["attempts"] = int(store["attempts"]) + 1
                store["last_error"] = ""
                store["last_attempt_started_at_utc"] = _utc_now_iso()

                output_path = Path(store["output_path"])
                output_path.parent.mkdir(parents=True, exist_ok=True)
                if output_path.exists():
                    output_path.unlink()

                futures[
                    executor.submit(
                        _run_single_store_scrape,
                        args=args,
                        project_dir=project_dir,
                        run_id=manifest["run_id"],
                        store=store,
                        logs_dir=logs_dir,
                    )
                ] = store_id

            manifest["updated_at_utc"] = _utc_now_iso()
            _save_manifest(manifest_path, manifest)
            if db_repo is not None:
                _sync_store_statuses(db_repo, manifest)
                db_repo.commit()

            for future in as_completed(futures):
                store_id = futures[future]
                result = future.result()
                store = manifest["stores"][store_id]
                store["last_attempt_finished_at_utc"] = _utc_now_iso()

                if result["ok"]:
                    store["status"] = "SUCCESS"
                else:
                    store["last_error"] = result["error"]
                    if int(store["attempts"]) < max_attempts:
                        store["status"] = "PENDING"
                    else:
                        store["status"] = "FAILED"

                manifest["updated_at_utc"] = _utc_now_iso()
                _save_manifest(manifest_path, manifest)
                if db_repo is not None:
                    _sync_store_statuses(db_repo, manifest)
                    db_repo.commit()


def _run_single_store_scrape(
    *,
    args: argparse.Namespace,
    project_dir: Path,
    run_id: str,
    store: dict[str, Any],
    logs_dir: Path,
) -> dict[str, Any]:
    attempt = int(store["attempts"])
    if attempt > 1:
        time.sleep(2 ** (attempt - 2))

    output_path = Path(store["output_path"])
    log_path = logs_dir / f"{store['store_id']}.attempt-{attempt}.log"
    delivery_methods = _normalize_delivery_methods(store.get("delivery_methods", []))

    command = [
        args.scrapy_cmd,
        "crawl",
        "ica_store",
        "-a",
        f"store_id={store['store_id']}",
        "-a",
        f"run_id={run_id}",
        "-a",
        f"store_name={store.get('store_name', '')}",
        "-a",
        f"city={store.get('city', '')}",
        "-a",
        f"zip_code={store.get('zip_code', '')}",
        "-a",
        f"lat={store.get('lat', '')}",
        "-a",
        f"lon={store.get('lon', '')}",
        "-a",
        f"store_format={store.get('store_format', '')}",
        "-a",
        "delivery_methods=" + ",".join(delivery_methods),
        "-a",
        f"source_store_catalog_id={store.get('source_store_catalog_id', '')}",
        "-a",
        f"category_depth={args.category_depth}",
        "-a",
        f"max_pages_per_category={args.max_pages_per_category}",
        "-a",
        f"max_products={args.max_products}",
        "-a",
        f"include_non_leaf={args.include_non_leaf}",
        "-O",
        f"{output_path}:jsonlines",
    ]

    if args.max_categories:
        command.extend(["-a", f"max_categories={args.max_categories}"])

    environment = os.environ.copy()
    environment["PYTHONUNBUFFERED"] = "1"

    completed = subprocess.run(
        command,
        cwd=str(project_dir),
        capture_output=True,
        text=True,
        env=environment,
    )

    combined_log = (
        "COMMAND: "
        + " ".join(command)
        + "\n\nSTDOUT:\n"
        + (completed.stdout or "")
        + "\n\nSTDERR:\n"
        + (completed.stderr or "")
    )
    log_path.write_text(combined_log, encoding="utf-8")

    if completed.returncode == 0 and output_path.exists():
        return {"ok": True, "error": "", "log_path": str(log_path)}

    tail = (completed.stderr or completed.stdout or "").strip().splitlines()[-8:]
    error = (
        f"scrapy exit={completed.returncode}; "
        + " | ".join(line.strip() for line in tail if line.strip())
    )
    if not output_path.exists():
        error = f"{error}; missing output file"

    return {"ok": False, "error": error, "log_path": str(log_path)}


def _successful_store_files(manifest: dict[str, Any]) -> list[Path]:
    files: list[Path] = []
    for store in manifest["stores"].values():
        if store["status"] != "SUCCESS":
            continue
        path = Path(store["output_path"])
        if path.exists() and path.stat().st_size > 0:
            files.append(path)
    return files


def _derive_final_status(manifest: dict[str, Any]) -> str:
    success_count = _count_status(manifest, "SUCCESS")
    failed_count = _count_status(manifest, "FAILED")
    if failed_count == 0:
        return "SUCCESS"
    if success_count > 0:
        return "PARTIAL_SUCCESS"
    return "FAILED"


def _count_status(manifest: dict[str, Any], status: str) -> int:
    return sum(1 for store in manifest["stores"].values() if store["status"] == status)


def _sync_store_statuses(repo: PostgresRepository, manifest: dict[str, Any]) -> None:
    for store_id, store in manifest["stores"].items():
        repo.upsert_crawl_store_status(
            run_id=manifest["run_id"],
            store_id=store_id,
            store_name=store.get("store_name", ""),
            attempts=int(store.get("attempts", 0)),
            status=str(store.get("status", "PENDING")),
            last_error=str(store.get("last_error", "")),
            output_path=str(store.get("output_path", "")),
            updated_at=datetime.now(timezone.utc),
        )


def _normalize_delivery_methods(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    return []


def _reset_running_stores_to_pending(manifest: dict[str, Any]) -> None:
    for store in manifest["stores"].values():
        if store.get("status") == "RUNNING":
            store["status"] = "PENDING"


def _generate_run_id() -> str:
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = uuid.uuid4().hex[:8]
    return f"ica-all-{now}-{suffix}"


def _load_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _save_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp_path.replace(path)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _from_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


if __name__ == "__main__":
    raise SystemExit(main())
