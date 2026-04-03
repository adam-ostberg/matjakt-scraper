from __future__ import annotations

import argparse
import json
import math
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show progress for all-store Coop scrape runs."
    )
    parser.add_argument(
        "--stage-root",
        default="runs",
        help="Root directory containing run folders (default: runs)",
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="Specific run id to inspect. If omitted, the latest run is used.",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Continuously refresh progress output.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=15.0,
        help="Refresh interval in seconds for --watch (default: 15)",
    )
    parser.add_argument(
        "--show-running",
        action="store_true",
        help="Show currently running stores.",
    )
    parser.add_argument(
        "--max-running",
        type=int,
        default=20,
        help="Max running stores to print with --show-running (default: 20)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        manifest_path = _resolve_manifest_path(
            stage_root=Path(args.stage_root),
            run_id=args.run_id.strip(),
        )
    except FileNotFoundError as error:
        print(str(error))
        return 1

    if args.watch:
        return _watch_progress(
            manifest_path=manifest_path,
            interval_seconds=max(0.5, args.interval),
            show_running=args.show_running,
            max_running=max(1, args.max_running),
        )

    manifest = _load_manifest(manifest_path)
    print(
        _format_progress(
            manifest=manifest,
            show_running=args.show_running,
            max_running=max(1, args.max_running),
        )
    )
    return 0


def _watch_progress(
    *,
    manifest_path: Path,
    interval_seconds: float,
    show_running: bool,
    max_running: int,
) -> int:
    try:
        while True:
            manifest = _load_manifest(manifest_path)
            print("\x1b[2J\x1b[H", end="")
            print(
                _format_progress(
                    manifest=manifest,
                    show_running=show_running,
                    max_running=max_running,
                )
            )
            if manifest.get("status") in {"SUCCESS", "PARTIAL_SUCCESS", "FAILED"}:
                break
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        return 0
    return 0


def _format_progress(
    *,
    manifest: dict[str, Any],
    show_running: bool,
    max_running: int,
) -> str:
    stores = manifest.get("stores", {})
    total = len(stores)
    status_counts = Counter(
        str(store.get("status", "UNKNOWN"))
        for store in stores.values()
        if isinstance(store, dict)
    )
    succeeded = status_counts.get("SUCCESS", 0)
    failed = status_counts.get("FAILED", 0)
    running = status_counts.get("RUNNING", 0)
    pending = status_counts.get("PENDING", 0)
    completed = succeeded + failed
    percent = math.floor((completed / total) * 100) if total else 0

    lines = [
        f"time={datetime.now(timezone.utc).isoformat()}",
        f"run_id={manifest.get('run_id', '')}",
        f"status={manifest.get('status', 'UNKNOWN')}",
        f"started_at_utc={manifest.get('started_at_utc', '')}",
        f"updated_at_utc={manifest.get('updated_at_utc', '')}",
        f"progress={completed}/{total} ({percent}%)",
        (
            f"counts: SUCCESS={succeeded} "
            f"FAILED={failed} RUNNING={running} PENDING={pending}"
        ),
    ]

    if show_running:
        running_stores = [
            (store_id, store)
            for store_id, store in stores.items()
            if isinstance(store, dict) and store.get("status") == "RUNNING"
        ]
        running_stores.sort(
            key=lambda item: str(item[1].get("last_attempt_started_at_utc", "")),
            reverse=True,
        )
        lines.append("")
        lines.append(f"running_stores (showing up to {max_running}):")
        for store_id, store in running_stores[:max_running]:
            lines.append(
                (
                    f"- {store_id} "
                    f"{store.get('store_name', '')} "
                    f"attempt={store.get('attempts', 0)} "
                    f"started={store.get('last_attempt_started_at_utc', '')}"
                )
            )
        if not running_stores:
            lines.append("- none")

    return "\n".join(lines)


def _resolve_manifest_path(*, stage_root: Path, run_id: str) -> Path:
    if run_id:
        manifest_path = stage_root / run_id / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(f"Run manifest not found: {manifest_path}")
        return manifest_path

    if not stage_root.exists():
        raise FileNotFoundError(f"Stage root does not exist: {stage_root}")

    run_dirs = sorted(
        [path for path in stage_root.iterdir() if path.is_dir()],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not run_dirs:
        raise FileNotFoundError(f"No run directories found under: {stage_root}")

    manifest_path = run_dirs[0] / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found for latest run: {manifest_path}")
    return manifest_path


def _load_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    raise SystemExit(main())
