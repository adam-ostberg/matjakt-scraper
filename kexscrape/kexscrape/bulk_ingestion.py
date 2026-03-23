from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Hashable, Iterable, Protocol

from .ingestion import (
    InMemoryRepository,
    NormalizedRecord,
    normalize_record,
    read_ndjson_records,
)
from .price_changes import build_price_snapshot, diff_price_fields

STAGING_COLUMNS = (
    "run_id",
    "store_id",
    "source_store_catalog_id",
    "store_name",
    "city",
    "zip_code",
    "lat",
    "lon",
    "store_format",
    "delivery_methods",
    "category_id",
    "category_name",
    "category_path",
    "retailer_product_id",
    "product_id",
    "product_key",
    "name",
    "brand",
    "pack_size",
    "country_of_origin",
    "product_image_url",
    "product_information",
    "ingredients",
    "unit",
    "product_type",
    "is_alcohol",
    "available",
    "is_new",
    "price",
    "promo_price",
    "unit_price",
    "promo_unit_price",
    "currency",
    "observed_at",
)


class BulkIngestionRepository(Protocol):
    def replace_staged_run(
        self, *, run_id: str, records: Iterable[NormalizedRecord]
    ) -> int:
        ...

    def merge_staged_run(self, *, run_id: str) -> None:
        ...

    def cleanup_staged_run(self, *, run_id: str) -> None:
        ...


class BulkIngestionService:
    def __init__(self, repository: BulkIngestionRepository):
        self._repository = repository

    def ingest_paths(self, paths: Iterable[Path], *, run_id: str) -> int:
        return self.ingest_records(read_ndjson_records(paths), run_id=run_id)

    def ingest_records(self, records: Iterable[dict[str, Any]], *, run_id: str) -> int:
        normalized_records = (
            normalize_record(record, run_id=run_id) for record in records
        )
        ingested = self._repository.replace_staged_run(
            run_id=run_id,
            records=normalized_records,
        )
        self._repository.merge_staged_run(run_id=run_id)
        self._repository.cleanup_staged_run(run_id=run_id)
        return ingested


def normalized_record_to_staging_row(record: NormalizedRecord) -> tuple[Any, ...]:
    return (
        record.run_id,
        record.store_id,
        record.source_store_catalog_id,
        record.store_name,
        record.city,
        record.zip_code,
        record.lat,
        record.lon,
        record.store_format,
        list(record.delivery_methods),
        record.category_id,
        record.category_name,
        record.category_path,
        record.retailer_product_id,
        record.product_id,
        record.product_key,
        record.name,
        record.brand,
        record.pack_size,
        record.country_of_origin,
        record.product_image_url,
        record.product_information,
        record.ingredients,
        record.unit,
        record.product_type,
        record.is_alcohol,
        record.available,
        record.is_new,
        record.price,
        record.promo_price,
        record.unit_price,
        record.promo_unit_price,
        record.currency,
        record.observed_at,
    )


class InMemoryBulkRepository(InMemoryRepository):
    def __init__(self):
        super().__init__()
        self.staged_runs: dict[str, list[NormalizedRecord]] = {}

    def replace_staged_run(
        self, *, run_id: str, records: Iterable[NormalizedRecord]
    ) -> int:
        staged_records = list(records)
        self.staged_runs[run_id] = staged_records
        return len(staged_records)

    def merge_staged_run(self, *, run_id: str) -> None:
        staged_records = self.staged_runs.get(run_id, [])
        if not staged_records:
            return

        for record in _latest_records(staged_records, lambda item: item.store_id).values():
            self.upsert_store(record)

        for record in _latest_records(
            staged_records, lambda item: item.product_key
        ).values():
            self.upsert_product(record)

        for record in _latest_records(
            staged_records, lambda item: (item.store_id, item.product_key)
        ).values():
            self.upsert_store_product(record)

        latest_prices = _latest_records(
            staged_records,
            lambda item: (item.store_id, item.product_key),
        )
        for record in latest_prices.values():
            current_price = self.get_current_price(
                store_id=record.store_id,
                product_key=record.product_key,
            )

            if current_price is not None:
                previous_snapshot = build_price_snapshot(current_price)
                current_snapshot = build_price_snapshot(record.__dict__)
                changed_fields = diff_price_fields(previous_snapshot, current_snapshot)
                if changed_fields:
                    self.insert_price_change(
                        record=record,
                        changed_fields=changed_fields,
                        previous_snapshot=previous_snapshot,
                        current_snapshot=current_snapshot,
                    )

            self.upsert_current_price(record)

    def cleanup_staged_run(self, *, run_id: str) -> None:
        self.staged_runs.pop(run_id, None)


def _latest_records(
    records: Iterable[NormalizedRecord],
    key_fn: Callable[[NormalizedRecord], Hashable],
) -> dict[Hashable, NormalizedRecord]:
    latest: dict[Hashable, tuple[tuple[Any, int], NormalizedRecord]] = {}
    for index, record in enumerate(records):
        key = key_fn(record)
        order_key = (record.observed_at, index)
        current = latest.get(key)
        if current is None or order_key >= current[0]:
            latest[key] = (order_key, record)
    return {key: item[1] for key, item in latest.items()}
