from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Protocol

from .price_changes import build_price_snapshot, diff_price_fields
from .product_keys import build_product_key


@dataclass
class NormalizedRecord:
    run_id: str
    store_id: str
    source_store_catalog_id: str
    store_name: str
    city: str
    zip_code: str
    lat: float | None
    lon: float | None
    store_format: str
    delivery_methods: list[str]
    category_id: str | None
    category_name: str | None
    category_path: str | None
    retailer_product_id: str
    product_id: str | None
    product_key: str
    name: str | None
    brand: str | None
    pack_size: str | None
    country_of_origin: str | None
    product_image_url: str | None
    product_information: str | None
    ingredients: str | None
    unit: str | None
    product_type: str | None
    is_alcohol: bool | None
    available: bool | None
    is_new: bool | None
    price: Decimal | None
    promo_price: Decimal | None
    unit_price: Decimal | None
    promo_unit_price: Decimal | None
    currency: str | None
    observed_at: datetime


class IngestionRepository(Protocol):
    def upsert_store(self, record: NormalizedRecord) -> None:
        ...

    def upsert_product(self, record: NormalizedRecord) -> None:
        ...

    def upsert_store_product(self, record: NormalizedRecord) -> None:
        ...

    def get_current_price(
        self, *, store_id: str, product_key: str
    ) -> dict[str, Any] | None:
        ...

    def upsert_current_price(self, record: NormalizedRecord) -> None:
        ...

    def insert_price_change(
        self,
        *,
        record: NormalizedRecord,
        changed_fields: list[str],
        previous_snapshot: dict[str, Any],
        current_snapshot: dict[str, Any],
    ) -> None:
        ...


class IngestionService:
    def __init__(self, repository: IngestionRepository):
        self._repository = repository

    def ingest_records(self, records: Iterable[dict[str, Any]], run_id: str) -> int:
        ingested = 0
        for raw_record in records:
            self.ingest_record(raw_record, run_id=run_id)
            ingested += 1
        return ingested

    def ingest_record(self, raw_record: dict[str, Any], *, run_id: str) -> None:
        normalized = normalize_record(raw_record, run_id=run_id)

        self._repository.upsert_store(normalized)
        self._repository.upsert_product(normalized)
        self._repository.upsert_store_product(normalized)

        current_price = self._repository.get_current_price(
            store_id=normalized.store_id,
            product_key=normalized.product_key,
        )

        previous_snapshot = (
            build_price_snapshot(current_price) if current_price is not None else {}
        )
        current_snapshot = build_price_snapshot(normalized.__dict__)

        if current_price is not None:
            changed_fields = diff_price_fields(previous_snapshot, current_snapshot)
            if changed_fields:
                self._repository.insert_price_change(
                    record=normalized,
                    changed_fields=changed_fields,
                    previous_snapshot=previous_snapshot,
                    current_snapshot=current_snapshot,
                )

        self._repository.upsert_current_price(normalized)


def read_ndjson_records(paths: Iterable[Path]) -> Iterable[dict[str, Any]]:
    for path in paths:
        with path.open("r", encoding="utf-8") as file_handle:
            for line in file_handle:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)


def normalize_record(raw_record: dict[str, Any], *, run_id: str) -> NormalizedRecord:
    product_id = _normalize_str(raw_record.get("product_id"))
    name = _normalize_str(raw_record.get("name"))
    brand = _normalize_str(raw_record.get("brand"))
    pack_size = _normalize_str(raw_record.get("pack_size"))

    retailer_product_id = _normalize_str(raw_record.get("retailer_product_id"))
    if not retailer_product_id:
        raise ValueError("retailer_product_id is required")

    store_id = _normalize_str(raw_record.get("store_id"))
    if not store_id:
        raise ValueError("store_id is required")

    effective_run_id = _normalize_str(raw_record.get("run_id")) or run_id

    return NormalizedRecord(
        run_id=effective_run_id,
        store_id=store_id,
        source_store_catalog_id=_normalize_str(
            raw_record.get("source_store_catalog_id")
        )
        or "",
        store_name=_normalize_str(raw_record.get("store_name")) or "",
        city=_normalize_str(raw_record.get("city")) or "",
        zip_code=_normalize_str(raw_record.get("zip_code")) or "",
        lat=_normalize_float(raw_record.get("lat")),
        lon=_normalize_float(raw_record.get("lon")),
        store_format=_normalize_str(raw_record.get("store_format")) or "",
        delivery_methods=_normalize_delivery_methods(raw_record.get("delivery_methods")),
        category_id=_normalize_str(raw_record.get("category_id")),
        category_name=_normalize_str(raw_record.get("category_name")),
        category_path=_normalize_str(raw_record.get("category_path")),
        retailer_product_id=retailer_product_id,
        product_id=product_id,
        product_key=build_product_key(product_id, name, brand, pack_size),
        name=name,
        brand=brand,
        pack_size=pack_size,
        country_of_origin=_normalize_str(raw_record.get("country_of_origin")),
        product_image_url=_normalize_str(raw_record.get("product_image_url")),
        product_information=_normalize_str(raw_record.get("product_information")),
        ingredients=_normalize_str(raw_record.get("ingredients")),
        unit=_normalize_str(raw_record.get("unit")),
        product_type=_normalize_str(raw_record.get("product_type")),
        is_alcohol=_normalize_bool(raw_record.get("is_alcohol")),
        available=_normalize_bool(raw_record.get("available")),
        is_new=_normalize_bool(raw_record.get("is_new")),
        price=_normalize_decimal(raw_record.get("price")),
        promo_price=_normalize_decimal(raw_record.get("promo_price")),
        unit_price=_normalize_decimal(raw_record.get("unit_price")),
        promo_unit_price=_normalize_decimal(raw_record.get("promo_unit_price")),
        currency=_normalize_str(raw_record.get("currency")),
        observed_at=_normalize_timestamp(raw_record.get("scraped_at_utc")),
    )


def _normalize_str(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalize_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _normalize_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_bool(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _normalize_delivery_methods(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return sorted(
            {
                str(item).strip()
                for item in value
                if str(item).strip()
            }
        )
    if isinstance(value, str):
        return sorted(
            {
                item.strip()
                for item in value.split(",")
                if item.strip()
            }
        )
    return []


def _normalize_timestamp(value: Any) -> datetime:
    if value:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


class InMemoryRepository(IngestionRepository):
    def __init__(self):
        self.stores: dict[str, dict[str, Any]] = {}
        self.products: dict[str, dict[str, Any]] = {}
        self.store_products: dict[tuple[str, str], dict[str, Any]] = {}
        self.current_prices: dict[tuple[str, str], dict[str, Any]] = {}
        self.price_changes: dict[tuple[str, str, str], dict[str, Any]] = {}

    def upsert_store(self, record: NormalizedRecord) -> None:
        self.stores[record.store_id] = {
            "store_id": record.store_id,
            "source_store_catalog_id": record.source_store_catalog_id,
            "store_name": record.store_name,
            "city": record.city,
            "zip_code": record.zip_code,
            "lat": record.lat,
            "lon": record.lon,
            "store_format": record.store_format,
            "delivery_methods": list(record.delivery_methods),
        }

    def upsert_product(self, record: NormalizedRecord) -> None:
        self.products[record.product_key] = {
            "product_key": record.product_key,
            "product_id": record.product_id,
            "name": record.name,
            "brand": record.brand,
            "pack_size": record.pack_size,
            "country_of_origin": record.country_of_origin,
            "product_image_url": record.product_image_url,
            "product_information": record.product_information,
            "ingredients": record.ingredients,
            "unit": record.unit,
            "product_type": record.product_type,
            "is_alcohol": record.is_alcohol,
        }

    def upsert_store_product(self, record: NormalizedRecord) -> None:
        self.store_products[(record.store_id, record.product_key)] = {
            "store_id": record.store_id,
            "product_key": record.product_key,
            "retailer_product_id": record.retailer_product_id,
            "category_id": record.category_id,
            "category_name": record.category_name,
            "category_path": record.category_path,
            "available": record.available,
            "is_new": record.is_new,
        }

    def get_current_price(
        self, *, store_id: str, product_key: str
    ) -> dict[str, Any] | None:
        return self.current_prices.get((store_id, product_key))

    def upsert_current_price(self, record: NormalizedRecord) -> None:
        self.current_prices[(record.store_id, record.product_key)] = {
            "run_id": record.run_id,
            "store_id": record.store_id,
            "product_key": record.product_key,
            "price": record.price,
            "promo_price": record.promo_price,
            "unit_price": record.unit_price,
            "promo_unit_price": record.promo_unit_price,
            "currency": record.currency,
            "available": record.available,
            "observed_at": record.observed_at,
        }

    def insert_price_change(
        self,
        *,
        record: NormalizedRecord,
        changed_fields: list[str],
        previous_snapshot: dict[str, Any],
        current_snapshot: dict[str, Any],
    ) -> None:
        key = (record.run_id, record.store_id, record.product_key)
        self.price_changes.setdefault(
            key,
            {
                "run_id": record.run_id,
                "store_id": record.store_id,
                "product_key": record.product_key,
                "observed_at": record.observed_at,
                "changed_fields": list(changed_fields),
                "previous_snapshot": dict(previous_snapshot),
                "current_snapshot": dict(current_snapshot),
            },
        )
