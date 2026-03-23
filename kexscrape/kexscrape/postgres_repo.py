from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from .ingestion import NormalizedRecord

try:
    import psycopg  # type: ignore
except ImportError:  # pragma: no cover
    psycopg = None

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None


class PostgresRepository:
    def __init__(self, dsn: str):
        if psycopg is not None:
            self._conn = psycopg.connect(dsn)
        elif psycopg2 is not None:
            self._conn = psycopg2.connect(dsn)
        else:  # pragma: no cover
            raise RuntimeError(
                "Neither psycopg nor psycopg2 is installed. Install one to use PostgreSQL ingestion."
            )
        self._conn.autocommit = False

    def close(self) -> None:
        self._conn.close()

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def apply_schema(self, schema_path: Path | None = None) -> None:
        path = schema_path or Path(__file__).parent / "sql" / "schema.sql"
        sql_text = path.read_text(encoding="utf-8")
        statements = [statement.strip() for statement in sql_text.split(";")]
        with self._conn.cursor() as cur:
            for statement in statements:
                if statement:
                    cur.execute(statement)
        self._conn.commit()

    def start_crawl_run(
        self,
        *,
        run_id: str,
        started_at: datetime,
        total_stores: int,
        status: str,
        metadata: dict[str, Any],
    ) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO crawl_runs (
                    run_id,
                    started_at,
                    status,
                    total_stores,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (run_id) DO UPDATE
                SET
                    status = EXCLUDED.status,
                    total_stores = EXCLUDED.total_stores,
                    metadata = EXCLUDED.metadata
                """,
                (
                    run_id,
                    started_at,
                    status,
                    total_stores,
                    _json_dumps(metadata),
                ),
            )

    def finish_crawl_run(
        self,
        *,
        run_id: str,
        finished_at: datetime,
        status: str,
        succeeded_stores: int,
        failed_stores: int,
    ) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE crawl_runs
                SET
                    finished_at = %s,
                    status = %s,
                    succeeded_stores = %s,
                    failed_stores = %s
                WHERE run_id = %s
                """,
                (
                    finished_at,
                    status,
                    succeeded_stores,
                    failed_stores,
                    run_id,
                ),
            )

    def upsert_crawl_store_status(
        self,
        *,
        run_id: str,
        store_id: str,
        store_name: str,
        attempts: int,
        status: str,
        last_error: str,
        output_path: str,
        updated_at: datetime,
    ) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO crawl_store_status (
                    run_id,
                    store_id,
                    store_name,
                    attempts,
                    status,
                    last_error,
                    output_path,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, store_id) DO UPDATE
                SET
                    store_name = EXCLUDED.store_name,
                    attempts = EXCLUDED.attempts,
                    status = EXCLUDED.status,
                    last_error = EXCLUDED.last_error,
                    output_path = EXCLUDED.output_path,
                    updated_at = EXCLUDED.updated_at
                """,
                (
                    run_id,
                    store_id,
                    store_name,
                    attempts,
                    status,
                    last_error,
                    output_path,
                    updated_at,
                ),
            )

    def upsert_store(self, record: NormalizedRecord) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stores (
                    store_id,
                    source_store_catalog_id,
                    store_name,
                    city,
                    zip_code,
                    street,
                    area,
                    slug,
                    store_format,
                    lat,
                    lon,
                    delivery_methods,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                ON CONFLICT (store_id) DO UPDATE
                SET
                    source_store_catalog_id = EXCLUDED.source_store_catalog_id,
                    store_name = EXCLUDED.store_name,
                    city = EXCLUDED.city,
                    zip_code = EXCLUDED.zip_code,
                    store_format = EXCLUDED.store_format,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon,
                    delivery_methods = EXCLUDED.delivery_methods,
                    last_seen_at = EXCLUDED.last_seen_at
                """,
                (
                    record.store_id,
                    record.source_store_catalog_id,
                    record.store_name,
                    record.city,
                    record.zip_code,
                    "",
                    "",
                    "",
                    record.store_format,
                    record.lat,
                    record.lon,
                    _json_dumps(record.delivery_methods),
                    record.observed_at,
                    record.observed_at,
                ),
            )

    def upsert_product(self, record: NormalizedRecord) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO products (
                    product_key,
                    product_id,
                    name,
                    brand,
                    pack_size,
                    country_of_origin,
                    product_image_url,
                    product_information,
                    ingredients,
                    unit,
                    product_type,
                    is_alcohol,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_key) DO UPDATE
                SET
                    product_id = COALESCE(products.product_id, EXCLUDED.product_id),
                    name = COALESCE(EXCLUDED.name, products.name),
                    brand = COALESCE(EXCLUDED.brand, products.brand),
                    pack_size = COALESCE(EXCLUDED.pack_size, products.pack_size),
                    country_of_origin = COALESCE(
                        EXCLUDED.country_of_origin,
                        products.country_of_origin
                    ),
                    product_image_url = COALESCE(
                        EXCLUDED.product_image_url,
                        products.product_image_url
                    ),
                    product_information = COALESCE(
                        EXCLUDED.product_information,
                        products.product_information
                    ),
                    ingredients = COALESCE(
                        EXCLUDED.ingredients,
                        products.ingredients
                    ),
                    unit = COALESCE(EXCLUDED.unit, products.unit),
                    product_type = COALESCE(EXCLUDED.product_type, products.product_type),
                    is_alcohol = COALESCE(EXCLUDED.is_alcohol, products.is_alcohol),
                    last_seen_at = EXCLUDED.last_seen_at
                """,
                (
                    record.product_key,
                    record.product_id,
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
                    record.observed_at,
                    record.observed_at,
                ),
            )

    def upsert_store_product(self, record: NormalizedRecord) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO store_products (
                    store_id,
                    product_key,
                    retailer_product_id,
                    category_id,
                    category_name,
                    category_path,
                    available,
                    is_new,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (store_id, product_key) DO UPDATE
                SET
                    retailer_product_id = EXCLUDED.retailer_product_id,
                    category_id = EXCLUDED.category_id,
                    category_name = EXCLUDED.category_name,
                    category_path = EXCLUDED.category_path,
                    available = EXCLUDED.available,
                    is_new = EXCLUDED.is_new,
                    last_seen_at = EXCLUDED.last_seen_at
                """,
                (
                    record.store_id,
                    record.product_key,
                    record.retailer_product_id,
                    record.category_id,
                    record.category_name,
                    record.category_path,
                    record.available,
                    record.is_new,
                    record.observed_at,
                    record.observed_at,
                ),
            )

    def get_current_price(
        self, *, store_id: str, product_key: str
    ) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    price,
                    promo_price,
                    unit_price,
                    promo_unit_price,
                    currency,
                    available,
                    observed_at
                FROM current_prices
                WHERE store_id = %s AND product_key = %s
                """,
                (store_id, product_key),
            )
            row = cur.fetchone()

        if not row:
            return None

        return {
            "price": row[0],
            "promo_price": row[1],
            "unit_price": row[2],
            "promo_unit_price": row[3],
            "currency": row[4],
            "available": row[5],
            "observed_at": row[6],
        }

    def upsert_current_price(self, record: NormalizedRecord) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO current_prices (
                    store_id,
                    product_key,
                    run_id,
                    observed_at,
                    price,
                    promo_price,
                    unit_price,
                    promo_unit_price,
                    currency,
                    available
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (store_id, product_key) DO UPDATE
                SET
                    run_id = EXCLUDED.run_id,
                    observed_at = EXCLUDED.observed_at,
                    price = EXCLUDED.price,
                    promo_price = EXCLUDED.promo_price,
                    unit_price = EXCLUDED.unit_price,
                    promo_unit_price = EXCLUDED.promo_unit_price,
                    currency = EXCLUDED.currency,
                    available = EXCLUDED.available
                """,
                (
                    record.store_id,
                    record.product_key,
                    record.run_id,
                    record.observed_at,
                    record.price,
                    record.promo_price,
                    record.unit_price,
                    record.promo_unit_price,
                    record.currency,
                    record.available,
                ),
            )

    def insert_price_change(
        self,
        *,
        record: NormalizedRecord,
        changed_fields: list[str],
        previous_snapshot: dict[str, Any],
        current_snapshot: dict[str, Any],
    ) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO price_changes (
                    run_id,
                    store_id,
                    product_key,
                    observed_at,
                    price,
                    promo_price,
                    unit_price,
                    promo_unit_price,
                    currency,
                    available,
                    change_fields,
                    previous_snapshot,
                    current_snapshot
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s::jsonb,
                    %s::jsonb
                )
                ON CONFLICT (run_id, store_id, product_key) DO NOTHING
                """,
                (
                    record.run_id,
                    record.store_id,
                    record.product_key,
                    record.observed_at,
                    record.price,
                    record.promo_price,
                    record.unit_price,
                    record.promo_unit_price,
                    record.currency,
                    record.available,
                    changed_fields,
                    _json_dumps(previous_snapshot),
                    _json_dumps(current_snapshot),
                ),
            )


def _json_dumps(value: Any) -> str:
    return json.dumps(value, default=_json_default, ensure_ascii=False)


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value
