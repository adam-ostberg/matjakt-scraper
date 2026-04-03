from __future__ import annotations

import csv
import io
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from .bulk_ingestion import STAGING_COLUMNS, normalized_record_to_staging_row
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
            self._driver = "psycopg"
        elif psycopg2 is not None:
            self._conn = psycopg2.connect(dsn)
            self._driver = "psycopg2"
        else:  # pragma: no cover
            raise RuntimeError(
                "Neither psycopg nor psycopg2 is installed. Install one to use PostgreSQL ingestion."
            )
        self._conn.autocommit = False
        with self._conn.cursor() as cur:
            cur.execute("SET statement_timeout = '60min'")

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

    def replace_staged_run(
        self, *, run_id: str, records: Iterable[NormalizedRecord]
    ) -> int:
        self.cleanup_staged_run(run_id=run_id)

        batch: list[NormalizedRecord] = []
        total = 0
        for record in records:
            batch.append(record)
            if len(batch) >= 5000:
                self._copy_staging_batch(batch)
                total += len(batch)
                batch.clear()

        if batch:
            self._copy_staging_batch(batch)
            total += len(batch)

        return total

    def merge_staged_run(self, *, run_id: str) -> None:
        statements = [
            _merge_stores_sql(),
            _merge_products_sql(),
            _merge_store_products_sql(),
            _insert_price_changes_sql(),
            _merge_current_prices_sql(),
        ]
        with self._conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement, (run_id,))

    def cleanup_staged_run(self, *, run_id: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "DELETE FROM coop.staging_scraped_prices WHERE run_id = %s",
                (run_id,),
            )

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
                INSERT INTO coop.crawl_runs (
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

    def _copy_staging_batch(self, batch: list[NormalizedRecord]) -> None:
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        for record in batch:
            writer.writerow(_serialize_copy_row(normalized_record_to_staging_row(record)))
        buffer.seek(0)

        columns_sql = ", ".join(STAGING_COLUMNS)
        sql = (
            "COPY coop.staging_scraped_prices "
            f"({columns_sql}) "
            "FROM STDIN WITH (FORMAT csv, NULL '\\N')"
        )

        with self._conn.cursor() as cur:
            if self._driver == "psycopg":
                with cur.copy(sql) as copy:
                    copy.write(buffer.getvalue())
            else:
                cur.copy_expert(sql, buffer)

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
                UPDATE coop.crawl_runs
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
                INSERT INTO coop.crawl_store_status (
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
                INSERT INTO coop.stores (
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
                INSERT INTO coop.products (
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
                    product_id = COALESCE(coop.products.product_id, EXCLUDED.product_id),
                    name = COALESCE(EXCLUDED.name, coop.products.name),
                    brand = COALESCE(EXCLUDED.brand, coop.products.brand),
                    pack_size = COALESCE(EXCLUDED.pack_size, coop.products.pack_size),
                    country_of_origin = COALESCE(
                        EXCLUDED.country_of_origin,
                        coop.products.country_of_origin
                    ),
                    product_image_url = COALESCE(
                        EXCLUDED.product_image_url,
                        coop.products.product_image_url
                    ),
                    product_information = COALESCE(
                        EXCLUDED.product_information,
                        coop.products.product_information
                    ),
                    ingredients = COALESCE(
                        EXCLUDED.ingredients,
                        coop.products.ingredients
                    ),
                    unit = COALESCE(EXCLUDED.unit, coop.products.unit),
                    product_type = COALESCE(EXCLUDED.product_type, coop.products.product_type),
                    is_alcohol = COALESCE(EXCLUDED.is_alcohol, coop.products.is_alcohol),
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
                INSERT INTO coop.store_products (
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
                FROM coop.current_prices
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
                INSERT INTO coop.current_prices (
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
                INSERT INTO coop.price_changes (
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


def _serialize_copy_row(row: tuple[Any, ...]) -> list[Any]:
    serialized: list[Any] = []
    for value in row:
        if value is None:
            serialized.append("\\N")
        elif isinstance(value, list):
            serialized.append(_json_dumps(value))
        elif isinstance(value, Decimal):
            serialized.append(str(value))
        elif isinstance(value, datetime):
            serialized.append(value.isoformat())
        else:
            serialized.append(value)
    return serialized


def _latest_staged_records_cte() -> str:
    return """
        WITH latest_staged_records AS (
            SELECT DISTINCT ON (store_id, product_key)
                staging_id,
                run_id,
                store_id,
                source_store_catalog_id,
                store_name,
                city,
                zip_code,
                lat,
                lon,
                store_format,
                delivery_methods,
                category_id,
                category_name,
                category_path,
                retailer_product_id,
                product_id,
                product_key,
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
                available,
                is_new,
                price,
                promo_price,
                unit_price,
                promo_unit_price,
                currency,
                observed_at
            FROM coop.staging_scraped_prices
            WHERE run_id = %s
            ORDER BY store_id, product_key, observed_at DESC, staging_id DESC
        )
    """


def _merge_stores_sql() -> str:
    return (
        """
        WITH latest_stores AS (
            SELECT DISTINCT ON (store_id)
                store_id,
                source_store_catalog_id,
                store_name,
                city,
                zip_code,
                store_format,
                lat,
                lon,
                delivery_methods,
                observed_at
            FROM coop.staging_scraped_prices
            WHERE run_id = %s
            ORDER BY store_id, observed_at DESC, staging_id DESC
        )
        INSERT INTO coop.stores (
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
        SELECT
            store_id,
            source_store_catalog_id,
            store_name,
            city,
            zip_code,
            '',
            '',
            '',
            store_format,
            lat,
            lon,
            delivery_methods,
            observed_at,
            observed_at
        FROM latest_stores
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
        """
    )


def _merge_products_sql() -> str:
    return (
        """
        WITH latest_products AS (
            SELECT DISTINCT ON (product_key)
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
                observed_at
            FROM coop.staging_scraped_prices
            WHERE run_id = %s
            ORDER BY product_key, observed_at DESC, staging_id DESC
        )
        INSERT INTO coop.products (
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
        SELECT
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
            observed_at,
            observed_at
        FROM latest_products
        ON CONFLICT (product_key) DO UPDATE
        SET
            product_id = COALESCE(coop.products.product_id, EXCLUDED.product_id),
            name = COALESCE(EXCLUDED.name, coop.products.name),
            brand = COALESCE(EXCLUDED.brand, coop.products.brand),
            pack_size = COALESCE(EXCLUDED.pack_size, coop.products.pack_size),
            country_of_origin = COALESCE(
                EXCLUDED.country_of_origin,
                coop.products.country_of_origin
            ),
            product_image_url = COALESCE(
                EXCLUDED.product_image_url,
                coop.products.product_image_url
            ),
            product_information = COALESCE(
                EXCLUDED.product_information,
                coop.products.product_information
            ),
            ingredients = COALESCE(EXCLUDED.ingredients, coop.products.ingredients),
            unit = COALESCE(EXCLUDED.unit, coop.products.unit),
            product_type = COALESCE(EXCLUDED.product_type, coop.products.product_type),
            is_alcohol = COALESCE(EXCLUDED.is_alcohol, coop.products.is_alcohol),
            last_seen_at = EXCLUDED.last_seen_at
        """
    )


def _merge_store_products_sql() -> str:
    return (
        """
        """
        + _latest_staged_records_cte()
        + """
        INSERT INTO coop.store_products (
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
        SELECT
            store_id,
            product_key,
            retailer_product_id,
            category_id,
            category_name,
            category_path,
            available,
            is_new,
            observed_at,
            observed_at
        FROM latest_staged_records
        ON CONFLICT (store_id, product_key) DO UPDATE
        SET
            retailer_product_id = EXCLUDED.retailer_product_id,
            category_id = EXCLUDED.category_id,
            category_name = EXCLUDED.category_name,
            category_path = EXCLUDED.category_path,
            available = EXCLUDED.available,
            is_new = EXCLUDED.is_new,
            last_seen_at = EXCLUDED.last_seen_at
        """
    )


def _insert_price_changes_sql() -> str:
    return (
        """
        """
        + _latest_staged_records_cte()
        + """
        , compared_prices AS (
            SELECT
                lsr.run_id,
                lsr.store_id,
                lsr.product_key,
                lsr.observed_at,
                lsr.price,
                lsr.promo_price,
                lsr.unit_price,
                lsr.promo_unit_price,
                lsr.currency,
                lsr.available,
                ARRAY_REMOVE(
                    ARRAY[
                        CASE WHEN cp.price IS DISTINCT FROM lsr.price THEN 'price' END,
                        CASE
                            WHEN cp.promo_price IS DISTINCT FROM lsr.promo_price
                            THEN 'promo_price'
                        END,
                        CASE
                            WHEN cp.unit_price IS DISTINCT FROM lsr.unit_price
                            THEN 'unit_price'
                        END,
                        CASE
                            WHEN cp.promo_unit_price IS DISTINCT FROM lsr.promo_unit_price
                            THEN 'promo_unit_price'
                        END,
                        CASE
                            WHEN cp.currency IS DISTINCT FROM lsr.currency
                            THEN 'currency'
                        END,
                        CASE
                            WHEN cp.available IS DISTINCT FROM lsr.available
                            THEN 'available'
                        END
                    ],
                    NULL
                ) AS change_fields,
                jsonb_build_object(
                    'price', cp.price,
                    'promo_price', cp.promo_price,
                    'unit_price', cp.unit_price,
                    'promo_unit_price', cp.promo_unit_price,
                    'currency', cp.currency,
                    'available', cp.available,
                    'observed_at', cp.observed_at
                ) AS previous_snapshot,
                jsonb_build_object(
                    'price', lsr.price,
                    'promo_price', lsr.promo_price,
                    'unit_price', lsr.unit_price,
                    'promo_unit_price', lsr.promo_unit_price,
                    'currency', lsr.currency,
                    'available', lsr.available,
                    'observed_at', lsr.observed_at
                ) AS current_snapshot
            FROM latest_staged_records lsr
            JOIN coop.current_prices cp
              ON cp.store_id = lsr.store_id
             AND cp.product_key = lsr.product_key
        )
        INSERT INTO coop.price_changes (
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
        SELECT
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
        FROM compared_prices
        WHERE COALESCE(array_length(change_fields, 1), 0) > 0
        ON CONFLICT (run_id, store_id, product_key) DO NOTHING
        """
    )


def _merge_current_prices_sql() -> str:
    return (
        """
        """
        + _latest_staged_records_cte()
        + """
        INSERT INTO coop.current_prices (
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
        SELECT
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
        FROM latest_staged_records
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
        """
    )
