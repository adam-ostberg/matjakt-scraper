from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from kexscrape.bulk_ingestion import BulkIngestionService, InMemoryBulkRepository


class BulkIngestionIntegrationTests(unittest.TestCase):
    def test_bulk_ingest_is_idempotent_and_tracks_price_changes(self) -> None:
        repository = InMemoryBulkRepository()
        service = BulkIngestionService(repository)

        run_1_records = [
            self._record(store_id="1004520", retailer_product_id="r-1", price=10.0),
            self._record(store_id="1005001", retailer_product_id="r-2", price=11.0),
        ]

        ingested_first = service.ingest_records(run_1_records, run_id="run-1")

        self.assertEqual(2, ingested_first)
        self.assertEqual(2, len(repository.stores))
        self.assertEqual(1, len(repository.products))
        self.assertEqual(2, len(repository.store_products))
        self.assertEqual(2, len(repository.current_prices))
        self.assertEqual(0, len(repository.price_changes))
        self.assertEqual({}, repository.staged_runs)

        run_2_records = [
            self._record(store_id="1004520", retailer_product_id="r-1", price=12.0),
            self._record(store_id="1005001", retailer_product_id="r-2", price=11.0),
        ]

        ingested_second = service.ingest_records(run_2_records, run_id="run-2")

        self.assertEqual(2, ingested_second)
        self.assertEqual(1, len(repository.price_changes))

        change_entry = next(iter(repository.price_changes.values()))
        self.assertEqual("run-2", change_entry["run_id"])
        self.assertEqual(["price"], change_entry["changed_fields"])

        service.ingest_records(run_2_records, run_id="run-2")
        self.assertEqual(
            1,
            len(repository.price_changes),
            "rerunning the same run id should not duplicate change rows",
        )

    def test_latest_observed_at_wins_for_duplicate_records_within_run(self) -> None:
        repository = InMemoryBulkRepository()
        service = BulkIngestionService(repository)

        records = [
            self._record(
                store_id="1004520",
                retailer_product_id="r-1",
                price=10.0,
                scraped_at_utc="2026-03-10T12:00:00+00:00",
            ),
            self._record(
                store_id="1004520",
                retailer_product_id="r-1",
                price=13.5,
                scraped_at_utc="2026-03-10T12:05:00+00:00",
            ),
        ]

        service.ingest_records(records, run_id="run-1")

        current_price = repository.current_prices[("1004520", self._product_key())]
        self.assertEqual("13.5", str(current_price["price"]))
        self.assertEqual("2026-03-10 12:05:00+00:00", str(current_price["observed_at"]))

    def test_bulk_ingest_reads_staged_ndjson_files(self) -> None:
        repository = InMemoryBulkRepository()
        service = BulkIngestionService(repository)

        with tempfile.TemporaryDirectory() as temp_dir:
            store_dir = Path(temp_dir)
            store_a = store_dir / "1004520.ndjson"
            store_b = store_dir / "1005001.ndjson"
            store_a.write_text(
                json.dumps(
                    self._record(
                        store_id="1004520",
                        retailer_product_id="r-1",
                        price=10.0,
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            store_b.write_text(
                json.dumps(
                    self._record(
                        store_id="1005001",
                        retailer_product_id="r-2",
                        price=11.0,
                    )
                )
                + "\n",
                encoding="utf-8",
            )

            ingested = service.ingest_paths([store_a, store_b], run_id="run-1")

        self.assertEqual(2, ingested)
        self.assertEqual(2, len(repository.current_prices))
        self.assertEqual({}, repository.staged_runs)

    def test_reingest_from_same_staged_files_rebuilds_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store_path = Path(temp_dir) / "1004520.ndjson"
            store_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            self._record(
                                store_id="1004520",
                                retailer_product_id="r-1",
                                price=10.0,
                                scraped_at_utc="2026-03-10T12:00:00+00:00",
                            )
                        ),
                        json.dumps(
                            self._record(
                                store_id="1004520",
                                retailer_product_id="r-1",
                                price=12.0,
                                scraped_at_utc="2026-03-11T12:00:00+00:00",
                            )
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            first_repository = InMemoryBulkRepository()
            first_service = BulkIngestionService(first_repository)
            first_service.ingest_paths([store_path], run_id="run-1")
            expected_snapshot = self._state_summary(first_repository)

            second_repository = InMemoryBulkRepository()
            second_service = BulkIngestionService(second_repository)
            second_service.ingest_paths([store_path], run_id="run-1")

        self.assertEqual(expected_snapshot, self._state_summary(second_repository))

    @staticmethod
    def _state_summary(repository: InMemoryBulkRepository) -> dict[str, object]:
        return {
            "stores": repository.stores,
            "products": repository.products,
            "store_products": repository.store_products,
            "current_prices": repository.current_prices,
            "price_changes": repository.price_changes,
        }

    @staticmethod
    def _product_key() -> str:
        return "product-1"

    @staticmethod
    def _record(
        store_id: str,
        retailer_product_id: str,
        price: float,
        *,
        scraped_at_utc: str = "2026-03-10T12:00:00+00:00",
    ) -> dict[str, object]:
        return {
            "run_id": "",
            "store_id": store_id,
            "store_name": "Store",
            "city": "City",
            "zip_code": "12345",
            "lat": 59.0,
            "lon": 18.0,
            "store_format": "supermarket",
            "delivery_methods": ["PICKUP"],
            "source_store_catalog_id": "01838",
            "category_id": "cat-1",
            "category_name": "Skafferi",
            "category_path": "Skafferi",
            "retailer_product_id": retailer_product_id,
            "product_id": BulkIngestionIntegrationTests._product_key(),
            "name": "Product",
            "brand": "Brand",
            "pack_size": "500g",
            "country_of_origin": "SE",
            "product_image_url": "https://example.com/product.jpg",
            "product_information": "Produktinformation text",
            "ingredients": "Ingredienser text",
            "unit": "fop.price.per.kg",
            "product_type": "REGULAR",
            "is_alcohol": False,
            "available": True,
            "is_new": False,
            "price": price,
            "promo_price": None,
            "unit_price": 20.0,
            "promo_unit_price": None,
            "currency": "SEK",
            "scraped_at_utc": scraped_at_utc,
        }


if __name__ == "__main__":
    unittest.main()
