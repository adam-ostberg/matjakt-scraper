from __future__ import annotations

import unittest

from kexscrape.ingestion import InMemoryRepository, IngestionService


class IngestionIntegrationTests(unittest.TestCase):
    def test_upsert_and_change_history_idempotency(self) -> None:
        repository = InMemoryRepository()
        service = IngestionService(repository)

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
        product_entry = next(iter(repository.products.values()))
        self.assertEqual(
            "https://example.com/product.jpg", product_entry["product_image_url"]
        )
        self.assertEqual(
            "Produktinformation text", product_entry["product_information"]
        )
        self.assertEqual("Ingredienser text", product_entry["ingredients"])

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

    @staticmethod
    def _record(store_id: str, retailer_product_id: str, price: float) -> dict[str, object]:
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
            "product_id": "product-1",
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
            "scraped_at_utc": "2026-03-10T12:00:00+00:00",
        }


if __name__ == "__main__":
    unittest.main()
