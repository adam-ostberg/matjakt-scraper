from __future__ import annotations

import unittest

from kexscrape.store_catalog import flatten_store_catalog_payload


class StoreCatalogTests(unittest.TestCase):
    def test_flatten_and_dedupe_by_account_id(self) -> None:
        payload = {
            "A": {
                "Alingsas": {
                    "Alingsas": [
                        {
                            "id": "01838",
                            "accountId": "1004520",
                            "name": "ICA Supermarket Noltorp",
                            "city": "Alingsas",
                            "zipCode": "44115",
                            "street": "Noltorps Centrum",
                            "area": "ALINGSAS",
                            "slug": "ica-supermarket-noltorp-id_01838",
                            "storeFormat": "supermarket",
                            "latitude": 57.93779,
                            "longitude": 12.52219,
                            "deliveryMethods": ["PICKUP"],
                            "enable": "1",
                            "published": "1",
                        },
                        {
                            "id": "DUPLICATE",
                            "accountId": "1004520",
                            "name": "Duplicate",
                            "deliveryMethods": ["HOME_DELIVERY"],
                            "enable": "1",
                            "published": "1",
                        },
                    ]
                }
            },
            "B": {
                "Borlange": {
                    "Borlange": [
                        {
                            "id": "50001",
                            "accountId": "1005001",
                            "name": "ICA Borlange",
                            "city": "Borlange",
                            "zipCode": "78433",
                            "deliveryMethods": ["HOME_DELIVERY", "PICKUP"],
                            "enable": "1",
                            "published": "1",
                        }
                    ]
                }
            },
        }

        entries = flatten_store_catalog_payload(payload)

        self.assertEqual(2, len(entries))
        self.assertEqual(["1004520", "1005001"], [entry.store_id for entry in entries])
        self.assertEqual(
            ("PICKUP",),
            entries[0].delivery_methods,
            "first occurrence should win during dedupe",
        )


if __name__ == "__main__":
    unittest.main()
