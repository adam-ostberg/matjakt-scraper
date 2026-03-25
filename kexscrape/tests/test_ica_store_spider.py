from __future__ import annotations

import unittest

from kexscrape.spiders.ica_store_spider import IcaStoreSpider


class IcaStoreSpiderTests(unittest.TestCase):
    def test_to_item_includes_image_product_information_and_ingredients(self) -> None:
        spider = IcaStoreSpider(store_id="1003380")

        category = {
            "categoryId": "cat-1",
            "name": "Skafferi",
            "breadcrumbs": [{"name": "Skafferi"}],
        }
        product = {
            "retailerProductId": "2139045",
            "productId": "abecf442-a7c5-4c7a-9ab3-0d76c42bdd26",
            "name": "Te Svart Collection 50p Lipton",
            "brand": "Lipton",
            "packSizeDescription": "50 per frp",
            "countryOfOrigin": "SE",
            "price": {"amount": "59.11", "currency": "SEK"},
            "unitPrice": {
                "price": {"amount": "1.18", "currency": "SEK"},
                "unit": "fop.price.per.each",
            },
            "available": True,
            "isNew": False,
            "alcohol": False,
            "type": "REGULAR",
            "images": [{"src": "https://example.com/product-500.jpg"}],
        }
        detail_payload = {
            "bopData": {
                "detailedDescription": (
                    "<b>Information från leverantör</b><br>"
                    "Produktinformation text"
                ),
                "fields": [
                    {"title": "ingredients", "content": "<p>Ingredienser text</p>"},
                ],
            }
        }

        item = spider._to_item(category, product, detail_payload)

        self.assertEqual(
            "https://example.com/product-500.jpg", item["product_image_url"]
        )
        self.assertEqual(
            "Information från leverantör\nProduktinformation text",
            item["product_information"],
        )
        self.assertEqual("Ingredienser text", item["ingredients"])

    def test_build_categories_to_visit_deduplicates_ids_and_skips_non_leaf_by_default(self) -> None:
        spider = IcaStoreSpider(store_id="1003380")

        categories = [
            {
                "categoryId": "parent",
                "name": "Parent",
                "productCount": 999,
                "childCategories": [
                    {
                        "categoryId": "leaf-a",
                        "name": "Leaf A",
                        "productCount": 12,
                        "childCategories": [],
                    },
                    {
                        "categoryId": "leaf-a",
                        "name": "Leaf A duplicate",
                        "productCount": 8,
                        "childCategories": [],
                    },
                ],
            },
            {
                "categoryId": "leaf-b",
                "name": "Leaf B",
                "productCount": 20,
                "childCategories": [],
            },
        ]

        selected = spider._build_categories_to_visit(categories)

        self.assertEqual(["leaf-b", "leaf-a"], [item["categoryId"] for item in selected])

    def test_build_categories_to_visit_can_include_non_leaf_categories(self) -> None:
        spider = IcaStoreSpider(store_id="1003380", include_non_leaf="true")

        categories = [
            {
                "categoryId": "parent",
                "name": "Parent",
                "productCount": 25,
                "childCategories": [
                    {
                        "categoryId": "leaf-a",
                        "name": "Leaf A",
                        "productCount": 12,
                        "childCategories": [],
                    }
                ],
            }
        ]

        selected = spider._build_categories_to_visit(categories)

        self.assertEqual(["parent", "leaf-a"], [item["categoryId"] for item in selected])


if __name__ == "__main__":
    unittest.main()
