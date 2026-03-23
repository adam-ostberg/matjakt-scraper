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


if __name__ == "__main__":
    unittest.main()
