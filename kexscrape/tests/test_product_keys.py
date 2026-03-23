from __future__ import annotations

import unittest

from kexscrape.product_keys import build_product_key


class ProductKeyTests(unittest.TestCase):
    def test_uses_product_id_when_present(self) -> None:
        key = build_product_key("abc-123", "Name", "Brand", "500g")
        self.assertEqual("abc-123", key)

    def test_fallback_key_is_stable_for_equivalent_text(self) -> None:
        key_a = build_product_key(None, "  Product Name ", "Brand", "500 g")
        key_b = build_product_key(None, "product   name", "brand", "500 g")

        self.assertTrue(key_a.startswith("fallback:"))
        self.assertEqual(key_a, key_b)


if __name__ == "__main__":
    unittest.main()
