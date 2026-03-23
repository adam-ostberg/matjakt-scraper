from __future__ import annotations

import unittest
from decimal import Decimal

from kexscrape.price_changes import build_price_snapshot, diff_price_fields


class PriceChangeTests(unittest.TestCase):
    def test_detects_tracked_field_changes(self) -> None:
        previous = build_price_snapshot(
            {
                "price": Decimal("10.00"),
                "promo_price": None,
                "unit_price": Decimal("20.00"),
                "promo_unit_price": None,
                "currency": "SEK",
                "available": True,
            }
        )
        current = build_price_snapshot(
            {
                "price": Decimal("12.00"),
                "promo_price": None,
                "unit_price": Decimal("20.00"),
                "promo_unit_price": None,
                "currency": "SEK",
                "available": False,
            }
        )

        changed = diff_price_fields(previous, current)
        self.assertEqual(["price", "available"], changed)


if __name__ == "__main__":
    unittest.main()
