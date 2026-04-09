"""Scrape average Swedish fuel prices from bensinpriskollen.se and store in Supabase."""

import re
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg2
import requests

URL = "https://bensinpriskollen.se/"

# Maps the label in the sidebar nav to our fuel_type key
FUEL_TYPES = {
    "Bensin 95": "bensin_95",
    "Diesel": "diesel",
    "98-oktan": "oktan_98",
}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fuel_prices (
    fuel_type       TEXT PRIMARY KEY,
    price_sek       NUMERIC(6,2) NOT NULL,
    scraped_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

UPSERT_SQL = """
INSERT INTO fuel_prices (fuel_type, price_sek, scraped_at)
VALUES (%s, %s, %s)
ON CONFLICT (fuel_type) DO UPDATE
SET price_sek = EXCLUDED.price_sek,
    scraped_at = EXCLUDED.scraped_at
"""


def get_dsn() -> str:
    path = Path(__file__).resolve().parent.parent / "connection_string.txt"
    return path.read_text(encoding="utf-8").strip()


def scrape_prices() -> dict[str, Decimal]:
    resp = requests.get(URL, timeout=30)
    resp.raise_for_status()
    html = resp.text

    prices: dict[str, Decimal] = {}
    for label, key in FUEL_TYPES.items():
        # Match the sidebar nav: label</span><span class="...font-mono...">PRICE</span>
        pattern = (
            re.escape(label)
            + r'</span><span class="[^"]*font-mono[^"]*">([\d]+[.,]\d+)</span>'
        )
        match = re.search(pattern, html)
        if match:
            price_str = match.group(1).replace(",", ".")
            prices[key] = Decimal(price_str)

    if not prices:
        print("ERROR: Could not parse any fuel prices from the page.", file=sys.stderr)
        sys.exit(1)

    return prices


def store_prices(prices: dict[str, Decimal]) -> None:
    dsn = get_dsn()
    conn = psycopg2.connect(dsn)
    try:
        now = datetime.now(timezone.utc)
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            for fuel_type, price in prices.items():
                cur.execute(UPSERT_SQL, (fuel_type, price, now))
        conn.commit()
        print(f"Stored {len(prices)} fuel prices at {now.isoformat()}")
    finally:
        conn.close()


def main() -> None:
    print("Scraping fuel prices from bensinpriskollen.se...")
    prices = scrape_prices()
    for fuel_type, price in sorted(prices.items()):
        print(f"  {fuel_type}: {price} kr/l")
    store_prices(prices)
    print("Done.")


if __name__ == "__main__":
    main()
