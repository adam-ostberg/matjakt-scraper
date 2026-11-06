# Matjakt Scraper

**Data collection for a grocery price comparison of Swedish stores.**

Matjakt Scraper collects product and price data from the online stores of Sweden's two largest grocery chains, **ICA** and **Coop**, store by store. It loads the data into PostgreSQL, where it can be compared across stores and tracked over time. It was built as the data-collection part of a bachelor's thesis (*kandidatexamensarbete*) at **KTH Royal Institute of Technology** in spring 2026.

<p>
  <img alt="Python" src="https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white">
  <img alt="Scrapy" src="https://img.shields.io/badge/Scrapy-60A839?logo=scrapy&logoColor=white">
  <img alt="Playwright" src="https://img.shields.io/badge/Playwright-2EAD33?logo=playwright&logoColor=white">
  <img alt="PostgreSQL" src="https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white">
  <img alt="Supabase" src="https://img.shields.io/badge/Supabase-3FCF8E?logo=supabase&logoColor=white">
</p>

| Scale of the collected dataset | |
| --- | ---: |
| Store-level prices | **~4.3 million** |
| Unique products | **~57,000** |
| Stores | **~500** |

---

## Why

Grocery prices in Sweden vary between chains and between individual stores of the same chain. There is no open dataset that shows these differences. To study them, the thesis needed the **full assortment and current prices of every store that sells online**, and it needed to collect them again regularly to see how prices change. This repository is the pipeline that produced that dataset.

## Highlights

- **Every store, the full assortment.** The scraper reads the chain's own store list, then goes through each store's category tree and every product page in it, including promotional prices, unit prices, availability, origin, ingredients and images.
- **Uses the stores' own JSON APIs instead of parsing HTML.** A headless Chromium browser (Playwright) opens a real session, and the JSON endpoints that the web shop itself uses are called *from inside that page*. This makes the data much more structured and stable than HTML parsing would. Product detail requests are batched and run concurrently in the browser.
- **Built for runs that take many hours.** Each store is scraped in its own Scrapy subprocess, with configurable concurrency, retries and a hard per-store timeout. Progress is checkpointed to a `manifest.json` file, so an interrupted run can be **resumed** with `--resume-run-id`. A separate progress CLI shows live status.
- **Staging first, ingestion second.** Scraped rows are first written as NDJSON files on disk. They are then streamed into a Postgres staging table with `COPY`, in batches of 5,000 rows. One set-based SQL merge then updates the normalized tables. A failed database load never costs a re-scrape.
- **Price history without duplicates.** `current_prices` holds the latest snapshot. `price_changes` gets a new row only when a tracked field (`price`, `promo_price`, `unit_price`, `promo_unit_price`, `currency`, `available`) actually changes, using `IS DISTINCT FROM`. Each row records which fields changed, plus JSON snapshots of the price before and after.
- **Stable product identity across stores.** Products are keyed on the retailer's global product id. When that id is missing, the key is a SHA-256 hash of the normalized name, brand and pack size.
- **Two chains, one data model.** ICA (`kexscrape`) and Coop (`coopscrape`) share the same pipeline and the same database schema. Only the spider and the store catalog are specific to each chain.
- **Fuel prices as context.** A small extra scraper collects the national average prices of petrol 95, petrol 98 and diesel, for estimating what it costs to drive to a cheaper store.

## Architecture

```mermaid
flowchart LR
    subgraph Sources
        A1[ICA store catalog API]
        A2[Coop store and point-of-service APIs]
        W[Store web shops<br/>JSON product APIs]
    end

    subgraph Orchestrator["run_all_stores (thread pool)"]
        M[(manifest.json<br/>per-store state)]
        S1[Scrapy + Playwright<br/>subprocess · store 1]
        S2[subprocess · store 2]
        S3[subprocess · store N]
    end

    subgraph Postgres["PostgreSQL / Supabase"]
        ST[staging_scraped_prices]
        N[stores · products ·<br/>store_products]
        CP[current_prices]
        PC[price_changes]
    end

    A1 --> M
    A2 --> M
    M --> S1 & S2 & S3
    W -.-> S1 & S2 & S3
    S1 & S2 & S3 --> F[NDJSON files<br/>runs/&lt;run_id&gt;/]
    F -- "COPY in batches of 5,000" --> ST
    ST -- set-based merge --> N
    ST -- "diff with IS DISTINCT FROM" --> PC
    ST --> CP
```

1. **Discover stores.** The chain's store catalog API returns every store that has an online shop, with its id, name, address, coordinates, store format and delivery methods. These are written to a run manifest.
2. **Scrape each store.** A thread pool starts one Scrapy process per store (3 at a time by default). Each spider opens the store's web shop in headless Chromium, fetches the category tree, pages through the products in every leaf category, and fetches product details in concurrent batches. Each store writes one NDJSON file.
3. **Track state.** Every store moves through `PENDING → RUNNING → SUCCESS/FAILED` in the manifest, and in `crawl_store_status` when a database is configured. Failed stores are retried. Stores that were in progress when a run crashed go back to `PENDING` when the run is resumed.
4. **Ingest.** When the scraping is done, the NDJSON files are normalized and loaded with `COPY` into the staging table. A set of `INSERT … ON CONFLICT` merges then deduplicates the rows with `DISTINCT ON`, upserts stores, products and store assortments, logs price changes and refreshes the current prices. Finally the staging rows for the run are deleted.

## Data model

| Table | Purpose |
| --- | --- |
| `crawl_runs` | One row per run: start and end time, status, number of stores that succeeded and failed |
| `crawl_store_status` | Status for each store in a run: attempts, last error, output file |
| `staging_scraped_prices` | Temporary, write-optimized landing table for a run |
| `stores` | Store metadata: location, format, delivery methods, first and last seen |
| `products` | Chain-wide product data: name, brand, pack size, origin, ingredients, image |
| `store_products` | Which store carries which product, in which category, and whether it is available |
| `current_prices` | Latest price snapshot per (store, product) |
| `price_changes` | Append-only history of changes, with the fields that changed and snapshots before and after |

The full schema is in [`kexscrape/kexscrape/sql/schema.sql`](kexscrape/kexscrape/sql/schema.sql). Example query: the cheapest stores for one product.

```sql
SELECT p.name, s.store_name, s.city, cp.price, cp.promo_price, cp.unit_price
FROM current_prices cp
JOIN products p ON p.product_key = cp.product_key
JOIN stores   s ON s.store_id    = cp.store_id
WHERE cp.product_key = '<product_key>'
ORDER BY COALESCE(cp.promo_price, cp.price) ASC NULLS LAST
LIMIT 10;
```

## Tech stack

- **Python 3.10+**
- **Scrapy**: crawling framework, feed export to NDJSON
- **scrapy-playwright / Playwright (Chromium)**: a real browser session and API calls made from inside the page
- **PostgreSQL / Supabase**: storage, with `psycopg` 3 for `COPY`-based bulk loading
- **requests + psycopg2**: the lightweight fuel price scraper
- **unittest**: unit and integration tests for the spider parsing, product keys, price diffing, the store catalog and ingestion
- **PowerShell and cron**: scripts for test and full runs on Windows, and an example nightly schedule

## Project structure

```
matjakt-scraper/
├── kexscrape/                  # ICA pipeline
│   ├── kexscrape/
│   │   ├── spiders/ica_store_spider.py   # Scrapy + Playwright spider for one store
│   │   ├── store_catalog.py              # Fetches the list of stores with online shops
│   │   ├── run_all_stores.py             # Orchestrator: concurrency, retries, resume
│   │   ├── progress.py                   # Live progress CLI for a run
│   │   ├── ingestion.py                  # Normalizes raw records
│   │   ├── bulk_ingestion.py             # Staging → merge workflow
│   │   ├── postgres_repo.py              # COPY loading and the set-based merge SQL
│   │   ├── ingest_staged_run.py          # Re-ingests an existing run without scraping
│   │   ├── price_changes.py              # Price snapshots and diffs
│   │   ├── product_keys.py               # Stable product identity
│   │   └── sql/schema.sql
│   ├── tests/
│   └── ops/                    # run_test.ps1, run_full.ps1, cron.example
├── coopscrape/                 # Coop pipeline (same structure, Coop spider and catalog)
└── fuelscrape/scrape.py        # National average fuel prices
```

## Getting started

### Install

```bash
cd kexscrape            # or coopscrape
python -m venv .venv
source .venv/bin/activate
pip install scrapy scrapy-playwright "psycopg[binary]"
playwright install chromium
```

### Try a small run (no database needed)

```bash
python -m kexscrape.run_all_stores --stage-root runs --max-stores 1 --max-products 20
```

Without a DSN, the output is only staged as NDJSON under `runs/<run_id>/`.

### Run every store and load the database

```bash
export DATABASE_DSN="postgresql://user:pass@localhost:5432/matjakt"
python -m kexscrape.run_all_stores --stage-root runs --db-dsn "$DATABASE_DSN" --concurrency 3
```

The DSN can point to a local PostgreSQL database or to a Supabase session-mode pooler. The schema is created automatically.

### Monitor, resume, re-ingest

```bash
# Live progress
python -m kexscrape.progress --stage-root runs --watch --show-running

# Continue an interrupted run
python -m kexscrape.run_all_stores --stage-root runs --resume-run-id <run_id> --db-dsn "$DATABASE_DSN"

# Load an existing staged run without scraping again
python -m kexscrape.ingest_staged_run --run-id <run_id> --run-dir runs/<run_id> --db-dsn "$DATABASE_DSN"
```

For Coop, replace `kexscrape` with `coopscrape`. On Windows, `ops/run_test.ps1` and `ops/run_full.ps1` do the same thing and pick up `DATABASE_DSN` from the environment.

### Tests

```bash
cd kexscrape
python -m unittest discover tests
```

### Fuel prices

`fuelscrape/scrape.py` reads its DSN from a `connection_string.txt` file in the repository root (this file is gitignored).

```bash
pip install requests psycopg2-binary
python fuelscrape/scrape.py
```

## My role

The thesis was a group project. I designed and built the data-collection part: the scrapers, the orchestration, the ingestion pipeline and the database schema in this repository. The dataset it produced was used for the price analysis in the thesis.

## Disclaimer

This is an academic project. It is not affiliated with, endorsed by or connected to ICA, Coop or Bensinpriskollen. All product and price data belongs to its respective owners. The code is published to show the work done in the thesis. If you run it, check the sites' terms of use first, and keep the request rate low.
