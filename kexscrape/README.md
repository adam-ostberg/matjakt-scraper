# ICA All-Store Scrape

## Windows Quick Start

PowerShell:

```powershell
cd C:\path\to\kex-scrape\kexscrape
py -3 -m venv .venv
.venv\Scripts\python -m pip install -U pip
.venv\Scripts\python -m pip install scrapy scrapy-playwright "psycopg[binary]"
.venv\Scripts\python -m playwright install chromium
```

Set your DSN once per shell:

```powershell
$env:DATABASE_DSN = "postgresql://postgres.<project-ref>:<password>@aws-0-<region>.pooler.supabase.com:5432/postgres"
```

Small test run:

```powershell
.\ops\run_test.ps1
```

Full run:

```powershell
.\ops\run_full.ps1
```

Both scripts also work without `DATABASE_DSN`; then they only stage NDJSON under `runs\`.

## Run All Stores (staging only)

```bash
cd /Users/adam/repos/kth/kex-scrape/kexscrape
python -m kexscrape.run_all_stores --stage-root runs
```

You can also limit products per store during test runs:

```bash
cd /Users/adam/repos/kth/kex-scrape/kexscrape
python -m kexscrape.run_all_stores --stage-root runs --max-stores 1 --max-products 20
```

## Run All Stores + PostgreSQL or Supabase Ingestion

```bash
cd /Users/adam/repos/kth/kex-scrape/kexscrape
python -m kexscrape.run_all_stores \
  --stage-root runs \
  --db-dsn "postgresql://user:pass@localhost:5432/ica_prices"
```

## Resume Partial Run

```bash
cd /Users/adam/repos/kth/kex-scrape/kexscrape
python -m kexscrape.run_all_stores --stage-root runs --resume-run-id <run_id>
```

## Track Run Progress

Single snapshot:

```bash
cd /Users/adam/repos/kth/kex-scrape/kexscrape
python -m kexscrape.progress --stage-root runs
```

Live watch:

```bash
cd /Users/adam/repos/kth/kex-scrape/kexscrape
python -m kexscrape.progress --stage-root runs --watch --show-running
```

## Ingest Existing Staged Run Only

```bash
cd /Users/adam/repos/kth/kex-scrape/kexscrape
python -m kexscrape.ingest_staged_run \
  --run-id <run_id> \
  --run-dir runs/<run_id> \
  --db-dsn "postgresql://user:pass@localhost:5432/ica_prices"
```

`--db-dsn` can point to a local PostgreSQL database or a Supabase Postgres/Supavisor session-mode connection string.

## Example Price Comparison Query

```sql
SELECT
  cp.product_key,
  p.name,
  s.store_name,
  cp.price,
  cp.promo_price,
  cp.currency,
  cp.observed_at
FROM current_prices cp
JOIN products p ON p.product_key = cp.product_key
JOIN stores s ON s.store_id = cp.store_id
WHERE cp.product_key = 'abecf442-a7c5-4c7a-9ab3-0d76c42bdd26'
ORDER BY cp.price ASC NULLS LAST;
```
