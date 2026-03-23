CREATE TABLE IF NOT EXISTS crawl_runs (
    run_id TEXT PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    total_stores INTEGER NOT NULL DEFAULT 0,
    succeeded_stores INTEGER NOT NULL DEFAULT 0,
    failed_stores INTEGER NOT NULL DEFAULT 0,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS crawl_store_status (
    run_id TEXT NOT NULL REFERENCES crawl_runs(run_id) ON DELETE CASCADE,
    store_id TEXT NOT NULL,
    store_name TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    last_error TEXT,
    output_path TEXT,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, store_id)
);

CREATE TABLE IF NOT EXISTS staging_scraped_prices (
    staging_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    store_id TEXT NOT NULL,
    source_store_catalog_id TEXT NOT NULL DEFAULT '',
    store_name TEXT NOT NULL DEFAULT '',
    city TEXT NOT NULL DEFAULT '',
    zip_code TEXT NOT NULL DEFAULT '',
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    store_format TEXT NOT NULL DEFAULT '',
    delivery_methods JSONB NOT NULL DEFAULT '[]'::jsonb,
    category_id TEXT,
    category_name TEXT,
    category_path TEXT,
    retailer_product_id TEXT NOT NULL,
    product_id TEXT,
    product_key TEXT NOT NULL,
    name TEXT,
    brand TEXT,
    pack_size TEXT,
    country_of_origin TEXT,
    product_image_url TEXT,
    product_information TEXT,
    ingredients TEXT,
    unit TEXT,
    product_type TEXT,
    is_alcohol BOOLEAN,
    available BOOLEAN,
    is_new BOOLEAN,
    price NUMERIC(12, 4),
    promo_price NUMERIC(12, 4),
    unit_price NUMERIC(12, 4),
    promo_unit_price NUMERIC(12, 4),
    currency TEXT,
    observed_at TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stores (
    store_id TEXT PRIMARY KEY,
    source_store_catalog_id TEXT,
    store_name TEXT,
    city TEXT,
    zip_code TEXT,
    street TEXT,
    area TEXT,
    slug TEXT,
    store_format TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    delivery_methods JSONB NOT NULL DEFAULT '[]'::jsonb,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    product_key TEXT PRIMARY KEY,
    product_id TEXT,
    name TEXT,
    brand TEXT,
    pack_size TEXT,
    country_of_origin TEXT,
    product_image_url TEXT,
    product_information TEXT,
    ingredients TEXT,
    unit TEXT,
    product_type TEXT,
    is_alcohol BOOLEAN,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL
);

ALTER TABLE products
    ADD COLUMN IF NOT EXISTS product_image_url TEXT;

ALTER TABLE products
    ADD COLUMN IF NOT EXISTS product_information TEXT;

ALTER TABLE products
    ADD COLUMN IF NOT EXISTS ingredients TEXT;

CREATE TABLE IF NOT EXISTS store_products (
    store_id TEXT NOT NULL REFERENCES stores(store_id) ON DELETE CASCADE,
    product_key TEXT NOT NULL REFERENCES products(product_key) ON DELETE CASCADE,
    retailer_product_id TEXT NOT NULL,
    category_id TEXT,
    category_name TEXT,
    category_path TEXT,
    available BOOLEAN,
    is_new BOOLEAN,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (store_id, product_key),
    UNIQUE (store_id, retailer_product_id)
);

CREATE TABLE IF NOT EXISTS current_prices (
    store_id TEXT NOT NULL REFERENCES stores(store_id) ON DELETE CASCADE,
    product_key TEXT NOT NULL REFERENCES products(product_key) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES crawl_runs(run_id) ON DELETE CASCADE,
    observed_at TIMESTAMPTZ NOT NULL,
    price NUMERIC(12, 4),
    promo_price NUMERIC(12, 4),
    unit_price NUMERIC(12, 4),
    promo_unit_price NUMERIC(12, 4),
    currency TEXT,
    available BOOLEAN,
    PRIMARY KEY (store_id, product_key)
);

CREATE TABLE IF NOT EXISTS price_changes (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES crawl_runs(run_id) ON DELETE CASCADE,
    store_id TEXT NOT NULL REFERENCES stores(store_id) ON DELETE CASCADE,
    product_key TEXT NOT NULL REFERENCES products(product_key) ON DELETE CASCADE,
    observed_at TIMESTAMPTZ NOT NULL,
    price NUMERIC(12, 4),
    promo_price NUMERIC(12, 4),
    unit_price NUMERIC(12, 4),
    promo_unit_price NUMERIC(12, 4),
    currency TEXT,
    available BOOLEAN,
    change_fields TEXT[] NOT NULL,
    previous_snapshot JSONB NOT NULL,
    current_snapshot JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, store_id, product_key)
);

CREATE INDEX IF NOT EXISTS idx_current_prices_product_key_price
    ON current_prices(product_key, price);

CREATE INDEX IF NOT EXISTS idx_price_changes_store_product_observed
    ON price_changes(store_id, product_key, observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_staging_scraped_prices_run_id
    ON staging_scraped_prices(run_id);

CREATE INDEX IF NOT EXISTS idx_staging_scraped_prices_run_store_product_observed
    ON staging_scraped_prices(run_id, store_id, product_key, observed_at DESC, staging_id DESC);
