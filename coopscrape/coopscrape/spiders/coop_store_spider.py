import asyncio
import json
from datetime import datetime, timezone

import scrapy

SUBSCRIPTION_KEY = "3becf0ce306f41a1ae94077c16798187"
ECOMMERCE_BASE = "https://external.api.coop.se/ecommerce"
PERSONALIZATION_BASE = "https://external.api.coop.se/personalization"


class CoopStoreSpider(scrapy.Spider):
    name = "coop_store"
    allowed_domains = ["coop.se", "external.api.coop.se"]

    FEED_FIELDS = [
        "run_id",
        "store_id",
        "store_name",
        "city",
        "zip_code",
        "lat",
        "lon",
        "store_format",
        "delivery_methods",
        "source_store_catalog_id",
        "category_id",
        "category_name",
        "category_path",
        "retailer_product_id",
        "product_id",
        "name",
        "brand",
        "pack_size",
        "country_of_origin",
        "product_image_url",
        "product_information",
        "ingredients",
        "price",
        "currency",
        "unit_price",
        "unit",
        "promo_price",
        "promo_unit_price",
        "available",
        "is_new",
        "is_alcohol",
        "product_type",
        "scraped_at_utc",
    ]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "USER_AGENT": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 120000,
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 0.25,
        "LOG_LEVEL": "INFO",
        "FEED_EXPORT_FIELDS": FEED_FIELDS,
        "FEED_EXPORT_ENCODING": "utf-8",
    }

    def __init__(
        self,
        store_id="251300",
        run_id="",
        store_name="",
        city="",
        zip_code="",
        lat="",
        lon="",
        store_format="",
        delivery_methods="",
        source_store_catalog_id="",
        max_products=None,
        max_categories=None,
        page_size="48",
        api_timeout_ms="30000",
        category_concurrency="6",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.store_id = str(store_id)
        self.run_id = str(run_id).strip()
        self.store_name = str(store_name).strip()
        self.city = str(city).strip()
        self.zip_code = str(zip_code).strip()
        self.lat = self._to_float_or_none(lat)
        self.lon = self._to_float_or_none(lon)
        self.store_format = str(store_format).strip()
        self.delivery_methods = self._parse_delivery_methods(delivery_methods)
        self.source_store_catalog_id = str(source_store_catalog_id).strip()
        self.max_products = int(max_products) if max_products else None
        self.max_categories = int(max_categories) if max_categories else None
        self.page_size = max(1, int(page_size))
        self.api_timeout_ms = max(1000, int(api_timeout_ms))
        self.category_concurrency = max(1, int(category_concurrency))
        self.start_urls = ["https://www.coop.se/handla/"]

    async def start(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_context": "coop_store_ctx",
                    "playwright_page_goto_kwargs": {
                        "wait_until": "networkidle",
                        "timeout": 120000,
                    },
                },
            )

    async def parse(self, response):
        page = response.meta["playwright_page"]
        seen_eans = set()
        scraped_count = 0

        try:
            categories = await self._fetch_json(
                page,
                f"{ECOMMERCE_BASE}/coop/users/anonymous/categories/tree/{self.store_id}?api-version=v1",
            )
            if not categories:
                self.logger.error("Could not fetch categories for store %s", self.store_id)
                return

            leaf_categories = self._build_leaf_categories(categories.get("nodes", []))
            if self.max_categories:
                leaf_categories = leaf_categories[: self.max_categories]

            self.logger.info(
                "Visiting %d leaf categories (concurrency=%d)",
                len(leaf_categories),
                self.category_concurrency,
            )

            # Process categories in batches, fetching first page of each concurrently.
            for batch_start in range(0, len(leaf_categories), self.category_concurrency):
                batch = leaf_categories[batch_start : batch_start + self.category_concurrency]

                self.logger.info(
                    "Batch [%d-%d/%d]: fetching first page of %d categories",
                    batch_start + 1,
                    min(batch_start + self.category_concurrency, len(leaf_categories)),
                    len(leaf_categories),
                    len(batch),
                )

                # Fire first-page fetch for all categories in the batch concurrently.
                first_pages = await asyncio.gather(*[
                    self._fetch_category_products(
                        page=page,
                        category_code=cat["code"],
                        category_name=cat["name"],
                        category_url=cat.get("url", ""),
                        skip=0,
                    )
                    for cat in batch
                ])

                for category, payload in zip(batch, first_pages):
                    if not payload:
                        continue

                    results = payload.get("results") or {}
                    items = results.get("items") or []
                    total_count = results.get("count", 0)

                    # Yield products from the first page.
                    for product in items:
                        ean = str(product.get("ean") or "").strip()
                        if not ean or ean in seen_eans:
                            continue
                        seen_eans.add(ean)
                        scraped_count += 1
                        yield self._to_item(category, product)
                        if self.max_products and scraped_count >= self.max_products:
                            break

                    if self.max_products and scraped_count >= self.max_products:
                        break

                    # Fetch any remaining pages for this category sequentially.
                    skip = self.page_size
                    while skip < total_count:
                        next_payload = await self._fetch_category_products(
                            page=page,
                            category_code=category["code"],
                            category_name=category["name"],
                            category_url=category.get("url", ""),
                            skip=skip,
                        )
                        if not next_payload:
                            break
                        for product in (next_payload.get("results") or {}).get("items") or []:
                            ean = str(product.get("ean") or "").strip()
                            if not ean or ean in seen_eans:
                                continue
                            seen_eans.add(ean)
                            scraped_count += 1
                            yield self._to_item(category, product)
                            if self.max_products and scraped_count >= self.max_products:
                                break
                        if self.max_products and scraped_count >= self.max_products:
                            break
                        skip += self.page_size

                    if self.max_products and scraped_count >= self.max_products:
                        break

                if self.max_products and scraped_count >= self.max_products:
                    self.logger.info(
                        "Reached max_products=%d for store %s",
                        self.max_products,
                        self.store_id,
                    )
                    break

            self.logger.info("Finished. Unique products scraped: %d", scraped_count)
        finally:
            await page.close()

    async def _fetch_category_products(
        self, page, category_code, category_name, category_url, skip=0
    ):
        body = {
            "attribute": {"name": "categoryIds", "value": category_code},
            "requestAlias": {
                "name": "Subcategory",
                "value": category_name,
                "details": category_url,
            },
            "resultsOptions": {
                "skip": skip,
                "take": self.page_size,
                "sortBy": [],
                "facets": [],
            },
            "customData": {
                "getEntitiesByAttributeABTest": False,
                "consent": False,
            },
        }
        url = (
            f"{PERSONALIZATION_BASE}/search/entities/by-attribute"
            f"?api-version=v1&store={self.store_id}"
            f"&groups=CUSTOMER_PRIVATE&device=desktop&direct=false"
        )
        return await self._fetch_json_post(page, url, body)

    async def _fetch_json(self, page, url, retries=4):
        for attempt in range(1, retries + 1):
            result = await page.evaluate(
                """
                async ({ url, subscriptionKey, timeoutMs }) => {
                    const controller = new AbortController();
                    const timer = setTimeout(() => controller.abort(), timeoutMs);
                    try {
                        const resp = await fetch(url, {
                            method: "GET",
                            headers: {
                                "Accept": "application/json",
                                "Ocp-Apim-Subscription-Key": subscriptionKey
                            },
                            signal: controller.signal,
                        });
                        return { status: resp.status, text: await resp.text() };
                    } catch (e) {
                        return { status: 0, text: String(e) };
                    } finally {
                        clearTimeout(timer);
                    }
                }
                """,
                {
                    "url": url,
                    "subscriptionKey": SUBSCRIPTION_KEY,
                    "timeoutMs": self.api_timeout_ms,
                },
            )

            status = result.get("status")
            text = result.get("text") or ""

            if status == 200:
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    self.logger.warning("JSON decode failed for %s", url)
                    return None

            if status in (0, 202, 403, 429):
                await page.wait_for_timeout(1000 * attempt)
                continue

            self.logger.warning(
                "Unexpected status %s for %s (attempt %d/%d)",
                status, url, attempt, retries,
            )
            await page.wait_for_timeout(500 * attempt)

        self.logger.warning("Failed to fetch %s after %d retries", url, retries)
        return None

    async def _fetch_json_post(self, page, url, body, retries=4):
        for attempt in range(1, retries + 1):
            result = await page.evaluate(
                """
                async ({ url, subscriptionKey, body, timeoutMs }) => {
                    const controller = new AbortController();
                    const timer = setTimeout(() => controller.abort(), timeoutMs);
                    try {
                        const resp = await fetch(url, {
                            method: "POST",
                            headers: {
                                "Content-Type": "application/json",
                                "Accept": "application/json",
                                "Ocp-Apim-Subscription-Key": subscriptionKey
                            },
                            body: JSON.stringify(body),
                            signal: controller.signal,
                        });
                        const text = await resp.text();
                        return { status: resp.status, text: text };
                    } catch (e) {
                        return { status: 0, text: String(e) };
                    } finally {
                        clearTimeout(timer);
                    }
                }
                """,
                {
                    "url": url,
                    "subscriptionKey": SUBSCRIPTION_KEY,
                    "body": body,
                    "timeoutMs": self.api_timeout_ms,
                },
            )

            status = result.get("status")
            text = result.get("text") or ""

            if status == 200:
                if not text:
                    return None
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    self.logger.warning("JSON decode failed for POST %s", url)
                    return None

            if status in (0, 202, 403, 429):
                await page.wait_for_timeout(1000 * attempt)
                continue

            self.logger.warning(
                "Unexpected status %s for POST %s (attempt %d/%d)",
                status, url, attempt, retries,
            )
            await page.wait_for_timeout(500 * attempt)

        self.logger.warning("Failed POST %s after %d retries", url, retries)
        return None

    def _build_leaf_categories(self, nodes, parent_path=""):
        leaf_categories = []

        for node in nodes:
            if "name" not in node or "code" not in node:
                continue

            name = node["name"]
            code = str(node["code"])
            url = node.get("url", "")
            current_path = f"{parent_path} > {name}" if parent_path else name
            children = node.get("children") or []

            if not children:
                leaf_categories.append({
                    "code": code,
                    "name": name,
                    "path": current_path,
                    "url": url,
                })
            else:
                leaf_categories.extend(
                    self._build_leaf_categories(children, current_path)
                )

        return leaf_categories

    def _to_item(self, category, product):
        now = datetime.now(timezone.utc).isoformat()

        sales_price = product.get("salesPriceData") or {}
        comparative_price = product.get("comparativePriceData") or {}
        comparative_unit = product.get("comparativePriceUnit") or {}

        # Build category path from navCategories
        category_path = category.get("path", "")
        nav_cats = product.get("navCategories") or []
        if nav_cats:
            cat_chain = self._build_nav_category_path(nav_cats[0])
            if cat_chain:
                category_path = cat_chain

        # Country of origin
        country_of_origin = None
        origin_codes = product.get("countryOfOriginCodes") or []
        if origin_codes:
            country_of_origin = origin_codes[0].get("value")

        # Image URL
        image_url = product.get("imageUrl")
        if image_url and image_url.startswith("http://"):
            image_url = "https://" + image_url[7:]

        return {
            "run_id": self.run_id,
            "store_id": self.store_id,
            "store_name": self.store_name,
            "city": self.city,
            "zip_code": self.zip_code,
            "lat": self.lat,
            "lon": self.lon,
            "store_format": self.store_format,
            "delivery_methods": self.delivery_methods,
            "source_store_catalog_id": self.source_store_catalog_id,
            "category_id": category.get("code"),
            "category_name": category.get("name"),
            "category_path": category_path,
            "retailer_product_id": product.get("ean"),
            "product_id": product.get("ean"),
            "name": product.get("name"),
            "brand": product.get("manufacturerName"),
            "pack_size": product.get("packageSizeInformation"),
            "country_of_origin": country_of_origin,
            "product_image_url": image_url,
            "product_information": product.get("description"),
            "ingredients": product.get("listOfIngredients"),
            "price": sales_price.get("b2cPrice"),
            "currency": "SEK",
            "unit_price": comparative_price.get("b2cPrice"),
            "unit": comparative_unit.get("text") or product.get("comparativePriceText"),
            "promo_price": None,
            "promo_unit_price": None,
            "available": product.get("availableOnline"),
            "is_new": None,
            "is_alcohol": None,
            "product_type": product.get("salesUnit"),
            "scraped_at_utc": now,
        }

    @staticmethod
    def _build_nav_category_path(nav_category):
        parts = []
        current = nav_category
        while current:
            name = current.get("name")
            if name:
                parts.append(name)
            supers = current.get("superCategories") or []
            current = supers[0] if supers else None
        parts.reverse()
        return " > ".join(parts) if parts else ""

    @staticmethod
    def _to_float_or_none(value):
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_delivery_methods(value):
        if value in (None, ""):
            return []
        if isinstance(value, list):
            return sorted({str(item).strip() for item in value if str(item).strip()})
        return sorted(
            {item.strip() for item in str(value).split(",") if item.strip()}
        )
