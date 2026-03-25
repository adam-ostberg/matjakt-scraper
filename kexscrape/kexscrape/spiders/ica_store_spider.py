import json
import re
from html import unescape
from datetime import datetime, timezone

import scrapy


class IcaStoreSpider(scrapy.Spider):
    name = "ica_store"
    allowed_domains = ["handlaprivatkund.ica.se"]

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
        store_id="1003833",
        run_id="",
        store_name="",
        city="",
        zip_code="",
        lat="",
        lon="",
        store_format="",
        delivery_methods="",
        source_store_catalog_id="",
        category_depth="8",
        max_pages_per_category="100",
        max_products=None,
        max_categories=None,
        include_non_leaf="false",
        api_timeout_ms="30000",
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
        self.category_depth = int(category_depth)
        self.max_pages_per_category = int(max_pages_per_category)
        self.max_products = int(max_products) if max_products else None
        self.max_categories = int(max_categories) if max_categories else None
        self.api_timeout_ms = max(1000, int(api_timeout_ms))
        self.include_non_leaf = str(include_non_leaf).lower() in {
            "1",
            "true",
            "yes",
            "y",
        }
        self.start_urls = [f"https://handlaprivatkund.ica.se/stores/{self.store_id}"]

    async def start(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_context": "ica_store_ctx",
                    "playwright_page_goto_kwargs": {
                        "wait_until": "networkidle",
                        "timeout": 120000,
                    },
                },
            )

    async def parse(self, response):
        page = response.meta["playwright_page"]
        seen_retailer_ids = set()
        scraped_count = 0

        try:
            categories = await self._fetch_json(
                page,
                f"/stores/{self.store_id}/api/webproductpagews/v1/categories"
                f"?decoration=true&categoryDepth={self.category_depth}",
            )
            if not categories:
                self.logger.error("Could not fetch categories")
                return

            categories_to_visit = self._build_categories_to_visit(categories)
            if self.max_categories:
                categories_to_visit = categories_to_visit[: self.max_categories]

            self.logger.info("Visiting %d categories", len(categories_to_visit))

            for idx, category in enumerate(categories_to_visit, start=1):
                category_id = category.get("categoryId")
                if not category_id:
                    continue

                category_name = category.get("name", "")
                self.logger.info(
                    "[%d/%d] Category: %s (%s)",
                    idx,
                    len(categories_to_visit),
                    category_name,
                    category_id,
                )

                page_token = None
                for page_no in range(1, self.max_pages_per_category + 1):
                    category_payload = await self._fetch_category_products(
                        page=page,
                        category_id=category_id,
                        page_token=page_token,
                    )
                    if not category_payload:
                        break

                    for product in self._iter_products(category_payload):
                        retailer_product_id = str(
                            product.get("retailerProductId") or ""
                        ).strip()
                        if not retailer_product_id:
                            continue
                        if retailer_product_id in seen_retailer_ids:
                            continue
                        seen_retailer_ids.add(retailer_product_id)
                        scraped_count += 1
                        detail_payload = await self._fetch_product_details(
                            page=page,
                            retailer_product_id=retailer_product_id,
                        )
                        yield self._to_item(category, product, detail_payload)
                        if self.max_products and scraped_count >= self.max_products:
                            self.logger.info(
                                "Reached max_products=%d for store %s",
                                self.max_products,
                                self.store_id,
                            )
                            break

                    page_token = (
                        category_payload.get("metadata") or {}
                    ).get("nextPageToken")
                    if self.max_products and scraped_count >= self.max_products:
                        break
                    if not page_token:
                        break

                if self.max_products and scraped_count >= self.max_products:
                    break

            self.logger.info(
                "Finished. Unique products scraped: %d", scraped_count
            )
        finally:
            await page.close()

    async def _fetch_category_products(self, page, category_id, page_token=None):
        params = [
            ("categoryId", category_id),
            ("tag", "web"),
            ("tag", "category-item"),
            ("includeAdditionalPageInfo", "true"),
            ("maxProductsToDecorate", "50"),
            ("maxPageSize", "300"),
        ]
        if page_token:
            params.append(("pageToken", page_token))
        query = "&".join(f"{k}={v}" for k, v in params)
        path = f"/stores/{self.store_id}/api/webproductpagews/v6/product-pages?{query}"
        return await self._fetch_json(page, path)

    async def _fetch_product_details(self, page, retailer_product_id):
        path = (
            f"/stores/{self.store_id}/api/webproductpagews/v5/products/bop"
            f"?retailerProductId={retailer_product_id}"
        )
        return await self._fetch_json(page, path)

    async def _fetch_json(self, page, path, retries=4):
        for attempt in range(1, retries + 1):
            result = await page.evaluate(
                """
                async ({ path, timeoutMs }) => {
                    const controller = new AbortController();
                    const timeoutHandle = setTimeout(() => controller.abort(), timeoutMs);
                    try {
                        const response = await fetch(path, {
                            method: "GET",
                            credentials: "include",
                            headers: { "accept": "application/json, text/plain, */*" },
                            signal: controller.signal,
                        });
                        return { status: response.status, text: await response.text() };
                    } catch (error) {
                        return { status: 0, text: String(error) };
                    } finally {
                        clearTimeout(timeoutHandle);
                    }
                }
                """,
                {"path": path, "timeoutMs": self.api_timeout_ms},
            )

            status = result.get("status")
            text = result.get("text") or ""

            if status == 200:
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    self.logger.warning("JSON decode failed for %s", path)
                    return None

            # AWS WAF challenge in progress: wait, then retry in same browser context.
            if status in (0, 202, 403):
                await page.wait_for_timeout(1000 * attempt)
                continue

            self.logger.warning(
                "Unexpected status %s for %s (attempt %d/%d)",
                status,
                path,
                attempt,
                retries,
            )
            await page.wait_for_timeout(500 * attempt)

        self.logger.warning("Failed to fetch %s after %d retries", path, retries)
        return None

    def _build_categories_to_visit(self, categories):
        selected_by_id = {}

        def walk(node):
            if not isinstance(node, dict):
                return

            category_id = str(node.get("categoryId") or "").strip()
            children = node.get("childCategories") or []
            product_count = int(node.get("productCount") or 0)
            is_leaf = len(children) == 0

            if (
                category_id
                and product_count > 0
                and (self.include_non_leaf or is_leaf)
            ):
                existing = selected_by_id.get(category_id)
                if existing is None or product_count > int(existing.get("productCount") or 0):
                    selected_by_id[category_id] = node

            for child in children:
                walk(child)

        for root in categories:
            walk(root)

        selected = list(selected_by_id.values())
        # Prefer categories with larger product counts first.
        selected.sort(key=lambda c: int(c.get("productCount") or 0), reverse=True)
        return selected

    @staticmethod
    def _iter_products(category_payload):
        for group in category_payload.get("productGroups") or []:
            products = group.get("decoratedProducts") or group.get("products") or []
            for product in products:
                if isinstance(product, dict):
                    yield product

    def _to_item(self, category, product, detail_payload=None):
        now = datetime.now(timezone.utc).isoformat()
        category_path = " > ".join(
            crumb.get("name", "")
            for crumb in (category.get("breadcrumbs") or [])
            if isinstance(crumb, dict)
        )

        price = product.get("price") or {}
        unit_price = product.get("unitPrice") or {}
        unit_price_money = unit_price.get("price") or {}
        promo_price = product.get("promoPrice") or {}
        promo_unit_price = product.get("promoUnitPrice") or {}
        bop_data = (detail_payload or {}).get("bopData") or {}
        detail_product = (detail_payload or {}).get("product") or {}

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
            "category_id": category.get("categoryId"),
            "category_name": category.get("name"),
            "category_path": category_path,
            "retailer_product_id": product.get("retailerProductId"),
            "product_id": product.get("productId"),
            "name": product.get("name"),
            "brand": product.get("brand"),
            "pack_size": product.get("packSizeDescription"),
            "country_of_origin": product.get("countryOfOrigin"),
            "product_image_url": self._extract_product_image_url(
                product, detail_product
            ),
            "product_information": self._clean_html_text(
                bop_data.get("detailedDescription")
            ),
            "ingredients": self._extract_bop_field(bop_data.get("fields"), "ingredients"),
            "price": price.get("amount"),
            "currency": price.get("currency"),
            "unit_price": unit_price_money.get("amount"),
            "unit": unit_price.get("unit"),
            "promo_price": promo_price.get("amount"),
            "promo_unit_price": promo_unit_price.get("amount"),
            "available": product.get("available"),
            "is_new": product.get("isNew"),
            "is_alcohol": product.get("alcohol"),
            "product_type": product.get("type"),
            "scraped_at_utc": now,
        }

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

    @classmethod
    def _extract_product_image_url(cls, product, detail_product):
        for source in (product, detail_product):
            images = source.get("images") or []
            if images:
                image_url = (images[0] or {}).get("src")
                if image_url:
                    return image_url

            image_url = (source.get("image") or {}).get("src")
            if image_url:
                return image_url

        return None

    @classmethod
    def _extract_bop_field(cls, fields, title):
        for field in fields or []:
            if not isinstance(field, dict):
                continue
            if field.get("title") == title:
                return cls._clean_html_text(field.get("content"))
        return None

    @staticmethod
    def _clean_html_text(value):
        if value in (None, ""):
            return None

        text = str(value)
        text = re.sub(r"(?i)<\s*br\s*/?\s*>", "\n", text)
        text = re.sub(r"(?i)</\s*(p|div|li|h[1-6]|tr|td|th)\s*>", "\n", text)
        text = re.sub(r"<[^>]+>", "", text)
        text = unescape(text)

        lines = [" ".join(line.split()) for line in text.splitlines()]
        cleaned = "\n".join(line for line in lines if line)
        return cleaned or None
