from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any, Iterable
from urllib.request import Request, urlopen

ONLINE_STORES_URL = "https://external.api.coop.se/ecommerce/coop/stores?api-version=v1"
POINT_OF_SERVICE_URL = "https://external.api.coop.se/ecommerce/coop/pointofservices?api-version=v1&pageSize=1000"
SUBSCRIPTION_KEY = "3becf0ce306f41a1ae94077c16798187"


@dataclass(frozen=True)
class StoreCatalogEntry:
    store_id: str
    source_store_catalog_id: str
    store_name: str
    city: str
    zip_code: str
    street: str
    area: str
    slug: str
    store_format: str
    lat: float | None
    lon: float | None
    delivery_methods: tuple[str, ...]
    enabled: bool
    published: bool

    def to_metadata(self) -> dict[str, Any]:
        return {
            "store_id": self.store_id,
            "source_store_catalog_id": self.source_store_catalog_id,
            "store_name": self.store_name,
            "city": self.city,
            "zip_code": self.zip_code,
            "street": self.street,
            "area": self.area,
            "slug": self.slug,
            "store_format": self.store_format,
            "lat": self.lat,
            "lon": self.lon,
            "delivery_methods": list(self.delivery_methods),
            "enabled": self.enabled,
            "published": self.published,
        }

    def to_manifest_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["delivery_methods"] = list(self.delivery_methods)
        return data


def fetch_store_catalog(
    customer_type: str = "B2C",
    timeout_seconds: int = 30,
    *,
    only_active: bool = True,
) -> list[StoreCatalogEntry]:
    online_stores = _fetch_online_stores(timeout_seconds)
    pos_by_store_id = _fetch_point_of_service_map(timeout_seconds)

    entries: list[StoreCatalogEntry] = []
    for store in online_stores:
        store_id = str(store.get("code") or "").strip()
        if not store_id:
            continue

        store_name = str(store.get("name") or "").strip()
        store_format = _derive_store_format(store_name)

        delivery_modes = store.get("deliveryModes") or []
        delivery_methods = tuple(
            sorted(
                {
                    str(dm.get("code") or "").strip()
                    for dm in delivery_modes
                    if str(dm.get("code") or "").strip()
                }
            )
        )

        # Add pickup modes from point-of-service data
        pos_entries = pos_by_store_id.get(store_id, [])
        for pos in pos_entries:
            for pm in pos.get("pickupDeliveryModes") or []:
                code = str(pm.get("code") or "").strip()
                if code:
                    delivery_methods = tuple(sorted(set(delivery_methods) | {code}))

        # Get location from the first matching point-of-service
        city = ""
        zip_code = ""
        street = ""
        lat = None
        lon = None
        if pos_entries:
            first_pos = pos_entries[0]
            address = first_pos.get("address") or {}
            city = str(address.get("town") or "").strip()
            zip_code = str(address.get("postalCode") or "").strip()
            street = str(address.get("line1") or "").strip()
            geo = first_pos.get("geoPoint") or {}
            lat = _to_float(geo.get("latitude"))
            lon = _to_float(geo.get("longitude"))

        entries.append(
            StoreCatalogEntry(
                store_id=store_id,
                source_store_catalog_id=store_id,
                store_name=store_name,
                city=city,
                zip_code=zip_code,
                street=street,
                area="",
                slug="",
                store_format=store_format,
                lat=lat,
                lon=lon,
                delivery_methods=delivery_methods,
                enabled=True,
                published=True,
            )
        )

    entries.sort(key=lambda e: e.store_id)
    return entries


def _fetch_online_stores(timeout_seconds: int) -> list[dict[str, Any]]:
    req = Request(ONLINE_STORES_URL)
    req.add_header("Accept", "application/json")
    req.add_header("Ocp-Apim-Subscription-Key", SUBSCRIPTION_KEY)

    with urlopen(req, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))

    return payload.get("stores") or []


def _fetch_point_of_service_map(
    timeout_seconds: int,
) -> dict[str, list[dict[str, Any]]]:
    req = Request(POINT_OF_SERVICE_URL)
    req.add_header("Accept", "application/json")
    req.add_header("Ocp-Apim-Subscription-Key", SUBSCRIPTION_KEY)

    with urlopen(req, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))

    result: dict[str, list[dict[str, Any]]] = {}
    for store in payload.get("stores") or []:
        store_id = str(store.get("storeId") or "").strip()
        if store_id:
            result.setdefault(store_id, []).append(store)

    return result


def _derive_store_format(store_name: str) -> str:
    name_lower = store_name.lower()
    if "stora coop" in name_lower:
        return "stora_coop"
    if "coop extra" in name_lower:
        return "coop_extra"
    if "coop nära" in name_lower or "coop nara" in name_lower:
        return "coop_nara"
    return "coop"


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
