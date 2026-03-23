from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any, Iterable
from urllib.request import urlopen

STORE_CATALOG_URL = "https://handla.ica.se/api/store/v1?groupby=citygroup&customerType={customer_type}"


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

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "StoreCatalogEntry":
        account_id = str(payload.get("accountId") or "").strip()
        if not account_id:
            raise ValueError("Missing accountId in store payload")

        delivery_methods = payload.get("deliveryMethods") or []
        normalized_delivery_methods = tuple(
            sorted(
                {
                    str(value).strip()
                    for value in delivery_methods
                    if str(value).strip()
                }
            )
        )

        return cls(
            store_id=account_id,
            source_store_catalog_id=str(payload.get("id") or "").strip(),
            store_name=str(payload.get("name") or "").strip(),
            city=str(payload.get("city") or "").strip(),
            zip_code=str(payload.get("zipCode") or "").strip(),
            street=str(payload.get("street") or "").strip(),
            area=str(payload.get("area") or "").strip(),
            slug=str(payload.get("slug") or "").strip(),
            store_format=str(payload.get("storeFormat") or "").strip(),
            lat=_to_float(payload.get("latitude")),
            lon=_to_float(payload.get("longitude")),
            delivery_methods=normalized_delivery_methods,
            enabled=_to_bool(payload.get("enable")),
            published=_to_bool(payload.get("published")),
        )

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
    customer_type = customer_type.upper().strip() or "B2C"
    url = STORE_CATALOG_URL.format(customer_type=customer_type)

    with urlopen(url, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))

    stores = flatten_store_catalog_payload(payload)
    if only_active:
        stores = [entry for entry in stores if entry.enabled and entry.published]

    return stores


def flatten_store_catalog_payload(payload: Any) -> list[StoreCatalogEntry]:
    by_store_id: dict[str, StoreCatalogEntry] = {}

    for node in _iter_store_nodes(payload):
        try:
            entry = StoreCatalogEntry.from_payload(node)
        except ValueError:
            continue
        by_store_id.setdefault(entry.store_id, entry)

    return sorted(by_store_id.values(), key=lambda entry: entry.store_id)


def _iter_store_nodes(node: Any) -> Iterable[dict[str, Any]]:
    if isinstance(node, dict):
        if "accountId" in node:
            yield node
            return
        for value in node.values():
            yield from _iter_store_nodes(value)
        return

    if isinstance(node, list):
        for value in node:
            yield from _iter_store_nodes(value)


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}
