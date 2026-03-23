from __future__ import annotations

from decimal import Decimal
from typing import Any

TRACKED_PRICE_FIELDS = (
    "price",
    "promo_price",
    "unit_price",
    "promo_unit_price",
    "currency",
    "available",
)


def build_price_snapshot(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "price": _normalize_decimal(record.get("price")),
        "promo_price": _normalize_decimal(record.get("promo_price")),
        "unit_price": _normalize_decimal(record.get("unit_price")),
        "promo_unit_price": _normalize_decimal(record.get("promo_unit_price")),
        "currency": _normalize_str(record.get("currency")),
        "available": _normalize_bool(record.get("available")),
    }


def diff_price_fields(
    previous_snapshot: dict[str, Any],
    current_snapshot: dict[str, Any],
) -> list[str]:
    changed: list[str] = []
    for field_name in TRACKED_PRICE_FIELDS:
        if previous_snapshot.get(field_name) != current_snapshot.get(field_name):
            changed.append(field_name)
    return changed


def _normalize_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _normalize_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value).strip() or None


def _normalize_bool(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}
