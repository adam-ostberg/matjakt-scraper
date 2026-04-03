from __future__ import annotations

import hashlib
import re

_whitespace_re = re.compile(r"\s+")


def build_product_key(
    product_id: str | None,
    name: str | None,
    brand: str | None,
    pack_size: str | None,
) -> str:
    if product_id and str(product_id).strip():
        return str(product_id).strip()

    normalized = "|".join(
        [
            _normalize_component(name),
            _normalize_component(brand),
            _normalize_component(pack_size),
        ]
    )
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return f"fallback:{digest}"


def _normalize_component(value: str | None) -> str:
    if value is None:
        return ""
    compact = _whitespace_re.sub(" ", str(value)).strip().lower()
    return compact
