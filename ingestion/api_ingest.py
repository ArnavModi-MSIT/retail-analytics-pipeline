"""
Daily incremental ingestion — DummyJSON /carts endpoint.
Separate landing path from the CSV historical bulk load (different schema on purpose;
forces real schema validation/normalization in the PySpark transform step before merge).

Writes raw flattened JSON to data/raw/api/carts_<run_date>.json via LocalBackend's
path convention. Not a Spark DataFrame at this stage — plain Python I/O, PySpark
reads it in the transform step alongside the CSV source.
"""

import json
from datetime import date, datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter, Retry

from storage_backend import get_backend

API_BASE = "https://dummyjson.com"
PAGE_LIMIT = 30


def _session_with_retries() -> requests.Session:
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def fetch_all_carts(session: requests.Session) -> list[dict]:
    carts = []
    skip = 0
    while True:
        resp = session.get(f"{API_BASE}/carts", params={"limit": PAGE_LIMIT, "skip": skip}, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
        carts.extend(payload["carts"])
        skip += PAGE_LIMIT
        if skip >= payload["total"]:
            break
    return carts


def flatten_carts(carts: list[dict], fetched_at: str) -> list[dict]:
    rows = []
    for cart in carts:
        for product in cart["products"]:
            rows.append({
                "cart_id": cart["id"],
                "user_id": cart["userId"],
                "product_id": product["id"],
                "title": product["title"],
                "price": product["price"],
                "quantity": product["quantity"],
                "discount_percentage": product.get("discountPercentage", 0.0),
                "discounted_total": product.get("discountedTotal", product["price"] * product["quantity"]),
                "fetched_at": fetched_at,
            })
    return rows


def write_raw(rows: list[dict], run_date: date) -> str:
    backend = get_backend()  # local by default; swap via config later
    rel_path = f"raw/api/carts_{run_date.isoformat()}.json"
    full_path = backend.resolve_path(rel_path)
    Path(full_path).parent.mkdir(parents=True, exist_ok=True)

    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)

    return full_path


def main():
    run_date = date.today()
    fetched_at = datetime.now(timezone.utc).isoformat()

    session = _session_with_retries()
    carts = fetch_all_carts(session)
    rows = flatten_carts(carts, fetched_at)

    out_path = write_raw(rows, run_date)
    print(f"Ingested {len(rows)} line items from {len(carts)} carts -> {out_path}")


if __name__ == "__main__":
    main()
