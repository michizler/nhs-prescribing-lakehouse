"""Pulls a small sample of the English Prescribing Dataset from the NHSBSA open data API into a CSV in the
published layout, for local development and test fixtures. The full monthly files are ~7 GB each, so
development never needs them.

Usage:
    python scripts/fetch_sample.py --months 202505 202506 --where "PCO_CODE = '00T00'" --limit 5000 --out sample.csv
"""
from __future__ import annotations

import argparse
import csv
import sys

import requests

API = "https://opendata.nhsbsa.net/api/3/action/datastore_search_sql"
COLUMNS = [
    "YEAR_MONTH", "REGIONAL_OFFICE_NAME", "REGIONAL_OFFICE_CODE", "ICB_NAME", "ICB_CODE", "PCO_NAME", "PCO_CODE",
    "PRACTICE_NAME", "PRACTICE_CODE", "ADDRESS_1", "ADDRESS_2", "ADDRESS_3", "ADDRESS_4", "POSTCODE",
    "BNF_CHEMICAL_SUBSTANCE", "CHEMICAL_SUBSTANCE_BNF_DESCR", "BNF_CODE", "BNF_DESCRIPTION", "BNF_CHAPTER_PLUS_CODE",
    "QUANTITY", "ITEMS", "TOTAL_QUANTITY", "ADQUSAGE", "NIC", "ACTUAL_COST", "UNIDENTIFIED",
]


def query(resource: str, sql: str, session: requests.Session | None = None) -> list[dict]:
    """Runs one SQL query against a monthly resource. The API nests its result one level deeper than CKAN's docs say."""
    s = session or requests.Session()
    r = s.get(API, params={"resource_id": resource, "sql": sql}, timeout=300)
    r.raise_for_status()
    body = r.json()
    if not body.get("success"):
        raise RuntimeError(body.get("error"))
    return body["result"]["result"]["records"]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--months", nargs="+", required=True, help="YYYYMM values")
    ap.add_argument("--where", default="1 = 1", help="SQL filter, e.g. \"PCO_CODE = '00T00'\"")
    ap.add_argument("--limit", type=int, default=5000, help="rows per month")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    rows: list[dict] = []
    with requests.Session() as s:
        for m in args.months:
            res = f"EPD_{m}"
            got = query(res, f"SELECT {', '.join(COLUMNS)} FROM `{res}` WHERE {args.where} LIMIT {args.limit}", s)
            print(f"{res}: {len(got)} rows", file=sys.stderr)
            rows += got
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        w.writerows({c: r.get(c) for c in COLUMNS} for r in rows)


if __name__ == "__main__":
    main()
