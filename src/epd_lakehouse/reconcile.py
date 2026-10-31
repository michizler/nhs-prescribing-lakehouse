"""Reconciliation against the publisher. NHSBSA's API can count rows and sum items and cost for any month, which
gives an independent total for every month the lakehouse loads. A load only counts as done when the lakehouse
matches it."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

API = "https://opendata.nhsbsa.net/api/3/action/datastore_search_sql"


@dataclass(frozen=True)
class Totals:
    year_month: int
    rows: int
    items: int
    actual_cost: Decimal


@dataclass(frozen=True)
class Result:
    year_month: int
    check: str
    published: str
    lakehouse: str
    passed: bool


class _Session(Protocol):
    def get(self, url: str, params: dict, timeout: int): ...


def published_totals(year_month: int, session: _Session) -> Totals:
    """Row count, items and cost for one month, computed by the publisher's own database."""
    res = f"EPD_{year_month}"
    sql = f"SELECT COUNT(*) AS n, SUM(ITEMS) AS items, SUM(ACTUAL_COST) AS cost FROM `{res}`"
    r = session.get(API, params={"resource_id": res, "sql": sql}, timeout=600)
    r.raise_for_status()
    body = r.json()
    if not body.get("success"):
        raise RuntimeError(f"NHSBSA API error for {res}: {body.get('error')}")
    rec = body["result"]["result"]["records"][0]   # nested one level deeper than standard CKAN
    return Totals(year_month, int(rec["n"]), int(rec["items"]), Decimal(str(rec["cost"])))


def compare(published: Totals, lakehouse: Totals, cost_tolerance: Decimal = Decimal("0.01")) -> list[Result]:
    """Rows and items must match exactly. Cost may differ by rounding only, by default under a penny for the month.

    `lakehouse.rows` counts bronze lines (every published line, quarantined or not), so a row lost anywhere between
    landing and bronze shows up here, and quarantine is reported separately rather than mistaken for a loss."""
    m = published.year_month
    cost_gap = abs(published.actual_cost - lakehouse.actual_cost)
    return [
        Result(m, "rows", str(published.rows), str(lakehouse.rows), published.rows == lakehouse.rows),
        Result(m, "items", str(published.items), str(lakehouse.items), published.items == lakehouse.items),
        Result(m, "actual_cost", f"{published.actual_cost:.5f}", f"{lakehouse.actual_cost:.5f}", cost_gap < cost_tolerance),
    ]
