"""Reconciliation against the publisher, with the API replaced by a stub that returns its real response shape."""
from __future__ import annotations

from decimal import Decimal

import pytest

from epd_lakehouse import reconcile
from epd_lakehouse.reconcile import Totals


class _Response:
    def __init__(self, body):
        self._body = body

    def raise_for_status(self):
        return None

    def json(self):
        return self._body


class _Session:
    def __init__(self, body):
        self.body, self.calls = body, []

    def get(self, url, params, timeout):
        self.calls.append(params)
        return _Response(self.body)


def test_published_totals_reads_the_nested_result():
    # The shape the live API returned on 31 October 2026 for June 2025.
    body = {"success": True, "result": {"success": "true", "result": {"records": [
        {"n": 18028356, "items": 103590213, "cost": 862338253.30353}], "fields": []}}}
    s = _Session(body)
    t = reconcile.published_totals(202506, s)
    assert t == Totals(202506, 18028356, 103590213, Decimal("862338253.30353"))
    assert s.calls[0]["resource_id"] == "EPD_202506" and "`EPD_202506`" in s.calls[0]["sql"]


def test_api_errors_are_raised():
    with pytest.raises(RuntimeError):
        reconcile.published_totals(202506, _Session({"success": False, "error": {"message": "nope"}}))


def test_compare_exact_counts_and_penny_tolerance():
    pub = Totals(202506, 100, 250, Decimal("1000.00000"))
    ok = reconcile.compare(pub, Totals(202506, 100, 250, Decimal("1000.00400")))
    assert all(r.passed for r in ok)
    lost_row = reconcile.compare(pub, Totals(202506, 99, 250, Decimal("1000")))
    assert [r.check for r in lost_row if not r.passed] == ["rows"]
    off_cost = reconcile.compare(pub, Totals(202506, 100, 250, Decimal("1000.02")))
    assert [r.check for r in off_cost if not r.passed] == ["actual_cost"]
