"""Type 2 history from monthly snapshots: new versions on change, reverts as new versions, stable keys."""
from __future__ import annotations

from datetime import date

from epd_lakehouse import scd2


def _snaps(spark, rows):
    return spark.createDataFrame(rows, "practice_code string, period_start date, pco_code string, practice_name string")


def test_versions_follow_attribute_changes(spark):
    rows = [
        ("P1", date(2025, 1, 1), "00T00", "HALLIWELL SURGERY"),
        ("P1", date(2025, 2, 1), "00T00", "HALLIWELL SURGERY"),
        ("P1", date(2025, 3, 1), "00V00", "HALLIWELL SURGERY"),     # moved commissioner
        ("P1", date(2025, 4, 1), "00T00", "HALLIWELL SURGERY"),     # moved back: a new version, not the old one
        ("P2", date(2025, 1, 1), "00T00", "KILDONAN HOUSE"),
    ]
    dim = scd2.from_snapshots(_snaps(spark, rows), "practice_code", ["pco_code", "practice_name"])
    p1 = sorted(dim.filter("practice_code = 'P1'").collect(), key=lambda r: r.valid_from)
    assert [(r.pco_code, r.valid_from, r.valid_to) for r in p1] == [
        ("00T00", date(2025, 1, 1), date(2025, 3, 1)),
        ("00V00", date(2025, 3, 1), date(2025, 4, 1)),
        ("00T00", date(2025, 4, 1), None),
    ]
    assert [r.is_current for r in p1] == [False, False, True]
    assert p1[0].last_seen == date(2025, 2, 1) and all(r.first_seen == date(2025, 1, 1) for r in p1)


def test_keys_are_stable_across_rebuilds(spark):
    rows = [("P1", date(2025, 1, 1), "00T00", "A"), ("P1", date(2025, 2, 1), "00V00", "A")]
    first = {r.sk for r in scd2.from_snapshots(_snaps(spark, rows), "practice_code", ["pco_code"]).collect()}
    more = rows + [("P1", date(2025, 3, 1), "00V00", "A")]
    second = {r.sk for r in scd2.from_snapshots(_snaps(spark, more), "practice_code", ["pco_code"]).collect()}
    assert first == second    # a new month that changes nothing adds no version and moves no key


def test_null_attributes_compare_as_values(spark):
    rows = [("P1", date(2025, 1, 1), "00T00", None), ("P1", date(2025, 2, 1), "00T00", None),
            ("P1", date(2025, 3, 1), "00T00", "NAMED")]
    dim = scd2.from_snapshots(_snaps(spark, rows), "practice_code", ["pco_code", "practice_name"])
    assert dim.count() == 2
