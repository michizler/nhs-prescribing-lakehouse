"""Per-month idempotent writes, the property every rerun and backfill depends on."""
from __future__ import annotations

from epd_lakehouse.io import months_in, overwrite_months


def _months(spark, rows):
    return spark.createDataFrame(rows, "year_month int, items bigint")


def test_rewriting_a_month_replaces_only_that_month(spark):
    name = "default.io_test"
    overwrite_months(_months(spark, [(202505, 1), (202506, 2)]), name, {202505, 202506})
    overwrite_months(_months(spark, [(202506, 20), (202506, 30)]), name, {202506})
    got = sorted((r.year_month, r.items) for r in spark.table(name).collect())
    assert got == [(202505, 1), (202506, 20), (202506, 30)]
    assert months_in(spark, name) == {202505, 202506}


def test_running_the_same_month_twice_changes_nothing(spark):
    name = "default.io_twice"
    batch = _months(spark, [(202506, 5)])
    overwrite_months(batch, name, {202506})
    overwrite_months(batch, name, {202506})
    assert [(r.year_month, r.items) for r in spark.table(name).collect()] == [(202506, 5)]


def test_a_batch_cannot_write_outside_its_months(spark):
    name = "default.io_guard"
    overwrite_months(_months(spark, [(202505, 1)]), name, {202505})
    try:
        overwrite_months(_months(spark, [(202504, 9)]), name, {202505})
    except Exception as e:      # Delta refuses rows that don't match replaceWhere
        assert "replaceWhere" in str(e) or "DELTA_REPLACE_WHERE_MISMATCH" in str(e)
    else:
        raise AssertionError("a row outside the declared months was written")
    assert months_in(spark, name) == {202505}
