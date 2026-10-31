"""Gold star schema: fact keys, the placeholder prescriber, point-in-time joins and totals that survive every step."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

from epd_lakehouse import gold, silver
from epd_lakehouse.gold import UNKNOWN_SK

_COLS = ("year_month int, period_start date, pco_code string, practice_code string, practice_name string, "
         "address_1 string, address_2 string, address_3 string, address_4 string, postcode string, pco_name string, "
         "icb_code string, icb_name string, regional_office_code string, regional_office_name string, "
         "bnf_code string, quantity decimal(20,3), unidentified boolean, "
         "items bigint, total_quantity decimal(20,3), adq_usage decimal(20,3), nic decimal(18,5), actual_cost decimal(18,5)")


def _silver(spark, rows):
    return spark.createDataFrame(rows, _COLS)


def _row(ym, pco, practice, name, qty, items, cost, unidentified=False):
    """A silver row with invented values. Only the columns a test looks at are filled."""
    return (ym, date(ym // 100, ym % 100, 1), pco, practice, name, None, None, None, None, None, None,
            "QOP", None, "Y62", None, "0411000G0AAAAAA", Decimal(qty), unidentified,
            items, Decimal(qty) * items, Decimal(0), Decimal(cost), Decimal(cost))


def test_placeholder_lines_sum_to_the_grain_and_keep_totals(spark):
    rows = [_row(202506, "AMW00", "-", "UNIDENTIFIED DOCTORS", "10", 1, "1.50", True),
            _row(202506, "AMW00", "-", "UNIDENTIFIED DOCTORS", "10", 2, "3.00", True),   # same grain, different line
            _row(202506, "00T00", "P82022", "HALLIWELL", "28", 4, "6.38")]
    s = _silver(spark, rows)
    fact = gold.fact_prescribing(s, gold.dim_practice(s))
    assert fact.count() == 2
    placeholder = fact.filter("practice_code = '-'").collect()[0]
    assert placeholder.practice_sk == UNKNOWN_SK and placeholder.items == 3 and placeholder.source_lines == 2
    totals = fact.agg(F.sum("items"), F.sum("actual_cost")).collect()[0]
    assert totals[0] == 7 and totals[1] == Decimal("10.88")


def test_fact_joins_the_practice_version_valid_that_month(spark):
    rows = [_row(202504, "00T00", "P1", "OLD NAME", "28", 1, "1"),
            _row(202505, "00T00", "P1", "NEW NAME", "28", 1, "1"),
            _row(202506, "00T00", "P1", "NEW NAME", "28", 1, "1")]
    s = _silver(spark, rows)
    dim = gold.dim_practice(s)
    names = dict(gold.fact_prescribing(s, dim).join(
        dim.select(F.col("sk").alias("practice_sk"), "practice_name"), "practice_sk").select("year_month", "practice_name")
        .collect())
    assert names == {202504: "OLD NAME", 202505: "NEW NAME", 202506: "NEW NAME"}


def test_loading_an_earlier_month_moves_keys_so_every_month_is_rewritten(spark):
    june, may = _row(202506, "00T00", "P1", "A", "28", 1, "1"), _row(202505, "00T00", "P1", "A", "28", 1, "1")
    first = _silver(spark, [june])
    old_fact = gold.fact_prescribing(first, gold.dim_practice(first))
    both = _silver(spark, [may, june])
    practices = gold.dim_practice(both)
    assert gold.orphaned_keys(old_fact, practices).collect()[0]["year_month"] == 202506
    assert gold.months_to_write({202505}, {202506}) == {202505, 202506}
    assert gold.orphaned_keys(gold.fact_prescribing(both, practices), practices).count() == 0


def test_newer_months_leave_written_months_alone():
    assert gold.months_to_write({202507}, {202505, 202506}) == {202507}
    assert gold.months_to_write({202505}, set()) == {202505}
    assert gold.months_to_write(set(), {202505, 202506}) == set()


def test_period_dimension_uses_the_nhs_financial_year(spark):
    s = _silver(spark, [_row(202503, "00T00", "P1", "A", "1", 1, "1"), _row(202504, "00T00", "P1", "A", "1", 1, "1")])
    labels = {r.year_month: r.financial_year_label for r in gold.dim_period(s).collect()}
    assert labels == {202503: "2024/25", 202504: "2025/26"}


def test_mart_totals_equal_fact_totals_on_real_rows(bronze):
    valid, _ = silver.build(bronze)
    practices, bnf, icbs = gold.dim_practice(valid), gold.dim_bnf(valid), gold.dim_icb(valid)
    fact = gold.fact_prescribing(valid, practices)
    mart = gold.mart_icb_chapter_month(fact, icbs, bnf)
    f = fact.agg(F.sum("items"), F.sum("actual_cost")).collect()[0]
    m = mart.agg(F.sum("items"), F.sum("actual_cost")).collect()[0]
    v = valid.agg(F.sum("items"), F.sum("actual_cost")).collect()[0]
    assert tuple(f) == tuple(m) == tuple(v)
    assert bnf.count() == bnf.select("bnf_code").distinct().count()
