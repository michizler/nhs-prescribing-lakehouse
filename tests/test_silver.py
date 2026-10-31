"""Silver conformance and the data quality rules, on real published rows and on deliberately broken ones."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

from epd_lakehouse import silver
from epd_lakehouse.bronze import with_ingest_metadata
from epd_lakehouse.schema import CORRUPT_COLUMN, GRAIN, RAW_COLUMNS


def test_every_published_column_is_read(bronze):
    assert [c for c in bronze.columns if c in RAW_COLUMNS] == RAW_COLUMNS
    assert bronze.count() == 36


def test_conform_types_and_hierarchy(bronze):
    rows = silver.conform(bronze).filter(F.col("bnf_code") == "0411000G0AAAAAA").collect()
    r = rows[0]
    assert isinstance(r.year_month, int) and r.period_start.day == 1
    assert isinstance(r.actual_cost, Decimal) and isinstance(r.items, int)
    assert (r.bnf_chapter_code, r.bnf_section_code, r.bnf_paragraph_code) == ("04", "0411", "041100")
    assert r.bnf_chemical_code == "0411000G0" and r.bnf_product_code == "0411000G0AA"
    assert r.bnf_chapter_name == "Central Nervous System"


def test_real_rows_pass_every_error_rule(bronze):
    valid, quarantine = silver.build(bronze)
    assert quarantine.count() == 0
    assert valid.count() == 36


def test_grain_is_unique_for_real_practices(bronze):
    df = silver.conform(bronze).filter(F.col("practice_code") != "-")
    assert df.count() == df.dropDuplicates(GRAIN).count()


def test_placeholder_rows_are_kept_with_warnings(bronze):
    valid, _ = silver.build(bronze)
    placeholder = valid.filter(F.col("practice_code").isin("-", "AMW998")).collect()
    assert placeholder, "fixture should contain the unidentified-prescriber rows"
    for r in placeholder:
        assert "prescriber_identified" in r._dq_warnings
        assert "names_complete" in r._dq_warnings      # "NHS SURREY HEARTLANDS INTEGRATED CARE BO" is 40 characters


def _rows(spark, cases):
    """Bronze-shaped rows, one per case, each a change to the same good row. Values are invented for the test."""
    good = dict.fromkeys(RAW_COLUMNS, None) | {
        "YEAR_MONTH": "202506", "PCO_CODE": "00T00", "PRACTICE_CODE": "P82022", "BNF_CODE": "0411000G0AAAAAA",
        "BNF_CHAPTER_PLUS_CODE": "04: Central Nervous System", "QUANTITY": "28", "ITEMS": "4",
        "TOTAL_QUANTITY": "112", "ADQUSAGE": "0", "NIC": "6.1", "ACTUAL_COST": "6.38464", "UNIDENTIFIED": "N"}
    rows = [(name, *[(good | change)[c] for c in RAW_COLUMNS]) for name, change in cases.items()]
    schema = ", ".join(f"{c} string" for c in ["case"] + RAW_COLUMNS)
    df = spark.createDataFrame(rows, schema).withColumn(CORRUPT_COLUMN, F.lit(None).cast("string"))
    return with_ingest_metadata(df, F.lit("raw/epd/epd_202506.csv.gz"))


def test_each_broken_row_is_quarantined_for_its_own_reason(spark):
    bronze = _rows(spark, {
        "items_valid": {"ITEMS": "four"},
        "bnf_code_valid": {"BNF_CODE": "0411000G0"},
        "period_matches_file": {"YEAR_MONTH": "202505"},
        "cost_non_negative": {"ACTUAL_COST": "-1.00"},
        "practice_present": {"PRACTICE_CODE": "  "},
    })
    cases = [r.case for r in bronze.select("case").collect()]
    for case in cases:
        valid, quarantine = silver.build(bronze.filter(F.col("case") == case))
        errs = quarantine.collect()
        assert valid.count() == 0 and len(errs) == 1, case
        assert case in errs[0]._dq_errors, f"{case} not flagged, got {errs[0]._dq_errors}"


def test_eleven_character_codes_are_valid_only_for_appliances(spark):
    # 10% of national rows have these codes, real ones taken from June 2025. Drugs always have 15 characters.
    valid, quarantine = silver.build(_rows(spark, {
        "appliance": {"BNF_CODE": "21300000956", "BNF_CHAPTER_PLUS_CODE": "21: Appliances"},
        "dressing": {"BNF_CODE": "20020200324", "BNF_CHAPTER_PLUS_CODE": "20: Dressings"},
        "incontinence": {"BNF_CODE": "22302083001", "BNF_CHAPTER_PLUS_CODE": "22: Incontinence Appliances"},
        "stoma": {"BNF_CODE": "23150901506", "BNF_CHAPTER_PLUS_CODE": "23: Stoma Appliances"},
        "drug_too_short": {"BNF_CODE": "0411000G0AA"},
    }))
    assert {r.bnf_code for r in valid.collect()} == {"21300000956", "20020200324", "22302083001", "23150901506"}
    assert [r.bnf_code for r in quarantine.collect()] == ["0411000G0AA"]


def test_period_start_is_first_of_month(bronze):
    months = {r.period_start for r in silver.conform(bronze).select("period_start").distinct().collect()}
    assert months == {date(2025, 5, 1), date(2025, 6, 1)}
