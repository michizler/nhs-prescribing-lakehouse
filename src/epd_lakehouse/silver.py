"""Silver: one typed, conformed row per published line, validated by the rules in `dq.py`. Rows that break an error
rule go to quarantine with the reasons attached. Everything else continues, with any warnings recorded on the row."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from epd_lakehouse import dq
from epd_lakehouse.schema import CORRUPT_COLUMN, GRAIN, NAME_COLUMNS, NAME_LIMIT

# Money keeps five decimal places, as published. DECIMAL, never float, so sums reconcile to the penny.
MONEY = "decimal(18,5)"
AMOUNT = "decimal(20,3)"

_TEXT = {
    "REGIONAL_OFFICE_NAME": "regional_office_name", "REGIONAL_OFFICE_CODE": "regional_office_code",
    "ICB_NAME": "icb_name", "ICB_CODE": "icb_code", "PCO_NAME": "pco_name", "PCO_CODE": "pco_code",
    "PRACTICE_NAME": "practice_name", "PRACTICE_CODE": "practice_code",
    "ADDRESS_1": "address_1", "ADDRESS_2": "address_2", "ADDRESS_3": "address_3", "ADDRESS_4": "address_4",
    "POSTCODE": "postcode", "BNF_CHEMICAL_SUBSTANCE": "bnf_chemical_code",
    "CHEMICAL_SUBSTANCE_BNF_DESCR": "chemical_substance", "BNF_CODE": "bnf_code", "BNF_DESCRIPTION": "bnf_description",
}
_NUMBERS = {"QUANTITY": ("quantity", AMOUNT), "ITEMS": ("items", "bigint"), "TOTAL_QUANTITY": ("total_quantity", AMOUNT),
            "ADQUSAGE": ("adq_usage", AMOUNT), "NIC": ("nic", MONEY), "ACTUAL_COST": ("actual_cost", MONEY)}


def _clean(c: str) -> F.Column:
    """Trimmed text, with empty strings as NULL so 'missing' has one representation."""
    t = F.trim(F.col(c))
    return F.when(t == "", None).otherwise(t)


def conform(bronze: DataFrame) -> DataFrame:
    """Types, names and derived columns. Uses `try_cast`, so a bad value becomes NULL and is caught by a rule,
    rather than one bad line failing the job."""
    cols = [_clean(src).alias(dst) for src, dst in _TEXT.items()]
    cols += [F.expr(f"try_cast(trim({src}) AS {typ})").alias(dst) for src, (dst, typ) in _NUMBERS.items()]
    year_month = F.expr("try_cast(trim(YEAR_MONTH) AS int)")
    chapter = _clean("BNF_CHAPTER_PLUS_CODE")
    df = bronze.select(
        year_month.alias("year_month"),
        *cols,
        F.regexp_extract(chapter, r"^(\d{2}):", 1).alias("bnf_chapter_code"),
        F.regexp_extract(chapter, r"^\d{2}:\s*(.*)$", 1).alias("bnf_chapter_name"),
        (F.upper(_clean("UNIDENTIFIED")) == "Y").alias("unidentified"),
        F.col(CORRUPT_COLUMN), F.col("_source_file"), F.col("_ingested_at"), F.col("_file_period"),
    )
    # try_to_timestamp, because Spark 4 runs in ANSI mode and to_date would fail the job on a month like 202513.
    period = F.to_date(F.try_to_timestamp(F.concat(F.col("year_month").cast("string"), F.lit("01")), F.lit("yyyyMMdd")))
    truncated = F.greatest(*[F.coalesce(F.length(c), F.lit(0)) for c in NAME_COLUMNS]) >= NAME_LIMIT
    return (df.withColumn("period_start", period)
            # The BNF code nests its own hierarchy: chapter (2), section (4), paragraph (6), chemical (9),
            # product (11) and presentation (15 characters).
            .withColumn("bnf_section_code", F.substring("bnf_code", 1, 4))
            .withColumn("bnf_paragraph_code", F.substring("bnf_code", 1, 6))
            .withColumn("bnf_product_code", F.substring("bnf_code", 1, 11))
            .withColumn("names_truncated", truncated)
            .withColumn("_grain_key", F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in GRAIN]), 256)))


def build(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(silver rows, quarantined rows) for a batch of bronze rows."""
    return dq.split(dq.apply_rules(conform(bronze)))
