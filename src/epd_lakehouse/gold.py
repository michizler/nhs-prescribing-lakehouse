"""Gold: a star schema for reporting. `fact_prescribing` at the published grain, a practice dimension that keeps its
history (type 2), a BNF product dimension with the full hierarchy, a period dimension, and one summary mart."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from epd_lakehouse import scd2
from epd_lakehouse.schema import GRAIN, PLACEHOLDER_PRACTICE_CODE

PRACTICE_ATTRS = ["practice_name", "address_1", "address_2", "address_3", "address_4", "postcode",
                  "pco_code", "pco_name", "icb_code", "icb_name", "regional_office_code", "regional_office_name"]
UNKNOWN_SK = -1
MEASURES = ["items", "total_quantity", "adq_usage", "nic", "actual_cost"]


def dim_practice(silver: DataFrame) -> DataFrame:
    """Type 2 practice dimension. A practice that moves sub-ICB, renames or changes address gets a new version, so
    a fact row always joins to the practice as it was in that month. The placeholder prescriber `-` becomes one
    fixed "unknown" member."""
    real = silver.filter(F.col("practice_code") != PLACEHOLDER_PRACTICE_CODE)
    dim = scd2.from_snapshots(real, "practice_code", PRACTICE_ATTRS)
    unknown = dim.sparkSession.createDataFrame(
        [(UNKNOWN_SK, PLACEHOLDER_PRACTICE_CODE, "UNKNOWN OR UNIDENTIFIED PRESCRIBER")],
        "sk bigint, practice_code string, practice_name string")
    return dim.unionByName(unknown, allowMissingColumns=True)


def dim_bnf(silver: DataFrame) -> DataFrame:
    """One row per BNF presentation with its hierarchy. Descriptions occasionally change between months, so the
    latest published wording wins."""
    latest = Window.partitionBy("bnf_code").orderBy(F.col("year_month").desc())
    cols = ["bnf_code", "bnf_description", "bnf_product_code", "bnf_chemical_code", "chemical_substance",
            "bnf_paragraph_code", "bnf_section_code", "bnf_chapter_code", "bnf_chapter_name"]
    return (silver.select("year_month", *cols)
            .withColumn("_r", F.row_number().over(latest)).filter("_r = 1").drop("_r", "year_month"))


def dim_period(silver: DataFrame) -> DataFrame:
    return (silver.select("year_month", "period_start").distinct()
            .withColumn("year", F.year("period_start"))
            .withColumn("month", F.month("period_start"))
            # The NHS financial year runs April to March, and budgets are set on it.
            .withColumn("financial_year", F.when(F.col("month") >= 4, F.col("year")).otherwise(F.col("year") - 1))
            .withColumn("financial_year_label", F.concat_ws("/", F.col("financial_year").cast("string"),
                                                            F.substring((F.col("financial_year") + 1).cast("string"), 3, 2))))


def fact_prescribing(silver: DataFrame, practices: DataFrame) -> DataFrame:
    """Fact rows at the published grain, keyed to the practice version valid in that month.

    Lines from the placeholder prescriber can repeat the grain with nothing to tell them apart, so every row is
    summed to the grain. For real practices that sum is over one row, and totals are unchanged either way."""
    by_grain = (silver.groupBy(*GRAIN, "period_start", "unidentified", "icb_code", "regional_office_code")
                .agg(*[F.sum(m).alias(m) for m in MEASURES], F.count(F.lit(1)).alias("source_lines")))
    versions = practices.filter(F.col("sk") != UNKNOWN_SK).select(
        F.col("sk").alias("practice_sk"), F.col("practice_code").alias("_pc"), "valid_from", "valid_to")
    joined = by_grain.join(
        versions,
        (by_grain.practice_code == versions._pc) & (by_grain.period_start >= versions.valid_from)
        & (versions.valid_to.isNull() | (by_grain.period_start < versions.valid_to)),
        "left")
    # The commissioner columns stay on the fact as recorded on the line, so NHS reporting by ICB includes the
    # placeholder prescriber's lines, which have no practice to look the ICB up from.
    return joined.select(
        "year_month", F.coalesce("practice_sk", F.lit(UNKNOWN_SK)).alias("practice_sk"), "practice_code",
        "pco_code", "icb_code", "regional_office_code", "bnf_code", "quantity", "unidentified", *MEASURES,
        "source_lines")


def dim_icb(silver: DataFrame) -> DataFrame:
    """ICB names by code, latest wording. ICB names are among the fields NHSBSA cuts at 40 characters."""
    latest = Window.partitionBy("icb_code").orderBy(F.col("year_month").desc())
    return (silver.select("year_month", "icb_code", "icb_name", "regional_office_code", "regional_office_name")
            .withColumn("_r", F.row_number().over(latest)).filter("_r = 1").drop("_r", "year_month"))


def mart_icb_chapter_month(fact: DataFrame, icbs: DataFrame, bnf: DataFrame) -> DataFrame:
    """Monthly items and cost by ICB and BNF chapter, the view most commissioning conversations start from."""
    b = bnf.select("bnf_code", "bnf_chapter_code", "bnf_chapter_name")
    return (fact.join(icbs.select("icb_code", "icb_name"), "icb_code", "left").join(b, "bnf_code", "left")
            .groupBy("year_month", "icb_code", "icb_name", "bnf_chapter_code", "bnf_chapter_name")
            .agg(F.sum("items").alias("items"), F.sum("actual_cost").alias("actual_cost"),
                 F.countDistinct("practice_code").alias("prescribers")))
