"""Data quality rules as data. Each rule is a SQL condition that must hold. An `error` rule sends the row to
quarantine. A `warn` rule keeps the row and records the rule name against it, so the issue is counted, not hidden."""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Rule:
    name: str
    condition: str          # true when the row is fine
    severity: str           # "error" or "warn"
    description: str


SILVER_RULES: list[Rule] = [
    # Errors: the row can't be trusted in a fact table.
    Rule("parsed", "_corrupt_record IS NULL", "error", "the CSV line parsed into the published columns"),
    Rule("period_present", "year_month IS NOT NULL AND period_start IS NOT NULL", "error", "YEAR_MONTH is a valid YYYYMM"),
    Rule("period_matches_file", "_file_period IS NULL OR year_month = _file_period", "error",
         "the row's month matches the monthly file it arrived in"),
    Rule("practice_present", "practice_code IS NOT NULL AND practice_code <> ''", "error", "a prescriber code is present"),
    Rule("bnf_code_valid", "length(bnf_code) = 15 OR (length(bnf_code) = 11 AND bnf_chapter_code BETWEEN '20' AND '23')",
         "error", "BNF codes are 15 characters, or 11 for the appliances in chapters 20 to 23"),
    Rule("items_valid", "items IS NOT NULL AND items >= 1", "error", "every line has at least one item"),
    Rule("measures_parse", "quantity IS NOT NULL AND total_quantity IS NOT NULL AND nic IS NOT NULL AND actual_cost IS NOT NULL",
         "error", "numeric measures parsed"),
    Rule("cost_non_negative", "actual_cost >= 0 AND nic >= 0", "error", "costs are never negative"),
    # Warnings: real features of the published data that analysts need to know about.
    Rule("names_complete", "NOT names_truncated", "warn", "no name field was cut at the 40-character limit"),
    Rule("prescriber_identified", "NOT unidentified AND practice_code <> '-'", "warn", "the prescriber is identified"),
    Rule("quantity_positive", "quantity > 0", "warn", "quantity per item is above zero"),
]


def _failed(rules: list[Rule]) -> Column:
    """Array of rule names whose condition is not true for this row. A NULL condition counts as a failure."""
    checks = [F.when(~F.coalesce(F.expr(r.condition), F.lit(False)), F.lit(r.name)) for r in rules]
    return F.filter(F.array(*checks), lambda x: x.isNotNull()) if checks else F.array().cast("array<string>")


def apply_rules(df: DataFrame, rules: list[Rule] = SILVER_RULES) -> DataFrame:
    """Adds `_dq_errors` and `_dq_warnings`, the names of the rules each row breaks."""
    errors = [r for r in rules if r.severity == "error"]
    warns = [r for r in rules if r.severity == "warn"]
    return df.withColumn("_dq_errors", _failed(errors)).withColumn("_dq_warnings", _failed(warns))


def split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(valid rows, quarantined rows). Run after `apply_rules`."""
    bad = F.size("_dq_errors") > 0
    return df.filter(~bad), df.filter(bad)


def summarise(df: DataFrame) -> DataFrame:
    """Row counts per broken rule and severity, for the run log."""
    errs = df.select(F.explode("_dq_errors").alias("rule")).withColumn("severity", F.lit("error"))
    warns = df.select(F.explode("_dq_warnings").alias("rule")).withColumn("severity", F.lit("warn"))
    return errs.unionByName(warns).groupBy("severity", "rule").count()
