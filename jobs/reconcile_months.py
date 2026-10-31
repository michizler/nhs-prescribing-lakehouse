"""Reconciliation task: for every month in gold, compare bronze rows, and gold items and cost, with the totals the
publisher's own API computes. Results are appended to ops.reconciliation, and the task fails on any mismatch, so a
bad load can't pass silently."""
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from epd_lakehouse.io import args, table
from epd_lakehouse.reconcile import Totals, compare, published_totals

a = args()
spark = SparkSession.builder.getOrCreate()
c = a.catalog
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{c}`.`ops`")

rows = {r.year_month: r.n for r in spark.table(table(c, "bronze", "epd_raw"))
        .groupBy(F.col("_file_period").alias("year_month")).agg(F.count(F.lit(1)).alias("n")).collect()}
gold = {r.year_month: r for r in spark.table(table(c, "gold", "fact_prescribing")).groupBy("year_month")
        .agg(F.sum("items").alias("items"), F.sum("actual_cost").alias("cost")).collect()}

results = []
with requests.Session() as http:
    for m in sorted(gold):
        lake = Totals(m, rows.get(m, 0), int(gold[m].items), gold[m].cost)
        results += compare(published_totals(m, http), lake)

out = spark.createDataFrame([r.__dict__ for r in results]).withColumn("checked_at", F.current_timestamp())
out.write.format("delta").mode("append").saveAsTable(table(c, "ops", "reconciliation"))
out.show(truncate=False)
failed = [r for r in results if not r.passed]
if failed:
    raise SystemExit(f"reconciliation failed for {len(failed)} checks: {failed}")
