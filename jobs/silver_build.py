"""Silver task: conform and validate every month that is in bronze but not yet in silver (or the months passed with
--months to reprocess). Valid rows and quarantined rows are written per month, replacing any earlier attempt."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from epd_lakehouse import dq, silver
from epd_lakehouse.io import args, months_in, overwrite_months, table

a = args("--months")
spark = SparkSession.builder.getOrCreate()
c = a.catalog
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{c}`.`silver`")
src, dst, bad = table(c, "bronze", "epd_raw"), table(c, "silver", "prescribing"), table(c, "silver", "quarantine")

bronze = spark.table(src)
landed = {r[0] for r in bronze.select("_file_period").distinct().collect() if r[0] is not None}
todo = {int(m) for m in a.months.split(",")} if a.months else landed - months_in(spark, dst)
if not todo:
    print("silver: nothing new")
else:
    batch = bronze.filter(F.col("_file_period").isin(sorted(todo)))
    checked = dq.apply_rules(silver.conform(batch)).cache()
    valid, quarantine = dq.split(checked)
    overwrite_months(valid, dst, todo)
    overwrite_months(quarantine.withColumn("year_month", F.coalesce("year_month", "_file_period")), bad, todo)
    print(f"silver: wrote months {sorted(todo)}")
    dq.summarise(checked).show(truncate=False)
    checked.unpersist()
