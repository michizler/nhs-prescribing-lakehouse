"""Bronze task: Auto Loader picks up any monthly file that Data Factory has landed since the last run, appends it to
bronze, then stops. The checkpoint records which files are done, so every file is loaded exactly once."""
from pyspark.sql import SparkSession

from epd_lakehouse.bronze import autoloader_stream
from epd_lakehouse.io import args, table

a = args()
spark = SparkSession.builder.getOrCreate()
target = table(a.catalog, "bronze", "epd_raw")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{a.catalog}`.`bronze`")

(autoloader_stream(spark, a.raw_path, f"{a.checkpoint_path}/bronze_schema")
 .writeStream
 .option("checkpointLocation", f"{a.checkpoint_path}/bronze")
 .trigger(availableNow=True)          # process everything new, then stop: batch cost, streaming bookkeeping
 .toTable(target)
 .awaitTermination())
