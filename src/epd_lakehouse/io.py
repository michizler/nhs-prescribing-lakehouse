"""Job plumbing shared by the Databricks tasks: arguments, table names and per-month idempotent Delta writes."""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession


def args(*extra: str) -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--raw-path", required=True, help="abfss://raw@<account>.dfs.core.windows.net/epd")
    ap.add_argument("--checkpoint-path", required=True, help="abfss://lake@<account>.dfs.core.windows.net/_checkpoints")
    for e in extra:
        ap.add_argument(e)
    return ap.parse_args()


def table(catalog: str, schema: str, name: str) -> str:
    return f"`{catalog}`.`{schema}`.`{name}`"


def months_in(spark: SparkSession, name: str) -> set[int]:
    if not spark.catalog.tableExists(name):
        return set()
    return {r[0] for r in spark.table(name).select("year_month").distinct().collect()}


def overwrite_months(df: DataFrame, name: str, months: set[int]) -> None:
    """Replaces exactly the given months and nothing else. Rerunning a month, or reloading a corrected one,
    gives the same table as running it once. The first write creates the table, clustered by month."""
    spark = df.sparkSession
    if not months:
        return
    if not spark.catalog.tableExists(name):
        # Liquid clustering on month, created through SQL because open-source Delta only accepts CLUSTER BY there.
        # Queries filtered to a month then skip every file that holds other months.
        view = f"_new_{abs(hash(name))}"
        df.createOrReplaceTempView(view)
        spark.sql(f"CREATE TABLE {name} USING DELTA CLUSTER BY (year_month) AS SELECT * FROM {view}")
        spark.catalog.dropTempView(view)
        return
    in_list = ", ".join(str(m) for m in sorted(months))
    (df.write.format("delta").mode("overwrite").option("replaceWhere", f"year_month IN ({in_list})")
     .saveAsTable(name))
