"""Type 2 slowly changing dimensions built from monthly snapshots.

Silver keeps every month, so the full history of every practice is already there. Rather than MERGE each new month
into the dimension, the dimension is rebuilt from that history on every run. Each rebuild produces the same rows
and the same keys, so a rerun or a corrected past month can never leave the dimension in a half-updated state.
At about 9,000 practices a month the rebuild takes seconds."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def from_snapshots(snapshots: DataFrame, key: str, attrs: list[str], period: str = "period_start") -> DataFrame:
    """One row per version of each `key`, where a version is a run of consecutive snapshots with identical `attrs`.

    Returns `key`, the attrs, `valid_from`, `valid_to` (exclusive, NULL for the current version), `first_seen`,
    `last_seen`, `is_current` and a surrogate key `sk` that depends only on (key, valid_from), so it is the same on
    every rebuild.

    This is the gaps-and-islands pattern. A version starts wherever the attribute hash differs from the previous
    snapshot's, and a running sum of those starts numbers the versions.
    """
    attr_hash = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(a).cast("string"), F.lit("∅")) for a in attrs]), 256)
    by_time = Window.partitionBy(key).orderBy(period)
    snaps = (snapshots.select(key, period, *attrs).dropDuplicates([key, period])
             .withColumn("_h", attr_hash)
             .withColumn("_new", F.when(F.lag("_h").over(by_time).isNull()
                                        | (F.lag("_h").over(by_time) != F.col("_h")), 1).otherwise(0))
             .withColumn("_version", F.sum("_new").over(by_time.rowsBetween(Window.unboundedPreceding, 0))))
    versions = snaps.groupBy(key, "_version").agg(
        F.min(period).alias("valid_from"), F.max(period).alias("last_seen"),
        *[F.first(a, ignorenulls=False).alias(a) for a in attrs])
    by_version = Window.partitionBy(key).orderBy("valid_from")
    first_seen = Window.partitionBy(key)
    return (versions
            .withColumn("valid_to", F.lead("valid_from").over(by_version))
            .withColumn("first_seen", F.min("valid_from").over(first_seen))
            .withColumn("is_current", F.col("valid_to").isNull())
            .withColumn("sk", F.xxhash64(F.col(key), F.col("valid_from")))
            .select("sk", key, *attrs, "valid_from", "valid_to", "first_seen", "last_seen", "is_current"))
