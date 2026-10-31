"""Bronze: the published files, as published, plus where and when each row arrived. Nothing is cleaned here, so any
silver or gold result can be traced back to the exact line in the exact file."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from epd_lakehouse.schema import CORRUPT_COLUMN, RAW_SCHEMA, RAW_SCHEMA_WITH_CORRUPT

# Files land as raw/epd/epd_YYYYMM.csv.gz, so the month a row should belong to is known from its file alone.
FILE_PERIOD_PATTERN = r"epd_(\d{6})\.csv"


def csv_options() -> dict[str, str]:
    """Reader options shared by the batch reader and Auto Loader. Quoted fields can contain commas and quotes.
    No `enforceSchema`, because Auto Loader rejects it alongside `rescuedDataColumn`."""
    return {"header": "true", "quote": '"', "escape": '"', "mode": "PERMISSIVE"}


def read_batch(spark: SparkSession, path: str) -> DataFrame:
    """Batch read of landed files with the declared schema (no inference pass). Lines that don't fit the 26
    published columns keep their raw text in `_corrupt_record` instead of failing the read."""
    df = (spark.read.options(**csv_options()).option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
          .schema(RAW_SCHEMA_WITH_CORRUPT).csv(path))
    return with_ingest_metadata(df, F.col("_metadata.file_path"))


def autoloader_stream(spark: SparkSession, path: str, schema_location: str) -> DataFrame:
    """Incremental read with Auto Loader (Databricks only). It tracks which files it has already loaded in the
    checkpoint, so each monthly file is ingested exactly once however often the job runs."""
    df = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.schemaLocation", schema_location)
          .option("cloudFiles.schemaEvolutionMode", "rescue")   # unexpected columns go to _rescued_data
          .option("rescuedDataColumn", "_rescued_data")
          .options(**csv_options())
          .schema(RAW_SCHEMA)
          .load(path))
    return with_ingest_metadata(df, F.col("_metadata.file_path")).withColumn(
        CORRUPT_COLUMN, F.col("_rescued_data").cast("string"))


def with_ingest_metadata(df: DataFrame, file_path: F.Column) -> DataFrame:
    """Lineage columns. `_file_period` is the month the file name promises, checked against YEAR_MONTH in silver."""
    return (df.withColumn("_source_file", file_path)
            .withColumn("_ingested_at", F.current_timestamp())
            # regexp_extract returns '' when the name doesn't match, and Spark 4's ANSI mode makes casting '' an
            # error, so an unexpected file name becomes NULL here instead of failing the load.
            .withColumn("_file_period", F.nullif(F.regexp_extract(file_path, FILE_PERIOD_PATTERN, 1), F.lit(""))
                        .cast("int")))
