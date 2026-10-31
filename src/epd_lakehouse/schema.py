"""The contract for the English Prescribing Dataset (EPD) as NHSBSA publishes it, and the facts about its shape that
the rest of the pipeline depends on. Each fact here was measured on the live data (June 2025, 18,028,356 rows)."""
from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

RAW_COLUMNS = [
    "YEAR_MONTH", "REGIONAL_OFFICE_NAME", "REGIONAL_OFFICE_CODE", "ICB_NAME", "ICB_CODE", "PCO_NAME", "PCO_CODE",
    "PRACTICE_NAME", "PRACTICE_CODE", "ADDRESS_1", "ADDRESS_2", "ADDRESS_3", "ADDRESS_4", "POSTCODE",
    "BNF_CHEMICAL_SUBSTANCE", "CHEMICAL_SUBSTANCE_BNF_DESCR", "BNF_CODE", "BNF_DESCRIPTION", "BNF_CHAPTER_PLUS_CODE",
    "QUANTITY", "ITEMS", "TOTAL_QUANTITY", "ADQUSAGE", "NIC", "ACTUAL_COST", "UNIDENTIFIED",
]

CORRUPT_COLUMN = "_corrupt_record"

# Bronze keeps every field as text, exactly as published. Typing happens in silver, where a value that won't parse
# sends its row to quarantine instead of failing a load of 18 million rows.
RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in RAW_COLUMNS])
RAW_SCHEMA_WITH_CORRUPT = StructType(RAW_SCHEMA.fields + [StructField(CORRUPT_COLUMN, StringType(), True)])

# One row per commissioner, practice, product and pack size (quantity per item) each month. Practice and product
# alone are not unique, because the same medicine appears once for every pack size it was dispensed in. The
# commissioner (PCO) is part of the grain because the placeholder prescriber below appears under many of them.
GRAIN = ["year_month", "pco_code", "practice_code", "bnf_code", "quantity"]

# Real practices are unique on the grain. The only repeats (26 in June 2025) come from the placeholder prescriber,
# whose lines have nothing to tell them apart, so they are summed to the grain in gold.
PLACEHOLDER_PRACTICE_CODE = "-"

# NHSBSA cuts every name field at 40 characters, e.g. "NHS LANCASHIRE AND SOUTH CUMBRIA INTEGRA".
NAME_LIMIT = 40
NAME_COLUMNS = ["regional_office_name", "icb_name", "pco_name", "practice_name"]
