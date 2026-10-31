"""Gold task: rebuild the dimensions from all of silver (seconds at this size, and always consistent), then write
the fact and mart for the months silver has but gold doesn't."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from epd_lakehouse import gold
from epd_lakehouse.io import args, months_in, overwrite_months, table

a = args("--months")
spark = SparkSession.builder.getOrCreate()
c = a.catalog
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{c}`.`gold`")
s = spark.table(table(c, "silver", "prescribing"))

practices, bnf, icbs, periods = gold.dim_practice(s), gold.dim_bnf(s), gold.dim_icb(s), gold.dim_period(s)
for df, name in [(practices, "dim_practice"), (bnf, "dim_bnf"), (icbs, "dim_icb"), (periods, "dim_period")]:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table(c, "gold", name))

fact_name = table(c, "gold", "fact_prescribing")
written = months_in(spark, fact_name)
new = ({int(m) for m in a.months.split(",")} if a.months
       else months_in(spark, table(c, "silver", "prescribing")) - written)
todo = gold.months_to_write(new, written)
if todo:
    practices = spark.table(table(c, "gold", "dim_practice"))
    fact = gold.fact_prescribing(s.filter(F.col("year_month").isin(sorted(todo))), practices)
    overwrite_months(fact, fact_name, todo)
    mart = gold.mart_icb_chapter_month(spark.table(fact_name).filter(F.col("year_month").isin(sorted(todo))),
                                       spark.table(table(c, "gold", "dim_icb")), spark.table(table(c, "gold", "dim_bnf")))
    overwrite_months(mart, table(c, "gold", "mart_icb_chapter_month"), todo)
print(f"gold: months written {sorted(todo)}")
