# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./00_setup

# COMMAND ----------
# Demo-only MERGE: updates a handful of existing rows and inserts new synthetic IDs.
# MERGE is re-runnable for the same IDs. This sample changes data in the project's Silver table.
from delta.tables import DeltaTable
from pyspark.sql import functions as F

base = spark.table(SILVER_ORDERS)
updates = base.limit(5).withColumn("quantity", (F.col("quantity") + F.lit(1)).cast("int"))
new_rows = (base.limit(5)
    .withColumn("order_id", F.concat(F.lit("INC-"), F.col("order_id")))
    .withColumn("ingested_at", F.current_timestamp()))
source = updates.unionByName(new_rows)

# Persist source for inspectability. A production pipeline would build this from a change feed/source delta.
merge_source_table = f"{CATALOG}.{SCHEMA}.merge_source_demo"
(source.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(merge_source_table))
source = spark.table(merge_source_table)

target = DeltaTable.forName(spark, SILVER_ORDERS)
(target.alias("t").merge(source.alias("s"), "t.order_id = s.order_id")
 .whenMatchedUpdate(set={"quantity": "s.quantity", "ingested_at": "current_timestamp()"})
 .whenNotMatchedInsertAll()
 .execute())

print("MERGE complete. Existing demo order quantities updated and INC-* rows inserted.")
display(spark.sql(f"SELECT order_id, quantity FROM {SILVER_ORDERS} WHERE order_id LIKE 'INC-%' ORDER BY order_id"))
