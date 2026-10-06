# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./00_setup

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

orders = (spark.table(BRONZE_ORDERS)
    .withColumn("order_ts", F.to_timestamp("order_ts"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("discount_pct", F.col("discount_pct").cast("double"))
    .withColumn("order_id", F.trim("order_id"))
    .withColumn("customer_id", F.trim("customer_id"))
    .withColumn("product_id", F.trim("product_id"))
    .withColumn("status", F.upper(F.trim("status")))
    .withColumn("city", F.initcap(F.trim("city")))
)

orders = orders.withColumn(
    "validation_error",
    F.concat_ws("; ",
        F.when(F.col("order_id").isNull() | (F.length("order_id") == 0), F.lit("missing_order_id")),
        F.when(F.col("customer_id").isNull() | (F.length("customer_id") == 0), F.lit("missing_customer_id")),
        F.when(F.col("product_id").isNull() | (F.length("product_id") == 0), F.lit("missing_product_id")),
        F.when(F.col("order_ts").isNull(), F.lit("invalid_order_ts")),
        F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("invalid_quantity")),
        F.when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), F.lit("invalid_unit_price")),
        F.when(F.col("discount_pct").isNull() | (F.col("discount_pct") < 0) | (F.col("discount_pct") > 1), F.lit("invalid_discount_pct")),
    )
)

invalid = orders.filter(F.length("validation_error") > 0)
(invalid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(QUARANTINE_ORDERS))

valid = orders.filter(F.length("validation_error") == 0).drop("validation_error")
# Keep the newest ingested row for each order_id. This removes intentional duplicates in the demo source.
w = Window.partitionBy("order_id").orderBy(F.col("ingested_at").desc())
valid = valid.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
(valid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_ORDERS))

customers = (spark.table(BRONZE_CUSTOMERS)
    .withColumn("customer_id", F.trim("customer_id"))
    .withColumn("customer_name", F.trim("customer_name"))
    .withColumn("city", F.initcap(F.trim("city")))
    .withColumn("segment", F.initcap(F.trim("segment")))
    .filter(F.col("customer_id").isNotNull() & (F.length("customer_id") > 0))
    .withColumn("_rn", F.row_number().over(Window.partitionBy("customer_id").orderBy(F.col("ingested_at").desc())))
    .filter(F.col("_rn") == 1).drop("_rn"))
(customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_CUSTOMERS))

products = (spark.table(BRONZE_PRODUCTS)
    .withColumn("product_id", F.trim("product_id"))
    .withColumn("product_name", F.trim("product_name"))
    .withColumn("category", F.initcap(F.trim("category")))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .filter(F.col("product_id").isNotNull() & F.col("unit_price").isNotNull() & (F.col("unit_price") >= 0))
    .withColumn("_rn", F.row_number().over(Window.partitionBy("product_id").orderBy(F.col("ingested_at").desc())))
    .filter(F.col("_rn") == 1).drop("_rn"))
(products.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_PRODUCTS))

display(spark.sql(f"SELECT 'silver_orders' AS table_name, COUNT(*) AS rows FROM {SILVER_ORDERS} UNION ALL SELECT 'quarantine_orders', COUNT(*) FROM {QUARANTINE_ORDERS} UNION ALL SELECT 'silver_customers', COUNT(*) FROM {SILVER_CUSTOMERS} UNION ALL SELECT 'silver_products', COUNT(*) FROM {SILVER_PRODUCTS}"))
