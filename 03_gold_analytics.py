# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./00_setup

# COMMAND ----------
from pyspark.sql import functions as F

orders = spark.table(SILVER_ORDERS).alias("o")
customers = spark.table(SILVER_CUSTOMERS).select("customer_id", "customer_name", F.col("segment").alias("customer_segment")).alias("c")
products = spark.table(SILVER_PRODUCTS).select("product_id", "product_name", "category").alias("p")

joined = (orders.join(customers, "customer_id", "left")
    .join(products, "product_id", "left")
    .withColumn("order_date", F.to_date("order_ts"))
    .withColumn("gross_sales", F.col("quantity") * F.col("unit_price"))
    .withColumn("net_sales", F.round(F.col("quantity") * F.col("unit_price") * (F.lit(1.0) - F.col("discount_pct")), 2)))

(joined.groupBy("order_date")
    .agg(F.countDistinct("order_id").alias("orders"), F.sum("quantity").alias("units_sold"), F.round(F.sum("net_sales"), 2).alias("net_sales"))
    .orderBy("order_date")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_DAILY))

(joined.groupBy("category")
    .agg(F.countDistinct("order_id").alias("orders"), F.sum("quantity").alias("units_sold"), F.round(F.sum("net_sales"), 2).alias("net_sales"))
    .orderBy(F.desc("net_sales"))
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_CATEGORY))

(joined.groupBy("customer_id", "customer_name", "customer_segment")
    .agg(F.countDistinct("order_id").alias("order_count"), F.round(F.sum("net_sales"), 2).alias("lifetime_net_sales"))
    .orderBy(F.desc("lifetime_net_sales"))
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_CUSTOMER))

display(spark.table(GOLD_CATEGORY))
display(spark.table(GOLD_DAILY).orderBy(F.desc("order_date")))
display(spark.table(GOLD_CUSTOMER).limit(20))
