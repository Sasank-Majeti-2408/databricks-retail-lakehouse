# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./00_setup

# COMMAND ----------
# Create repeatable synthetic CSV source files, then ingest them into Bronze Delta tables.
import random
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F

rng = random.Random(2408)

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("segment", StringType(), True),
])
product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", StringType(), True),
])
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_ts", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("discount_pct", StringType(), True),
    StructField("status", StringType(), True),
    StructField("city", StringType(), True),
])

customers = [
    (f"C{i:04d}", f"Customer {i:04d}", rng.choice(["Bengaluru", "Hyderabad", "Chennai", "Pune", "Mysuru"]), rng.choice(["Consumer", "Small Business", "Enterprise"]))
    for i in range(1, 81)
]
customers.append(("C0001", "  Customer 0001  ", "Bengaluru", "Consumer"))  # intentional duplicate to clean
products = [
    (f"P{i:03d}", f"Product {i:03d}", rng.choice(["Electronics", "Home", "Sports", "Books", "Apparel"]), str(round(rng.uniform(99, 12000), 2)))
    for i in range(1, 26)
]

base_time = datetime(2026, 9, 1, 9, 0, 0)
orders = []
for i in range(1, 601):
    dt = base_time + timedelta(minutes=rng.randint(0, 60 * 24 * 30))
    product = rng.choice(products)
    orders.append((
        f"O{i:06d}", rng.choice(customers[:80])[0], product[0], dt.strftime("%Y-%m-%d %H:%M:%S"),
        str(rng.choice([1, 1, 1, 2, 3, 4, 5])), product[3], str(round(rng.choice([0, 0, 0.05, 0.10, 0.15, 0.20]), 2)),
        rng.choice(["COMPLETE", "COMPLETE", "COMPLETE", "RETURNED", "CANCELLED"]), rng.choice(["Bengaluru", "Hyderabad", "Chennai", "Pune", "Mysuru"])
    ))
# Intentional quality issues for data-quality teaching: duplicate IDs, missing keys, invalid numbers, bad timestamp.
orders += [orders[0], orders[10], ("O900001", None, "P001", "2026-09-10 10:00:00", "2", "999.00", "0.1", "COMPLETE", "Bengaluru"),
           ("O900002", "C0002", "P002", "not-a-date", "2", "100.00", "0.0", "COMPLETE", "Mysuru"),
           ("O900003", "C0003", "P003", "2026-09-12 12:00:00", "-2", "100.00", "0.0", "COMPLETE", "Pune"),
           ("O900004", "C0004", "P004", "2026-09-13 12:00:00", "1", "-10.00", "0.0", "COMPLETE", "Pune")]

# Write source CSV folders in the governed Unity Catalog volume.
for name, rows, schema in [
    ("customers", customers, customer_schema),
    ("products", products, product_schema),
    ("orders", orders, order_schema),
]:
    path = f"{LANDING_PATH}/{name}"
    dbutils.fs.rm(path, True)
    (spark.createDataFrame(rows, schema)
         .coalesce(1)
         .write.mode("overwrite").option("header", True).csv(path))
    print(f"Wrote synthetic CSV source: {path}")

# Read the CSVs with explicit schemas as the ingestion step.
raw_customers = spark.read.schema(customer_schema).option("header", True).csv(f"{LANDING_PATH}/customers")
raw_products = spark.read.schema(product_schema).option("header", True).csv(f"{LANDING_PATH}/products")
raw_orders = spark.read.schema(order_schema).option("header", True).csv(f"{LANDING_PATH}/orders")

(raw_customers.withColumn("ingested_at", F.current_timestamp()).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE_CUSTOMERS))
(raw_products.withColumn("ingested_at", F.current_timestamp()).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE_PRODUCTS))
(raw_orders.withColumn("ingested_at", F.current_timestamp()).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE_ORDERS))

display(spark.sql(f"SELECT 'orders' AS entity, COUNT(*) AS row_count FROM {BRONZE_ORDERS} UNION ALL SELECT 'customers', COUNT(*) FROM {BRONZE_CUSTOMERS} UNION ALL SELECT 'products', COUNT(*) FROM {BRONZE_PRODUCTS}"))
