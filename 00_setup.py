# Databricks notebook source
# COMMAND ----------
# Shared project configuration. Run this first, then run the other notebooks in order.

# Widgets are intentionally used so the same code can run in another writable catalog.
try:
    dbutils.widgets.text("catalog", "main", "Unity Catalog catalog (edit if needed)")
    dbutils.widgets.text("schema", "retail_lakehouse", "Project schema")
    dbutils.widgets.text("volume", "retail_files", "Project volume")
except Exception:
    pass

import re
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
for label, value in [("catalog", CATALOG), ("schema", SCHEMA), ("volume", VOLUME)]:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid {label}: {value!r}. Use letters, digits and underscores; must start with a letter or underscore.")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`")
BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
LANDING_PATH = f"{BASE_PATH}/landing"
for folder in [LANDING_PATH, f"{BASE_PATH}/checkpoints"]:
    dbutils.fs.mkdirs(folder)

BRONZE_ORDERS = f"{CATALOG}.{SCHEMA}.bronze_orders"
BRONZE_CUSTOMERS = f"{CATALOG}.{SCHEMA}.bronze_customers"
BRONZE_PRODUCTS = f"{CATALOG}.{SCHEMA}.bronze_products"
SILVER_ORDERS = f"{CATALOG}.{SCHEMA}.silver_orders"
SILVER_CUSTOMERS = f"{CATALOG}.{SCHEMA}.silver_customers"
SILVER_PRODUCTS = f"{CATALOG}.{SCHEMA}.silver_products"
QUARANTINE_ORDERS = f"{CATALOG}.{SCHEMA}.quarantine_orders"
GOLD_DAILY = f"{CATALOG}.{SCHEMA}.gold_sales_daily"
GOLD_CATEGORY = f"{CATALOG}.{SCHEMA}.gold_sales_by_category"
GOLD_CUSTOMER = f"{CATALOG}.{SCHEMA}.gold_customer_value"

print(f"Ready: {CATALOG}.{SCHEMA}; project files at {BASE_PATH}")
