# Databricks notebook source
# MAGIC %run ../00-helpers/01.enviroment-config

# COMMAND ----------

# MAGIC %run ../00-helpers/04.gold-helpers

# COMMAND ----------

silver_table_path = f"{catalog_name}.{silver_schema}.products"
gold_table_path = f"{catalog_name}.{gold_schema}.dim_products"

# COMMAND ----------

products_silver_table = spark.table(silver_table_path)

# COMMAND ----------

from functools import partial

upsert_dim(
    src=products_silver_table,
    gold_table=gold_table_path,
    business_key="product_id",
    attr_cols={
        "product_name":    "n.product_name",
        "category":    "n.category",
        "brand":   "n.brand",
        "price": "n.price",
        "discounted_price": "n.discounted_price"
    })