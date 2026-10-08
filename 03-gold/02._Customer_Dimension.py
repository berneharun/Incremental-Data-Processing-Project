# Databricks notebook source
# MAGIC %run ../00-helpers/01.enviroment-config

# COMMAND ----------

# MAGIC %run ../00-helpers/04.gold-helpers

# COMMAND ----------

silver_table_path = f"{catalog_name}.{silver_schema}.customers"
gold_table_path = f"{catalog_name}.{gold_schema}.dim_customers"

# COMMAND ----------

customers_silver_table = spark.table(silver_table_path)

# COMMAND ----------

from functools import partial

upsert_dim(
    src=customers_silver_table,
    gold_table=gold_table_path,
    business_key="customer_id",
    attr_cols={
        "name":    "n.name",
        "city":    "n.city",
        "state":   "n.state",
        "domains": "n.domains"
    })