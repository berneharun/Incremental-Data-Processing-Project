# Databricks notebook source
# MAGIC %run ../00-helpers/01.enviroment-config

# COMMAND ----------

# MAGIC %run ../00-helpers/04.gold-helpers

# COMMAND ----------

silver_table_path = f"{catalog_name}.{silver_schema}.regions"
gold_table_path = f"{catalog_name}.{gold_schema}.dim_regions"

# COMMAND ----------

regions_silver_table = spark.table(silver_table_path )

# COMMAND ----------

upsert_dim(
    src=regions_silver_table,
    gold_table=gold_table_path,
    business_key="region_id",
    attr_cols={
        "region":    "n.region"
    })