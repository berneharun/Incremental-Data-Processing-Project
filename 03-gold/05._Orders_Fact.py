# Databricks notebook source
# MAGIC %run ../00-helpers/01.enviroment-config

# COMMAND ----------

# MAGIC %run ../00-helpers/04.gold-helpers

# COMMAND ----------

orders_silver_table_path = f"{catalog_name}.{silver_schema}.orders"
customers_gold_table_path = f"{catalog_name}.{gold_schema}.dim_customers"
products_gold_table_path = f"{catalog_name}.{gold_schema}.dim_products"
gold_table_path = f"{catalog_name}.{gold_schema}.fact_orders"

# COMMAND ----------

orders = spark.read.table(orders_silver_table_path)
dim_customers = spark.read.table(customers_gold_table_path).select("customer_id", "customer_sk")
dim_products = spark.read.table(products_gold_table_path).select("product_id", "product_sk")

# COMMAND ----------

fact_df = (orders
    .join(dim_customers, "customer_id", "left")
    .join(dim_products,  "product_id",  "left")
    .withColumn("customer_sk", F.coalesce("customer_sk", F.lit(-1)))
    .withColumn("product_sk",  F.coalesce("product_sk",  F.lit(-1)))
    .select("order_id", "customer_sk", "product_sk", "quantity", "total_amount"))

# COMMAND ----------

upsert_dim(
           src = fact_df,
           gold_table=gold_table_path,
           business_key="order_id",
           attr_cols=["customer_sk", "product_sk", "quantity", "total_amount"])