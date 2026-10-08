# Databricks notebook source
# MAGIC %run ../00-helpers/01.enviroment-config

# COMMAND ----------

# MAGIC %run ../00-helpers/03.silver-helpers

# COMMAND ----------

bronze_table_path = f"{catalog_name}.{bronze_schema}.products"
silver_table_path = f"{catalog_name}.{silver_schema}.products"
checkpoint_base = '/Volumes/anshlamba_yt_project/checkpoints/files/batch_ingest/silver'
checkpoint_path = f"{checkpoint_base}/products"

# COMMAND ----------

from functools import partial

products_df = products_read_transformations(bronze_table_path)

(products_df.writeStream
    .foreachBatch(partial(upsert_to_silver,
                          silver_table_path=silver_table_path,
                          primary_key_ = "product_id",
                          batch_id_ = "_batch_id",
                          variables_ ={
                                        "product_name": "n.product_name",
                                        "category": "n.category",
                                        "brand": "n.brand",
                                        "price": "n.price",
                                        "_batch_id":   "n._batch_id",
                                        "_updated_at": "n._updated_at"
                                    }))
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
    .awaitTermination())