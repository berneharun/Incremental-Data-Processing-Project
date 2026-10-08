# Databricks notebook source
# MAGIC %run ../00-helpers/01.enviroment-config

# COMMAND ----------

# MAGIC %run ../00-helpers/03.silver-helpers

# COMMAND ----------

bronze_table_path = f"{catalog_name}.{bronze_schema}.orders"
silver_table_path = f"{catalog_name}.{silver_schema}.orders"
checkpoint_base = '/Volumes/anshlamba_yt_project/checkpoints/files/batch_ingest/silver'
checkpoint_path = f"{checkpoint_base}/orders"

# COMMAND ----------

from functools import partial

orders_df = orders_read_transformations(bronze_table_path)

(orders_df.writeStream
    .foreachBatch(partial(upsert_to_silver,
                          silver_table_path=silver_table_path,
                          primary_key_ = "order_id",
                          batch_id_ = "_batch_id",
                          variables_ ={
                                        "customer_id": "n.customer_id",
                                        "product_id": "n.product_id",
                                        "order_date": "n.order_date",
                                        "quantity": "n.quantity",
                                        "total_amount": "n.total_amount",
                                        "_batch_id":   "n._batch_id",
                                        "_updated_at": "n._updated_at"
                                    }))
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
    .awaitTermination())