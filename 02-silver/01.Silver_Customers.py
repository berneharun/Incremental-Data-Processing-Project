# Databricks notebook source
# MAGIC %run ../00-helpers/01.enviroment-config

# COMMAND ----------

# MAGIC %run ../00-helpers/03.silver-helpers
# MAGIC

# COMMAND ----------

bronze_table_path = f"{catalog_name}.{bronze_schema}.customers"
silver_table_path = f"{catalog_name}.{silver_schema}.customers"
checkpoint_base = '/Volumes/anshlamba_yt_project/checkpoints/files/batch_ingest/silver'
checkpoint_path = f"{checkpoint_base}/customers"

# COMMAND ----------

from functools import partial

customers_df = customers_read_transformations(bronze_table_path)

(customers_df.writeStream
    .foreachBatch(partial(upsert_to_silver,
                          silver_table_path=silver_table_path,
                          primary_key_ = "customer_id",
                          batch_id_ = "_batch_id",
                          variables_ ={
                                        "name":        "n.name",
                                        "city":        "n.city",
                                        "state":       "n.state",
                                        "domains":     "n.domains",
                                        "_batch_id":   "n._batch_id",
                                        "_updated_at": "n._updated_at"
                                    }))
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
    .awaitTermination())