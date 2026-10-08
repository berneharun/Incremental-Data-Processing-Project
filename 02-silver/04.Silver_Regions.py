# Databricks notebook source
# MAGIC %run ../00-helpers/01.enviroment-config

# COMMAND ----------

# MAGIC %run ../00-helpers/03.silver-helpers

# COMMAND ----------

bronze_table_path = f"{catalog_name}.{bronze_schema}.regions"
silver_table_path = f"{catalog_name}.{silver_schema}.regions"
checkpoint_base = '/Volumes/anshlamba_yt_project/checkpoints/files/batch_ingest/silver'
checkpoint_path = f"{checkpoint_base}/regions"

# COMMAND ----------

from functools import partial

regions_df = regions_read_transformations(bronze_table_path)

(regions_df.writeStream
    .foreachBatch(partial(upsert_to_silver,
                          silver_table_path=silver_table_path,
                          primary_key_ = "region_id",
                          batch_id_ = "_batch_id",
                          variables_ ={
                                        "region": "n.region"
                                    }))
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
    .awaitTermination())