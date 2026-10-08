# Databricks notebook source
# MAGIC %run ../00-helpers/01.enviroment-config

# COMMAND ----------

landing_path = '/Volumes/anshlamba_yt_project/landing/files'
checkpoint_base = '/Volumes/anshlamba_yt_project/checkpoints/files/batch_ingest/bronze'

# COMMAND ----------

entities = {
    "customers": ["customer_first", "customers_second"],
    "orders":    ["orders_first",   "orders_second"],
    "products":  ["products_first", "products_second"],
    "regions":   ["regions"],
}

# COMMAND ----------

from pyspark.sql import functions as F

for table, sources in entities.items():
    bronze_table = f"{catalog_name}.{bronze_schema}.{table}"

    for file in sources:
        source_path = f"{landing_path}/*/{file}"
        checkpoint_path = f"{checkpoint_base}/{table}/{file}"

        (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
            .load(source_path)
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_source_name", F.lit(file))
            .withColumn("_batch_id", F.regexp_extract(F.col("_metadata.file_path"), r"/batch_(\d+)/", 1).cast("int"))
            .withColumn("_ingested_at", F.current_timestamp())
            .writeStream
            .option("checkpointLocation", f"{checkpoint_path}/checkpoint")
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(bronze_table)
            .awaitTermination()
        )