# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window

#Common
def upsert_to_silver(batch_df, epoch_id, silver_table_path, primary_key_, batch_id_, variables_):
    spark = batch_df.sparkSession

    latest = window_func(batch_df, primary_key_, batch_id_)

    if not spark.catalog.tableExists(silver_table_path):

        latest.write.format("delta").saveAsTable(silver_table_path)

    else:
        (DeltaTable.forName(spark, silver_table_path).alias("d")
            .merge(latest.alias("n"), f"d.{primary_key_} = n.{primary_key_}")
            .whenMatchedUpdate(
                condition=f"n.{batch_id_} >= d.{batch_id_}",
                set= variables_)
            .whenNotMatchedInsertAll()
            .execute())   

def window_func(df,primary_key,batch_id):
    w = Window.partitionBy(primary_key).orderBy(F.col(batch_id).desc())
    return (df.withColumn("rn", F.row_number().over(w))
              .filter("rn = 1").drop("rn"))
    
#Customers
def customers_read_transformations(bronze_table_path):
    return  (spark.readStream.table(bronze_table_path)
                .drop("_rescued_data","_source_file","_source_name","_ingested_at")
                .withColumn("name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))
                .withColumn("domains", F.split(F.col("email"), "@")[1])
                .withColumn("_created_at",F.current_timestamp())
                .withColumn("_updated_at",F.current_timestamp())
                .drop('first_name','last_name','email')
                .filter(F.col("customer_id").isNotNull())
                .select("customer_id","name","city","state","domains","_batch_id","_created_at","_updated_at"))
#orders
def orders_read_transformations(bronze_table_path):
    return (spark.readStream.table(bronze_table_path)
            .withColumn("order_date", F.to_timestamp("order_date", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("year",F.year("order_date"))
            .withColumn("_created_at",F.current_timestamp())
            .withColumn("_updated_at",F.current_timestamp())
            .drop("_rescued_data","_source_file","_source_name","_ingested_at")
            .filter(
                (F.col("customer_id").isNotNull()) & 
                (F.col("order_id").isNotNull()) & 
                (F.col("product_id").isNotNull()))
            .select("order_id","customer_id","product_id","quantity","total_amount","year","_batch_id","order_date","_created_at","_updated_at")
            
            )
#products
def products_read_transformations(bronze_table_path):
    return (spark.readStream.table(bronze_table_path)
            .drop("_rescued_data","_source_file","_source_name","_ingested_at")
            .withColumn("_created_at",F.current_timestamp())
            .withColumn("_updated_at",F.current_timestamp())
            .withColumn("discounted_price",(F.col("price")*0.9))
            .withColumn("brand",F.upper(F.col("brand")))
            .filter(F.col("product_id").isNotNull())
    )
#regions
def regions_read_transformations(bronze_table_path):
    return (spark.readStream.table(bronze_table_path)
            .drop("_rescued_data","_source_file","_source_name","_ingested_at")
            .withColumn("_created_at",F.current_timestamp())
            .withColumn("_updated_at",F.current_timestamp())
            .filter(F.col("region_id").isNotNull()))