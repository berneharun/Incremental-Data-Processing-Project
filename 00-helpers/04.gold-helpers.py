# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F

def upsert_dim(src, gold_table, business_key, attr_cols):

    update_set  = {c: f"n.{c}" for c in attr_cols}
    insert_vals = {business_key: f"n.{business_key}", **update_set}

   
    changed = " OR ".join(f"NOT (d.{c} <=> n.{c})" for c in attr_cols)

    (DeltaTable.forName(spark, gold_table).alias("d")
        .merge(src.alias("n"), f"d.{business_key} = n.{business_key}")
        .whenMatchedUpdate(condition=changed, set=update_set)
        .whenNotMatchedInsert(values=insert_vals)
        .execute())