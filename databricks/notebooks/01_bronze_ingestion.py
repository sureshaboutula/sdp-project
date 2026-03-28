# Databricks notebook source

import dlt
from pyspark.sql import functions as F

env = spark.conf.get("env")
schema_bronze = spark.conf.get("schema_bronze")
schema_silver = spark.conf.get("schema_silver")
schema_gold = spark.conf.get("schema_gold")

@dlt.table(
    name= f"sdp_catalog_{env}.{schema_bronze}.yellow_taxi_raw",
    comment = "ingest taxi source data into bronze streaming table",
    table_properties = {
        "quality" : "bronze",
        "delta.autoOptimize.optimizeWrite": "true"
    }
                        
)
@dlt.expect("fare_amount_positive", "fare_amount > 0")
@dlt.expect("vendor_id_not_null", "vendor_id is not null")
def ingest_taxi_data_bronze():
    source_path = "/databricks-datasets/nyctaxi/tripdata/yellow/"
    return (
        spark.readStream.format("cloudFiles") \
            .option("cloudFiles.format", "csv") \
            .option("cloudFiles.schemaLocation", f"/Volumes/sdp_catalog_{env}/bronze/schema_store/yellow_taxi")
            .load(source_path)
    )

@dlt.table(
    name = f"sdp_catalog_{env}.{schema_bronze}.orders_raw",
    comment = "Ingest orders raw data into bronze streaming table",
    table_properties = {
        "quality" : "bronze",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)

@dlt.expect("cust_key_not_null", "o_custkey is not null")
def ingest_orders_data_bronze():
    return spark.readStream.table("samples.tpch.orders")