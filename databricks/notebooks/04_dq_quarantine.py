# Databricks notebook source

import dlt
from pyspark.sql import functions as F

env = spark.conf.get("env")
schema_bronze = spark.conf.get("schema_bronze")
schema_silver = spark.conf.get("schema_silver")
schema_gold = spark.conf.get("schema_gold")
schema_quarantine = spark.conf.get("schema_quarantine")

@dlt.table(
    name= f"sdp_catalog_{env}.{schema_quarantine}.yellow_taxi_bad_data",
    comment = "read bad taxi bronze data into quarantine table",
    table_properties = {
        "quality" : "quarantine",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)

def yellow_taxi_bad_data_clean():
    return(
        dlt.read(f"sdp_catalog_{env}.{schema_bronze}.yellow_taxi_raw") \
        .filter(F.col("fare_amount") < 0)
    )