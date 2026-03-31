# Databricks notebook source

import dlt
from pyspark.sql import functions as F

env = spark.conf.get("env")
schema_bronze = spark.conf.get("schema_bronze")
schema_silver = spark.conf.get("schema_silver")
schema_gold = spark.conf.get("schema_gold")

def transform_taxi_data(df):
    return df.dropDuplicates(["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"]) \
            .withColumn("fare_amount",F.col("fare_amount").cast("double")) \
            .withColumn("trip_distance",F.col("trip_distance").cast("double")) \
            .withColumn("total_amount",F.col("total_amount").cast("double")) \
            .withColumn("pickup_datetime",F.col("tpep_pickup_datetime").cast("timestamp")) \
            .withColumn("dropoff_datetime",F.col("tpep_dropoff_datetime").cast("timestamp")) \
            .withColumn("trip_year",F.year(F.col("tpep_pickup_datetime"))) \
            .withColumn("trip_month",F.month(F.col("tpep_pickup_datetime")))\
            .withColumn("trip_hour",F.hour(F.col("tpep_pickup_datetime")))

@dlt.table(
    name= f"sdp_catalog_{env}.{schema_silver}.yellow_taxi_clean",
    comment = "read taxi bronze data into silver layer and clean it",
    table_properties = {
        "quality" : "silver",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
@dlt.expect_or_drop("valid_fare_amount", "fare_amount > 0")
@dlt.expect_or_drop("vendor_id_not_null", "VendorID is not null")
def yellow_taxi_data_clean():
    df = dlt.read(f"sdp_catalog_{env}.{schema_bronze}.yellow_taxi_raw")
    return transform_taxi_data(df)  

def transform_orders_data(df):
    return df.withColumn("order_year", F.year(F.col("o_orderdate"))) \
        .withColumn("order_month", F.month(F.col("o_orderdate")))

@dlt.table(
    name= f"sdp_catalog_{env}.{schema_silver}.orders_clean",
    comment = "read orders bronze data into silver layer and clean it",
    table_properties = {
        "quality" : "silver",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)

@dlt.expect_or_drop("cust_key_not_null", "o_custkey is not null")
def clean_orders_data():
    df = dlt.read(f"sdp_catalog_{env}.{schema_bronze}.orders_raw")
    return transform_orders_data(df)
