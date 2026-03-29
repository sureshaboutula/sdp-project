# Databricks notebook source

import dlt
from pyspark.sql import functions as F

env = spark.conf.get("env")
schema_bronze = spark.conf.get("schema_bronze")
schema_silver = spark.conf.get("schema_silver")
schema_gold = spark.conf.get("schema_gold")

@dlt.table(
    name = f"sdp_catalog_{env}.{schema_gold}.vendors_monthly_trips_revenue",
    comment = "This table gives monthly rides and revenue for each vendor",
    table_properties = {
        "quality" : "gold",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def vendor_monthly_trips_agg():
    return (
        dlt.read(f"sdp_catalog_{env}.{schema_silver}.yellow_taxi_clean") \
        .groupBy("VendorID", "trip_year", "trip_month").agg(
            F.count("*").alias("total_rides"), 
            F.sum("total_amount").alias("total_revenue")
        ).select("VendorID", "trip_year", "trip_month", "total_rides", "total_revenue")
    )

@dlt.table(
    name = f"sdp_catalog_{env}.{schema_gold}.customers_order_summary",
    comment = "This table gives total spend and order count per customer",
    table_properties = {
        "quality" : "gold",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def customers_order_summary():
    return (
        dlt.read(f"sdp_catalog_{env}.{schema_silver}.orders_clean") \
        .groupBy("o_custkey").agg(
            F.count("o_orderkey").alias("total_orders"),
            F.sum("o_totalprice").alias("total_spend")
        ).select("o_custkey", "total_orders", "total_spend")
    )