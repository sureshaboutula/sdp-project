# Databricks notebook source

#Create variables for interactive deleopment - In Job these will be overwritten
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("schema_bronze", "bronze")
dbutils.widgets.text("schema_silver", "silver")
dbutils.widgets.text("schema_gold", "gold")

# Get variables from .yml file
env = dbutils.widgets.get("env")
schema_bronze = dbutils.widgets.get("schema_bronze")
schema_silver = dbutils.widgets.get("schema_silver")
schema_gold = dbutils.widgets.get("schema_gold")

# Optimize and ZORDER code
def optimize_table(table_name, *zorder_columns):
  spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({','.join(zorder_columns)})")

optimize_table(f"sdp_catalog_{env}.{schema_bronze}.yellow_taxi_raw", "VendorID", "tpep_pickup_datetime")
optimize_table(f"sdp_catalog_{env}.{schema_bronze}.orders_raw", "o_custkey", "o_orderdate")

# Vaccum function
def vacuum_table(table_name, retention_hours=168):
  spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")

vacuum_table(f"sdp_catalog_{env}.{schema_bronze}.yellow_taxi_raw")
vacuum_table(f"sdp_catalog_{env}.{schema_bronze}.orders_raw")

