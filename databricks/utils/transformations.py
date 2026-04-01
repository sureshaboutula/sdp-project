from pyspark.sql import functions as F

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

def transform_orders_data(df):
    return df.withColumn("order_year", F.year(F.col("o_orderdate"))) \
        .withColumn("order_month", F.month(F.col("o_orderdate")))

def aggregate_vendor_monthly_trips(df):
    return df.groupBy("VendorID", "trip_year", "trip_month").agg(
            F.count("*").alias("total_rides"), 
            F.sum("total_amount").alias("total_revenue")
        ).select("VendorID", "trip_year", "trip_month", "total_rides", "total_revenue")

def aggregate_customers_orders(df):
    return df.groupBy("o_custkey").agg(
            F.count("o_orderkey").alias("total_orders"),
            F.sum("o_totalprice").alias("total_spend")
        ).select("o_custkey", "total_orders", "total_spend")