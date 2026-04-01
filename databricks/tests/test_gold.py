from pyspark.sql import Row
from pyspark.sql import functions as F
from datetime import date, datetime
from utils.transformations import transform_taxi_data, aggregate_vendor_monthly_trips

data = [
    Row(VendorID=1, tpep_pickup_datetime=datetime(2016, 6, 21, 22, 11, 12), tpep_dropoff_datetime=datetime(2016, 6, 21, 22, 17, 59), 
        fare_amount=6.5, trip_distance=1.12, total_amount=6.8),
    Row(VendorID=1, tpep_pickup_datetime=datetime(2016, 3, 10, 7, 26, 41), tpep_dropoff_datetime=datetime(2016, 3, 10, 7, 30, 38), 
        fare_amount=5.0, trip_distance=0.81, total_amount=5.8),
    Row(VendorID=1, tpep_pickup_datetime=datetime(2016, 6, 21, 22, 11, 12), tpep_dropoff_datetime=datetime(2016, 6, 21, 22, 17, 59), 
        fare_amount=-6.5, trip_distance=1.12, total_amount=6.8),
    Row(VendorID=None, tpep_pickup_datetime=datetime(2016, 6, 21, 22, 11, 12), tpep_dropoff_datetime=datetime(2016, 6, 21, 22, 17, 59), 
        fare_amount=-6.5, trip_distance=1.12, total_amount=6.8),
    Row(VendorID=2, tpep_pickup_datetime=datetime(2016, 3, 10, 8, 16, 23), tpep_dropoff_datetime=datetime(2016, 3, 10, 8, 33, 42), 
        fare_amount=13.0, trip_distance=2.51, total_amount=16.56)
]

def test_columns_exist(spark):
    df = spark.createDataFrame(data)
    transformed_df = transform_taxi_data(df)
    aggregation_df = aggregate_vendor_monthly_trips(transformed_df)
    columns = ["VendorID", "trip_year", "trip_month", "total_rides", "total_revenue"]
    for column in columns:
        assert column in aggregation_df.columns, f"{column} is missing in aggregation table"

def test_aggregated_rows_count(spark):
    df = spark.createDataFrame(data)
    transformed_df = transform_taxi_data(df)
    aggregation_df = aggregate_vendor_monthly_trips(transformed_df)
    assert aggregation_df.count() == 4, "aggregated rows count is wrong"

def test_aggregated_values(spark):
    df = spark.createDataFrame(data)
    transformed_df = transform_taxi_data(df)
    aggregation_df = aggregate_vendor_monthly_trips(transformed_df)
    result_df = aggregation_df.filter(F.col("VendorID") == 1) \
    .agg(F.sum("total_rides").alias("total_rides"), F.sum("total_revenue").alias("total_revenue")) \
    .collect()[0]
    assert result_df["total_rides"] == 2, "total_rides calculated wrongly"
    assert result_df["total_revenue"] == 12.6, "total_revenue calculation is wrong"



