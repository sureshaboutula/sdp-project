from pyspark.sql import Row
from datetime import date, datetime
from utils.transformations import transform_taxi_data, transform_orders_data

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
#df = spark.createDataFrame(data)

def test_derived_columns_exist(spark):
    df = spark.createDataFrame(data)
    result_df = transform_taxi_data(df)
    assert "trip_year" in result_df.columns
    assert "trip_month" in result_df.columns
    assert "trip_hour" in result_df.columns

def test_column_data_types(spark):
    df = spark.createDataFrame(data)
    result_df = transform_taxi_data(df)
    schema_dict = dict(result_df.dtypes)
    assert schema_dict["fare_amount"] == "double", "fare_amount should be double"
    assert schema_dict["trip_distance"] == "double", "trip_distance should be double"
    assert schema_dict["total_amount"] == "double", "total_amount should be double"
    assert schema_dict["pickup_datetime"] == "timestamp", "pickup_datetime should be timestamp"
    assert schema_dict["dropoff_datetime"] == "timestamp", "dropoff_datetime should be timestamp"

def test_deduplication(spark):
    df = spark.createDataFrame(data)
    result_df = transform_taxi_data(df)
    assert result_df.count() == 4
