from pyspark.sql import Row
from datetime import date, datetime

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
df = spark.createDataFrame(data)