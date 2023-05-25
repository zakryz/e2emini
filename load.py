from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, monotonically_increasing_id
# Create a SparkSession
spark = SparkSession.builder \
    .appName("Transformation") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()


df = spark.sql("select * from staging.transform_table")

df_vendor = df.selectExpr("VendorID", "LPEPVendor").distinct()
df_payment = df.selectExpr("Payment_type", "PaymentMethod").distinct()
df_rate = df.selectExpr("RateCodeID", "RateDescription").distinct()
df_trip = df.selectExpr("Trip_type","TripDescription").distinct()
df_flag = df.selectExpr("Store_and_fwd_flag", "FlagDescription").distinct()
df_fact = df.selectExpr("VendorID","Payment_type",'RateCodeID','Trip_type',"Store_and_fwd_flag","lpep_pickup_datetime", "lpep_dropoff_datetime",
                         "Passenger_count", "Trip_distance", "PULocationID", "DOLocationID", "Fare_amount", "Extra",
                         "MTA_tax", "Improvement_surcharge", "Tip_amount", "Tolls_amount", "Total_amount", "congestion_surcharge")




spark.sql("CREATE DATABASE IF NOT EXISTS final")
spark.sql("USE final")
df_fact.write.mode('overwrite').saveAsTable('fact_tbl')

df_vendor.write.mode("overwrite").saveAsTable('vendor_dim')
df_payment.write.mode("overwrite").saveAsTable('payment_dim')
df_rate.write.mode("overwrite").saveAsTable('rate_dim')
df_flag.write.mode("overwrite").saveAsTable('payment_dim')
df_trip.write.mode("overwrite").saveAsTable('trip_dim')