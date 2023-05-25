from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, monotonically_increasing_id
# Create a SparkSession
spark = SparkSession.builder \
    .appName("Transformation") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.sql("select * from staging.staging_table")

# Convert column lpep_pickup_datetime & lpep_dropoff_datetime into datetime format
df = df.withColumn("lpep_pickup_datetime", from_unixtime(col("lpep_pickup_datetime") / 1000000).cast("timestamp"))
df = df.withColumn("lpep_dropoff_datetime", from_unixtime(col("lpep_dropoff_datetime") / 1000000).cast("timestamp"))
df = df.dropna(subset=["Payment_type"])
df = df.drop("ehail_fee")
df = df.withColumn("VendorID",col("VendorID").cast("integer"))
df = df.withColumn("Passenger_count",col("Passenger_count").cast("integer"))
df = df.withColumn("trip_type",col("trip_type").cast("integer"))
df = df.withColumn("Payment_type",col("Payment_type").cast("integer"))
df = df.withColumn("RateCodeID",col("RateCodeID").cast("integer"))





# Transform
from pyspark.sql.functions import when
df = df.withColumn("LPEPVendor", when(col("VendorID") == 1, "Creative Mobile Technologies.LLC") \
                                 .when(col("VendorID") == 2, "Verifone Inc") \
                                 .otherwise("VendorNotProvided"))



df = df.withColumn("PaymentMethod", when(col("Payment_type") == 1, "Credit card") \
                                        .when(col("Payment_type") == 2, "Cash") \
                                        .when(col("Payment_type") == 3, "No charge") \
                                        .when(col("Payment_type") == 4, "Dispute") \
                                        .otherwise("PaymentNotInType"))

df = df.withColumn("RateDescription", when(col("RateCodeID") == 1, "Standard rate") \
                                    .when(col("RateCodeID") == 2, "JFK") \
                                    .when(col("RateCodeID") == 3, "Newark") \
                                    .when(col("RateCodeID") == 4, "Nassau or Westchester") \
                                    .when(col("RateCodeID") == 5, "Negotiated fare") \
                                    .otherwise("GroupRide"))

df = df.withColumn('TripDescription', when(col("trip_type") == 1, 'Street-hail')\
                    .when(col("trip_type") == 2, 'Dispatch')\
                    .otherwise('TripTypeUnknown'))

df = df.withColumn("FlagDescription", when(col("Store_and_fwd_flag") == "Y", "Store and Forward") \
                                    .otherwise("Not Store and Forward"))

spark.sql("CREATE DATABASE IF NOT EXISTS staging")
spark.sql("USE staging")
df.write.mode('overwrite').saveAsTable('transform_table')

# Making dim and fact table 




