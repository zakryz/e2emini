from pyspark.sql import SparkSession



#enableHiveSupport() -> enables sparkSession to connect with Hive
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()


create_table_query = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS green_taxi2022 (
        VendorID BIGINT,
        lpep_pickup_datetime BIGINT,
        lpep_dropoff_datetime BIGINT,
        store_and_fwd_flag STRING,
        RatecodeID DOUBLE,
        PULocationID BIGINT,
        DOLocationID BIGINT,
        passenger_count DOUBLE,
        trip_distance DOUBLE,
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        ehail_fee DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE,
        payment_type DOUBLE,
        trip_type DOUBLE,
        congestion_surcharge DOUBLE
    )
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/asd123/e2e'
    TBLPROPERTIES ('skip.header.line.count'='1')
'''
#load table dari hdfs ke query
spark.sql(create_table_query)
# Show Tables
spark.sql("show tables").show()

# Execute HiveQL query
df = spark.sql("select * from default.green_taxi2022")
spark.sql("CREATE DATABASE IF NOT EXISTS staging")
spark.sql("USE staging")
df.write.mode('overwrite').saveAsTable('staging_table')

