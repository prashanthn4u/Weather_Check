from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql import types
from pyspark.sql import functions
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_date


spark = SparkSession.builder.appName('weathercheck').getOrCreate()
#Load weather data csv files
df = spark.read\
    .format("csv")\
    .options(header='True', inferSchema='True', delimiter=',')\
    .load("file:///D:/Data Engineer Test/")
#df.show()
#df.printSchema()
#Set Row Size to 2MB
PARQUET_BLOCK_SIZE = 2 * 1024 * 1024
#Save as parquet file
df.repartition(3).write.option("parquet.block.size",PARQUET_BLOCK_SIZE).mode("overwrite").parquet("file:///D:/parquet_output/")


#Load parquet files
parq_df = spark.read\
    .format("parquet")\
    .load("file:///D:/parquet_output/")

#Query for the hottest temperature and get data as well as Region
hot_day = parq_df\
    .select("ObservationDate", "ScreenTemperature", "Region")\
    .sort(col("ScreenTemperature").desc())\
    .limit(1)\
    .select(to_date(col("ObservationDate"), "yyyy-MM-dd").alias("hottest_day"),
        col("ScreenTemperature").alias("hottest_Temperature"),
        col("Region").alias("hottest_Region"))
hot_day.show(truncate=False)
