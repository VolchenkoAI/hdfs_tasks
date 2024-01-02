import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row


spark = SparkSession.builder \
    .appName("user50_spark_loader_bank_accounts") \
    .enableHiveSupport() \
    .config("spark.jars", "/home/trainer/postgresql-42.6.0.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://") \
    .option("driver", "") \
    .option("dbtable", "") \
    .option("user", "") \
    .option("password", "") \
    .load()

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("user50.bank_clients")

spark.stop()