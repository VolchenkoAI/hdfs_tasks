import pyspark
from pyspark.sql import SparkSession


spark = SparkSession.builder \
.appName("user50CreateExternalTable") \
.enableHiveSupport() \
.getOrCreate()

spark.sql( """
CREATE EXTERNAL TABLE IF NOT EXISTS user50.trans_loading (
    trans_id VARCHAR(20),
    trans_dt VARCHAR(20),
    client_id VARCHAR(20),
    amt DECIMAL(18,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/user50/trans_loading'
""")

spark.stop()
