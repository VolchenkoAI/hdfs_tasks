import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder \
    .appName("user50ReportBuild") \
    .enableHiveSupport() \
    .getOrCreate()

table_name = "user50.report_turnover"

spark.sql( """ 
    CREATE TABLE IF NOT EXISTS """ + table_name + """ (
    ID STRING,
    Phone STRING,
    Name STRING,
    Turnover DECIMAL(18,2))
    STORED AS PARQUET
""")

spark.sql("""
    INSERT INTO """ + table_name + """
    SELECT cl.client_id,
    cl.phone,
    (cl.first_name || ' ' || cl.patronymic) Name,
    SUM(ABS(tr.amt)) Turnover
    FROM user50.bank_clients cl
    LEFT JOIN user50.trans_loading tr
    ON cl.client_id = tr.client_id
    GROUP BY cl.client_id, cl.phone, (cl.first_name || ' ' || cl.patronymic)
""")

spark.stop()