import subprocess
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[1]") \
                    .appName('pyspark-examples') \
                    .getOrCreate()

subprocess.run(["echo","Loading data from local system to HDFS"])

subprocess.run(["hadoop", "fs", "-put",  "/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/pyspark-sql-assignments/Assignment4/bankmarketdata.csv", "hdfs://localhost:9000/user/training/bankmarketing/raw"])

subprocess.run(["echo","Data Loaded Successfully to HDFS"])

df = spark.read.load("hdfs://localhost:9000/user/training/bankmarketing/raw/bankmarketdata.csv",format = "csv", sep = ";", delimiter=';',header=True,inferSchema=True)

df.write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/user/training/bankmarketing/staging')

try:
      subprocess.run(["hadoop", "fs", "-put" ,"/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Assignment4/pyspark-sql-assignments/bankmarketdata.csv", "hdfs://localhost:9000/user/training/bankmarketing/raw/yyyymmdd/success"])
except:
      subprocess.run(["hadoop", "fs", "-put" ,"/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Assignment4/pyspark-sql-assignments/bankmarketdata.csv" ,"hdfs://localhost:9000/user/training/bankmarketing/raw/yyyymmdd/failure"])
