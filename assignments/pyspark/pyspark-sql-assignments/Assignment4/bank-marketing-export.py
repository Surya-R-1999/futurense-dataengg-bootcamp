import subprocess
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[1]") \
                     .appName('pyspark-examples') \
                     .getOrCreate()
                     
df = spark.read.load("hdfs://localhost:9000/user/training/bankmarketing/processed", format = "avro")

df.write \
.format("jdbc") \
.option("url", "jdbc:mysql://localhost:3306/bankmarketing") \
.option("driver", "com.mysql.jdbc.Driver") \
.option("dbtable", "subscription_count") \
.option("user", "pyspark") \
.option("password", "pyspark") \
.mode("overwrite") \
.save()

try:
 df.write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/user/training/bankmarketing/processed/yyyymmdd/success')
except:
 df.write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/user/training/bankmarketing/processed/yyyymmdd/failure')
