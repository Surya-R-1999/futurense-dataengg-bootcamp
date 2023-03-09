import subprocess
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[1]") \
                    .appName('pyspark-examples') \
                    .getOrCreate()

df = spark.read.load("hdfs://localhost:9000/user/training/bankmarketing/validated", format = "parquet")

df.createOrReplaceTempView("Banking_Filter")

age_group_count = spark.sql("SELECT age, count(y) as count from Banking_Filter group by age")
 
age_group_count_gt_2000 = spark.sql("SELECT age, count(y) from Banking_Filter group by age having count(y) > 2000").show()
 
age_group_count.write.mode('overwrite').format('avro').save('hdfs://localhost:9000/user/training/bankmarketing/processed')
 
try:
    age_group_count.write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/user/training/bankmarketing/processed/yyyymmdd/success')
except:
    age_group_count.write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/user/training/bankmarketing/processed/yyyymmdd/failure')
