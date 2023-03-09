import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[1]") \
                    .appName('pyspark-examples') \
                    .getOrCreate()

df1 = spark.read.load("hdfs://localhost:9000/user/training/bankmarketing/staging",format = "parquet")

df1.createOrReplaceTempView("Banking")

df2 = spark.sql("select * from Banking where job != 'unknown'")

df3 = df2.withColumn('contact', regexp_replace('contact', 'unknown', '123467890'))

df3 = df3.withColumn('poutcome', regexp_replace('poutcome', 'unknown', 'na'))


df3.write.mode('overwrite').format('parquet').save("hdfs://localhost:9000/user/training/bankmarketing/validated")

try:
  df3.write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/user/training/bankmarketing/staging/yyyymmdd/success')
except:
  df3.write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/user/training/bankmarketing/staging/yyyymmdd/failure')




