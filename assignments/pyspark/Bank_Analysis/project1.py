import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[1]") \
                    .appName('pyspark-examples') \
                    .getOrCreate()




df  = spark.read.load("/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Bank_Analysis/bankmarketdata.csv",format = "csv", sep = ";", delimiter=';',header=True,inferSchema=True)

df.write.mode('overwrite').parquet("/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Bank_Analysis/parquet")
