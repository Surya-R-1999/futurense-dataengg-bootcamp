import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[1]") \
                    .appName('pyspark-examples') \
                    .getOrCreate()



resultant_data1  = spark.read.load("/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Bank_Analysis/Results/impact_of_age",format = "parquet",header=True,inferSchema=True)

resultant_data2  = spark.read.load("/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Bank_Analysis/Results/impact_of_marital_status",format = "parquet",header=True,inferSchema=True)

resultant_data3  = spark.read.load("/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Bank_Analysis/Results/impact_of_age_and_marital_status",format = "parquet",header=True,inferSchema=True)

resultant_data4  = spark.read.load("/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Bank_Analysis/Results/analysis",format = "parquet",header=True,inferSchema=True)

print(resultant_data1.show())

print(resultant_data2.show())

print(resultant_data3.show())

print(resultant_data4.show())


resultant_data1.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/banking_analysis") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("dbtable", "impact_of_age") \
    .option("user", "pyspark") \
    .option("password", "pyspark") \
    .mode("overwrite") \
    .save()

resultant_data2.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/banking_analysis") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("dbtable", "impact_of_marital_status") \
    .option("user", "pyspark") \
    .option("password", "pyspark") \
    .mode("overwrite") \
    .save()

resultant_data3.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/banking_analysis") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("dbtable", "impact_of_age_and_marital_status") \
    .option("user", "pyspark") \
    .option("password", "pyspark") \
    .mode("overwrite") \
    .save()

resultant_data4.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/banking_analysis") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("dbtable", "analysis") \
    .option("user", "pyspark") \
    .option("password", "pyspark") \
    .mode("overwrite") \
    .save()



