import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[1]") \
                    .appName('pyspark-examples') \
                    .getOrCreate()


data  = spark.read.load("/mnt/c/Users/miles/Documents/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv",format = "csv", sep = ";", delimiter=';',header=True,inferSchema=True)


data.createOrReplaceTempView("Banking")

result = spark.sql("SELECT count(*) as count, case when age < 13 then 'Kids' \
		   when (age >= 13) and (age <= 19) then 'Teenagers' \
           when (age > 19) and (age <= 30) then 'Youngsters' \
           when (age > 30) and (age < 50) then 'MiddleAgers' \
           else 'Seniors' END as age_group \
           FROM Banking where (y is not Null and y = 'yes')  \
           group by (age_group)")

for i in result:
    print(i)



result.write.format('parquet').save("/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/PySpark/fileFormat/parquet/results_csv")

resultant_data  = spark.read.load("/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/PySpark/fileFormat/parquet/results_csv",format = "parquet",header=True,inferSchema=True)

print(resultant_data.show())

resultant_data.createOrReplaceTempView("Banking2")

result2 = spark.sql("SELECT * from Banking2 where count > 2000")

result2.write.format('avro').save("/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/PySpark/fileFormat/avro/results2_csv")

resultant_data2  = spark.read.load("/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/PySpark/fileFormat/avro/results2_csv",format = "avro",header=True,inferSchema=True)

print(resultant_data2.show())
