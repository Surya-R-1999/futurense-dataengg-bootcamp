
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[1]") \
                    .appName('pyspark-examples') \
                    .getOrCreate()


import pandas as pd
import numpy as np


data = spark.read.parquet("/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Bank_Analysis/parquet")

data.createOrReplaceTempView("Banking")

print(data.show())



print("Give marketing Success rate")

yes_count = spark.sql("SELECT COUNT(*) FROM Banking WHERE y = 'yes'").collect()[0][0]
total_count = spark.sql("SELECT COUNT(*) FROM Banking where y is not NUll").collect()[0][0]
success_rate = (yes_count / total_count) * 100
print("Success Rate : ",success_rate)

success_rate_str = "Success Rate : " + str(success_rate)

print("Give marketing failure rate")

no_count = spark.sql("SELECT COUNT(*) FROM Banking WHERE y = 'no'").collect()[0][0]
total_count = spark.sql("SELECT COUNT(*) FROM Banking where y is not NUll").collect()[0][0]
failure_rate = (no_count / total_count) * 100
print("Failure Rate : ",failure_rate)

failure_rate_str = "Failure Rate : " + str(failure_rate)

print("Maximum, Mean, and Minimum age of the average targeted customer")

maximum_Age = spark.sql("select max(age) from Banking where y is not Null").collect()[0][0]
mean_Age = spark.sql("select mean(age) from Banking where y is not Null").collect()[0][0]
minimum_Age = spark.sql("select min(age) from Banking where y is not Null").collect()[0][0]
print("Maximum Age, Minimum Age and Average Age : ", maximum_Age,minimum_Age, mean_Age)

age_wise_targetted_customers_str = "Maximum Age, Minimum Age and Average Age : " + str(maximum_Age) + str(minimum_Age) + " " +  str(mean_Age)

print("Check the quality of customers by checking the average balance, median balance of customers")

average_balance = spark.sql("select percentile_approx(balance,0.5) from Banking where y is not Null").collect()[0][0]
median_balance = spark.sql("select mean(balance) from Banking where y is not Null").collect()[0][0]
print("Average Balance and Median Banlance :",average_balance, median_balance)

quality_of_customers_str = "Average Balance and Median Banlance :" + str(average_balance) + " " + str(median_balance)

print("Check if age matters in marketing subscription for deposit")

result1 = spark.sql("SELECT count(*) as count, case when age < 13 then 'Kids' \
		   when (age >= 13) and (age <= 19) then 'Teenagers' \
           when (age > 19) and (age <= 30) then 'Youngsters' \
           when (age > 30) and (age < 50) then 'MiddleAgers' \
           else 'Seniors' END as age_group \
           FROM Banking where (y is not Null and y = 'yes')  \
           group by (age_group)")

for i in result1:
    print(i)

print("Check if marital status mattered for subscription to deposit.")

result2 = spark.sql("SELECT count(*) as count, marital \
           FROM Banking where (y is not Null and y = 'yes') \
           group by (marital)")

for i in result2:
    print(i)



print("Check if age and marital status together mattered for subscription to deposit scheme")

result3 = spark.sql("SELECT count(*) as count, marital,  case when age < 13 then 'Kids' \
		   when (age >= 13) and (age <= 19) then 'Teenagers' \
           when (age > 19) and (age <= 30) then 'Youngsters' \
           when (age > 30) and (age < 50) then 'MiddleAgers' \
           else 'Seniors' END as age_group \
           FROM Banking where (y is not Null and y = 'yes') \
           group by marital, age_group")

for i in result3:
    print(i)


result1.write.mode('overwrite').format('parquet').save("/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Bank_Analysis/Results/impact_of_age")

result2.write.mode('overwrite').format('parquet').save("/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Bank_Analysis/Results/impact_of_marital_status")

result3.write.mode('overwrite').format('parquet').save("/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Bank_Analysis/Results/impact_of_age_and_marital_status")

import pandas as pd

results = {}

results['Success_Rate'] = success_rate_str

results['Failure_Rate'] = failure_rate_str

results['age_wise_targetted_customers'] =  age_wise_targetted_customers_str

results['quality_of_customers'] = quality_of_customers_str

results_df = pd.DataFrame([results])

spark_df = spark.createDataFrame(results_df)


spark_df.write.mode('overwrite').format('parquet').save('/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Bank_Analysis/Results/analysis')


