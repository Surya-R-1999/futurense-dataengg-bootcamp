# Databricks notebook source
file_location = "/FileStore/tables/sharemarket.csv"

rdd = sc.textFile(file_location)

# COMMAND ----------

rdd.take(2)

# COMMAND ----------

rdd = rdd.map(lambda x : x.split(','))
rdd.take(3)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
schema = StructType([ \
    StructField("MARKET",StringType(),True), \
    StructField("SERIES",StringType(),True), \
    StructField("SYMBOL",StringType(),True), \
    StructField("SECURITY", StringType(), True), \
    StructField("PREV_CL_PR", StringType(), True), \
    StructField("OPEN_PRICE", StringType(), True), \
    StructField("HIGH_PRICE", StringType(), True), \
    StructField("LOW_PRICE", StringType(), True), \
    StructField("CLOSE_PRICE", StringType(), True), \
    StructField("NET_TRDVAL", StringType(), True) ,\
    StructField("NET_TRDQTY", StringType(), True) ,\
    StructField("CORP_IND", StringType(), True), \
    StructField("TRADES", StringType(), True), \
    StructField("HI_52_WK", StringType(), True), \
    StructField("LO_52_WK", StringType(), True) \
  ])


# COMMAND ----------

df = spark.createDataFrame(rdd, schema)

# COMMAND ----------

df.show(5)

# COMMAND ----------

df = df.drop('CORP_IND')

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.createOrReplaceTempView("ShareMarket")

# COMMAND ----------

spark.sql("select * from ShareMarket limit 5").show()


# COMMAND ----------

# 1.Query to display the number of series present in the data.(using hive)

result1 = spark.sql("select count(distinct SERIES) as Total_Count from ShareMarket")
result1.coalesce(1).write.format("csv").option("header", "true").mode("append").save("/FileStore/tables/output1.txt")

# COMMAND ----------

# 2.Display the series present in the data.(using hive)

result2 = spark.sql("select distinct SERIES from ShareMarket ")
result2.coalesce(1).write.format("csv").option("header", "true").mode("append").save("/FileStore/tables/output2.txt")

# COMMAND ----------

# 3.Find the sum of all the prices in the each series.(Using hive)

result3  = spark.sql("select SERIES, sum(PREV_CL_PR) , sum(OPEN_PRICE) , sum(HIGH_PRICE) , sum(LOW_PRICE) , sum(CLOSE_PRICE) from ShareMarket group by SERIES")
result3.coalesce(1).write.format("csv").option("header", "true").mode("append").save("/FileStore/tables/output3.txt")

# COMMAND ----------

# 4.Display security,series with highest net trade value(use pyspark)
result4 = spark.sql("select SECURITY, SERIES, NET_TRDVAL from ShareMarket where NET_TRDVAL = (select max(NET_TRDVAl) from ShareMarket) ")
result4.coalesce(1).write.format("csv").option("header", "true").mode("append").save("/FileStore/tables/output4.txt")

# COMMAND ----------

# 5.Display the series whose sum of all prices greater than the net trade value.(Using pyspark)

result5 = spark.sql("select SERIES, round(PREV_CL_PR + OPEN_PRICE + HIGH_PRICE + LOW_PRICE + CLOSE_PRICE) as Total_price, NET_TRDVAL from ShareMarket where (PREV_CL_PR + OPEN_PRICE + HIGH_PRICE + LOW_PRICE + CLOSE_PRICE) > NET_TRDVAL ")
result5.coalesce(1).write.format("csv").option("header", "true").mode("append").save("/FileStore/tables/output5.txt")

# COMMAND ----------

# 6.Display the series with highest net trade quantity.(Using pyspark)
result6 = spark.sql("select series from ShareMarket where NET_TRDQTY = (select max(NET_TRDQTY) from ShareMarket)")
result6.coalesce(1).write.format("csv").option("header", "true").mode("append").save("/FileStore/tables/output6.txt")

# COMMAND ----------

# 7. Display the highest and lowest open price(Using sql)
result7 = spark.sql("select max(OPEN_PRICE) as max_open_price, min(OPEN_PRICE) as min_open_price from ShareMarket")
result7.coalesce(1).write.format("csv").option("header", "true").mode("append").save("/FileStore/tables/output7.txt")

# COMMAND ----------

# 8.Query to display the series which have trades more than 80.(Using SQL).

result8 = spark.sql("select series from ShareMarket where Trades  > 80")
result8.coalesce(1).write.format("csv").option("header", "true").mode("append").save("/FileStore/tables/output8.txt")

# COMMAND ----------

# 9.Display the difference between the net trade value net trade quantity for each series.(Using sql).
result9 = spark.sql("select series, sum(net_trdval - net_trdqty) as difference from ShareMarket group by SERIES ")
result9.coalesce(1).write.format("csv").option("header", "true").mode("append").save("/FileStore/tables/output9.txt")

# COMMAND ----------


