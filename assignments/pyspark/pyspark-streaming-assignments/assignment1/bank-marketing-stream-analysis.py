import subprocess
from pkg_resources._vendor.pyparsing import col
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StringType, StructField, StructType
from pyspark.sql.functions import *


#Create Spark Session
spark = SparkSession \
    .builder \
    .appName("BankMarketingStreamingAnalysis") \
    .getOrCreate()


topic_name = "bank-marketing-events"
bootstrap_servers = "localhost:9092"

create_topic_cmd = f"/opt/kafka/bin/kafka-topics.sh --create --bootstrap-server {bootstrap_servers} --replication-factor 1 --partitions 1 --topic {topic_name} --if-not-exists"
subprocess.run(create_topic_cmd.split(), check=True)

bootstrap_servers = "localhost:9092"
topic_name = "bank-marketing-events"
csv_file_path = "/mnt/c/Users/miles/Documents/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv"

producer_cmd = f"/opt/kafka/bin/kafka-console-producer.sh --broker-list {bootstrap_servers} --topic {topic_name}"

# Read contents of CSV file into a string
with open(csv_file_path, "r") as f:
    csv_contents = f.read()

# Encode string as bytes and pass as input to the producer process
subprocess.Popen(producer_cmd.split(), stdin=subprocess.PIPE).communicate(input=csv_contents.encode("utf-8"))

#Consume message from Kafka topic and create Dataset
from pyspark.sql.functions import from_csv, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

banking_schema = "age INT, job STRING, marital STRING, education STRING, default STRING, balance STRING, housing STRING, loan STRING, contact STRING, day STRING, month STRING, duration STRING, campaign STRING, pdays STRING, previous STRING, poutcome STRING, y STRING"

bankMarketingDF = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bank-marketing-events") \
    .option("header", "true") \
    .option("delimiter", ";") \
    .load() \
    .select(col("value").cast("string"))


bankMarketingcsvDF = bankMarketingDF.select(from_csv(col("value"), banking_schema, options={"delimiter": ";"}).alias("bankMarketing"))

bankMarketingcsvDF.printSchema()

#Schema definition for consuming message

bankMarketingcsvDF.createOrReplaceTempView("bankMarketing_")

df = spark.sql("select bankMarketing.age, count(bankMarketing.y) from bankMarketing_ where bankMarketing.y = 'yes' group by bankMarketing.age")

df.printSchema()

topic_name = "bank-marketing-subcount"
bootstrap_servers = "localhost:9092"

create_topic_cmd_2 = f"/opt/kafka/bin/kafka-topics.sh --create --bootstrap-server {bootstrap_servers} --replication-factor 1 --partitions 1 --topic {topic_name} --if-not-exists"
subprocess.run(create_topic_cmd_2.split(), check=True)


query = df\
    .writeStream \
    .outputMode("complete") \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("topic","bank-marketing-subcount") \
    .option("checkpointLocation", "bankMarketing") \
    .start()

query.awaitTermination()
