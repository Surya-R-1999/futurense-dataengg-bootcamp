# Kafka 

- Using kafka, implementing word Count program

- Creating Kafka Topic:

      bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic wordCount
    
- PySpark Shell:
  
      
      from __future__ import print_function
      import sys
      from pyspark.sql import SparkSession
      from pyspark.sql.functions import explode
      from pyspark.sql.functions import split

      spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()
        
      lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "wordCount")\
        .load()\
        .selectExpr("CAST(value AS STRING)")    
        
      words = lines.select(
        # explode turns each item in an array into a separate row
        explode(
            split(lines.value, ' ')
        ).alias('word')
      )
      
      wordCounts = words.groupBy('word').count()
      
      
      query = wordCounts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()
