# Structured Streaming

- link : https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

- Window Operations on Event Time

- Handling Late Data and Watermarking

- Code:

      import sys

      from pyspark.sql import SparkSession
      from pyspark.sql.functions import explode
      from pyspark.sql.functions import split
      from pyspark.sql.functions import window

      spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCountWindowed")\
        .getOrCreate()
        
        
      lines = spark\
        .readStream\
        .format('socket')\
        .option('host', "localhost")\
        .option('port', 9999)\
        .option('includeTimestamp', 'true')\
        .load()
        
        
       words = lines.select(
       explode(split(lines.value, ' ')).alias('word'),
       lines.timestamp
       )
       
       windowedCounts = words.groupBy(
       window(words.timestamp,'30 seconds','10 seconds'),     # 30 -> window duration and 10 -> Slide duration
       words.word
       ).count().orderBy('window')
       
       query = windowedCounts\
        .writeStream\
        .outputMode('complete')\
        .format('console') \
        .option("checkpointLocation", "checkpoint") \
        .option('truncate', 'false')\
        .start()
