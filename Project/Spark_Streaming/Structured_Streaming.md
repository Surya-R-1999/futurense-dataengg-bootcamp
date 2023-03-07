# Structured Streaming

- link : https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

- Window Operations on Event Time

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

- Handling Late Data and Watermarking

- Code:


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
              .option('host', 'localhost')\
              .option('port', 9999)\
              .option('includeTimestamp', 'true')\
              .load()

      words = lines.select(
              explode(split(lines.value, ' ')).alias('word'),
              lines.timestamp
          )

      windowedCounts = words.withWatermark("timestamp", "30 seconds") \
              .groupBy(\
              window(words.timestamp, '30 seconds', '10 seconds'),
              words.word
          ).count().orderBy('window')

      query = windowedCounts\
              .writeStream\
              .outputMode('complete')\
              .format('console') \
              .option("checkpointLocation", "checkpoint") \
              .option('truncate', 'false')\
              .start()

- Loans Example:

      from pyspark.sql import SparkSession
      from pyspark.sql.types import TimestampType, StringType, StructField, StructType
      from pyspark.sql.functions import window
      from pyspark.sql.types import *


      spark = SparkSession \
          .builder \
          .appName("LoanStreamingAnalysis") \
          .getOrCreate()


      schema = StructType([ StructField("time", TimestampType(), True),
                            StructField("customer", StringType(), True),
                            StructField("loanId", StringType(), True),
                            StructField("status", StringType(), True)])

      loansStreamingDF = (
        spark
          .readStream
          .schema(schema)
          .option("maxFilesPerTrigger", 1)
          .json("/mnt/c/Users/miles/Documents/futurense_hadoop-pyspark/labs/dataset/loan/")
      )

      loanStatusCountsDF = (
        loansStreamingDF
          .groupBy(
            loansStreamingDF.status
          )
          .count()
      )

      query = (
        loanStatusCountsDF
          .writeStream
          .format("console")
          .outputMode("complete")
          .start()
      )

      query.awaitTermination()
      
- Loans Watermarked Example:

      from pyspark.sql import SparkSession
      from pyspark.sql.types import TimestampType, StringType, StructField, StructType
      from pyspark.sql.functions import window
      from pyspark.sql.types import *


      spark = SparkSession \
          .builder \
          .appName("LoanStreamingAnalysis") \
          .getOrCreate()


      schema = StructType([ StructField("time", TimestampType(), True),
                            StructField("customer", StringType(), True),
                            StructField("loanId", StringType(), True),
                            StructField("status", StringType(), True)])


      loansStreamingDF = (
        spark
          .readStream
          .schema(schema)
          .option("maxFilesPerTrigger", 1)
          .json("/mnt/c/Users/miles/Documents/futurense_hadoop-pyspark/labs/dataset/loan/")
      )

      loanStatusCountsWindowedDF = loansStreamingDF.withWatermark("time", "30 seconds") \
          .groupBy( \
          window(loansStreamingDF.time, "10 seconds", "5 seconds"),
          loansStreamingDF.status
      ).count().orderBy('window')

      query = (
        loanStatusCountsWindowedDF
          .writeStream
          .format("console")
          .outputMode('complete')
          .start()
      )
      query.awaitTermination()
