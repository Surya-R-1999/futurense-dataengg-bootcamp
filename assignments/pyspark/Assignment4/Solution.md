# Assignment 4

- Enhance above Bank Marketing Campaign Data Analysis PySpark Application to perform the data processing
- as pipeline of four different stages viz. Loading, Validation, Transformation and Expot each as separate PySpark applicaiton.

- Create PySpark Application - bank-marketing-data-loading.py. Perform below operations.

 - 	a) Load Bank Marketing Campaign Data csv file from local to HDFS file system under '/user/training/bankmarketing/raw'
 - 	b) Load Bank Marketing Campaign Data from HDFS file system under '/user/training/bankmarketing/raw' and create DataFrame
 - 	c) Convert the data into parquet format and write into HDFS file system under '/user/training/bankmarketing/staging'
 - 	d) Data should be moved to '/user/training/bankmarketing/raw/yyyymmdd/success' once the data loading job completed successfully
 - 	f) Data should be moved to '/user/training/bankmarketing/raw/yyyymmdd/error' once the data loading job is failed due to data error
  
          import subprocess
          import pyspark
          from pyspark.sql import SparkSession
          from pyspark.sql.functions import *

          spark = SparkSession.builder.master("local[1]") \
                              .appName('pyspark-examples') \
                              .getOrCreate()

          subprocess.run(["echo","Loading data from local system to HDFS"])

          subprocess.run(["hadoop", "fs", "-put",  "/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Assignment4/bankmarketdata.csv", "hdfs://localhost:9000/user/training/bankmarketing/raw"])

          subprocess.run(["echo","Data Loaded Successfully to HDFS"])

          df = spark.read.load("hdfs://localhost:9000/user/training/bankmarketing/raw/bankmarketdata.csv",format = "csv", sep = ";", delimiter=';',header=True,inferSchema=True)

          df.write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/user/training/bankmarketing/staging')

          try:
                subprocess.run(["hadoop", "fs", "-put" ,"/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Assignment4/bankmarketdata.csv", "hdfs://localhost:9000/user/training/bankmarketing/raw/yyyymmdd/success"])
          except:
                subprocess.run(["hadoop", "fs", "-put" ,"/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Assignment4/bankmarketdata.csv" ,"hdfs://localhost:9000/user/training/bankmarketing/raw/yyyymmdd/failure"])


- 	Create PySpark Application - bank-marketing-validation.py. Perform below operations.
- 	a) Load Bank Marketing Campaign Data in Parquet format from HDFS file system under '/user/training/bankmarketing/staging'
- 	b) Remove all 'unknown' job records 
- 	c) Replace 'unknown' contact nos with 1234567890 and 'unknown' poutcome with 'na'
- 	d) Write the output as Parquet format into HDFS file system under '/user/training/bankmarketing/validated'
- 	e) Data should be moved to '/user/training/bankmarketing/staging/yyyymmdd/success' once the validation job completed successfully
- 	f) Data should be moved to '/user/training/bankmarketing/staging/yyyymmdd/error' once the validation job is failed due to data error


            import subprocess
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
 
- Create PySpark Application - bank-marketing-tranformation.py. Perform below operations.
- 	a) Load validated Bank Marketing Campaign Data in Parquet format from HDFS file system under '/user/training/bankmarketing/validated'
- 	b) Get AgeGroup wise SubscriptionCount
- 	c) Filter AgeGroup with SubcriptionCount > 2000 
-	  d) Write the output as Avro format into HDFS file system under '/user/training/bankmarketing/processed'
- 	e) Data should be moved to '/user/training/bankmarketing/validated/yyyymmdd/success' once the trasnfomation job completed successfully
- 	f) Data should be moved to '/user/training/bankmarketing/validated/yyyymmdd/error' once the transformation job is failed



