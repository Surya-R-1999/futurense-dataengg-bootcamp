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
-	 d) Write the output as Avro format into HDFS file system under '/user/training/bankmarketing/processed'
- 	e) Data should be moved to '/user/training/bankmarketing/validated/yyyymmdd/success' once the trasnfomation job completed successfully
- 	f) Data should be moved to '/user/training/bankmarketing/validated/yyyymmdd/error' once the transformation job is failed
 
- Note: Start Spark Shell with Avro dependency
-       pyspark --packages org.apache.spark:spark-avro_2.12:3.3.2
 
      import subprocess
      import pyspark
      from pyspark.sql import SparkSession
      from pyspark.sql.functions import *

      spark = SparkSession.builder.master("local[1]") \
                           .appName('pyspark-examples') \
                           .getOrCreate()
 
      df = spark.read.load("hdfs://localhost:9000/user/training/bankmarketing/validated", format = "parquet")
      df.createOrReplaceTempView("Banking_Filter")
      spark.sql("SELECT  from Banking_Filter")
       
      age_group_count = spark.sql("SELECT age, count(y) as count from Banking_Filter group by age")
       
      age_group_count_gt_2000 = spark.sql("SELECT age, count(y) from Banking_Filter group by age having count(y) > 2000").show()
       
      age_group_count.write.mode('overwrite').format('avro').save('hdfs://localhost:9000/user/training/bankmarketing/processed')
       
      try:
       age_group_count.write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/user/training/bankmarketing/processed/yyyymmdd/success')
      except:
       age_group_count.write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/user/training/bankmarketing/processed/yyyymmdd/failure')
 
 
- Create PySpark Application - bank-marketing-export.py. Perform below operations.
- a) Load processed Bank Marketing Campaign Data in Avro format from HDFS file system under '/user/training/bankmarketing/processed'
- b) Export the data into RDBMS (MySQL DB) under bankmaketing schema and subcription_count table
- c) Data should be moved to '/user/training/bankmarketing/processed/yyyymmdd/success' once the export job completed successfully
- d) Data should be moved to '/user/training/bankmarketing/processed/yyyymmdd/error' once the export job is failed
- spark-submit --jars "/home/miles/mysql-connector-j-8.0.32/mysql-connector-j-8.0.32.jar" --packages org.apache.spark:spark-avro_2.12:3.3.2 bank-marketing-export.py
     
      import subprocess
      import pyspark
      from pyspark.sql import SparkSession
      from pyspark.sql.functions import *

      spark = SparkSession.builder.master("local[1]") \
                           .appName('pyspark-examples') \
                           .getOrCreate()
                           
      df = spark.read.load("hdfs://localhost:9000/user/training/bankmarketing/processed", format = "avro")
      
      df.write \
      .format("jdbc") \
      .option("url", "jdbc:mysql://localhost:3306/bankmarketing") \
      .option("driver", "com.mysql.jdbc.Driver") \
      .option("dbtable", "subscription_count") \
      .option("user", "pyspark") \
      .option("password", "pyspark") \
      .mode("overwrite") \
      .save()
      
      try:
       df.write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/user/training/bankmarketing/processed/yyyymmdd/success')
      except:
       df.write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/user/training/bankmarketing/processed/yyyymmdd/failure')
 
- Create Shell Script - bank-marketing-workflow.sh. Performa below operations.
- a) Create a workflow to sequentially execute Data Loading, Validation, Tranformation and Export jobs
- b) Schedule to run this workflow every N mins e.g. 15 mins and process the new input dataset if any

       echo "*** Started Execution *****"

      echo "bank-marketing-data-loading.py"
      spark-submit  "/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Assignment4/bank-marketing-data-loading.py"

      if [ $? -eq 0 ]
      then
          echo "executing bank-marketing-validation.py"
          spark-submit "/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Assignment4/bank-marketing-validation.py"

          if [ $? -eq 0 ]
          then
              echo "executing bank-marketing-tranformation.py"
              spark-submit  --packages org.apache.spark:spark-avro_2.12:3.3.2 "/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Assignment4/bank-marketing-tranformation.py"

              if [ $? -eq 0 ]
              then
                  echo "executing bank-marketing-export.py"
                  spark-submit --jars "/home/miles/mysql-connector-j-8.0.32/mysql-connector-j-8.0.32.jar"  --packages org.apache.spark:spark-avro_2.12:3.3.2 "/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Assignment4/bank-marketing-export.py"

                  if [ $? -eq 0 ]
                  then
                      echo  "All Jobs done"
                  else
                      echo "============== ERROR in bank_export.py  ==================="
                  fi
              else
                  echo "============== ERROR in bank_transformation.py  ==================="
              fi
          else
              echo "================ ERROR in bank_cleaning.py  ===================="
          fi
      else
          echo "================ ERROR in FILE bank_load.py =================="
      fi
      echo "Good Work :)"
