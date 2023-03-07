# Assignment 4

- Enhance above Bank Marketing Campaign Data Analysis PySpark Application to perform the data processing
- as pipeline of four different stages viz. Loading, Validation, Transformation and Expot each as separate PySpark applicaiton.

- Create PySpark Application - bank-marketing-data-loading.py. Perform below operations.
  
  - a.
        
          import pyspark
          from pyspark.sql import SparkSession
          from pyspark.sql.functions import *

          spark = SparkSession.builder.master("local[1]") \
                              .appName('pyspark-examples') \
                              .getOrCreate()
                              
          hadoop fs -put bankmarketdata.csv hdfs://localhost:9000/user/training/bankmarketing/raw
          
