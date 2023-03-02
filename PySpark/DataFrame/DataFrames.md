# Spark DataFrames : (RDD with schema Information)

- Different ways to create a Dataframe, 
   - Loading a data from database
   - rdd (rdd.toDF())
            
            rdd = sc.parallelize([(1,2,3),(4,5,6),(7,8,9)])
            rdd.toDF().show()
   
   - Using CreateDataFrame with RDD -> spark.createDataFrame(rdd)
   - Using CreateDataFrame with list of Rows -> spark.createDataFrame([Row(),Row()])
   - Using CreateDataFrame with list of tupples -> spark.createDataFrame([(),()])
   - Using Pandas DataFrame -> spark.createDataFrame(pandas DF)
   - Using CreateDataFrame with explicit schema -> spark.createDataFrame(rdd, schema = ' col_name datatype') 
   
            data = [(100,"DEBIT",1000.0,"IND"),(101,"CREDIT",2000.0,"IND"),(102,"DEBIT",3000.0,"AUS"),
                            (103,"CREDIT",4000.0,"JPN"),(104,"DEBIT",5000.0,"IND"),(105,"CREDIT",6000.0,"AUS")]
            columns = 'id int, type string, amt float, code string'
            df = spark.createDataFrame(data = data, schema = columns)
   
   - Dictionary

- Print Schema

            df.printSchema()

- List values as a table:

            df.show()
- To Select the columns:

            df.select(df.col, df['col'], "col")
- Functions:

   - String Functions
        
            from pyspark.sql.functions import lower, upper 
            df.select(df.amt, lower(df.type)).show()
   
    - concat function:
           
            from pyspark.sql.functions import concat_ws
            df.withColumn("desc", concat_ws(" ", "type", "amt")).show()
            
    - WildCard Matching:
            
            df = spark.createDataFrame([
             (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
             (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
             (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
            ], schema='a long, b double, c string, d date, e timestamp')

            df.filter((df.a >= 1) & (df.b <= 5.0) & (df.c.like('%2'))).show()
            
     - Find the amt transactioned above 1000.0 and the type is CREDIT.

            df.filter((df.type.like("%CREDIT%") & (df.amt > 1000.0))).show()
            
     - Find the amt transactioned above 1000.0 in last 1 week and the type is CREDIT.
 
            from pyspark.sql.functions import datediff
            from pyspark.sql.functions import current_timestamp
            data = spark.createDataFrame([
                (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
                (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
                (3, 4., 'string3', date(2000, 3, 1), datetime(2023, 2, 27, 12, 0))
            ], schema='a long, b double, c string, d date, e timestamp')

            data.filter(datediff(current_timestamp(), data.e) < 7).show()

   - Math Functions
   - Date Functions
   - Window Functions

- Columns:
   
   - Add Column 
            
            from pyspark.sql.functions import current_timestamp
            # Syntax:
            df.withColumn('colName', fun())
            # Example
            df_with_ts = df.withColumn("curr_timestamp", current_timestamp())
     
     
- Filtering Rows:

            df.filter(df.col_name > 1) # single condition
            df.filter(df.col1 > 5 and df.col2 < 20) # multiple conditions
            df.filter((df.col1 > 5) & (df.col2 < 20)).show()
            
- Grouping of data:

            # Syntax
            df.groupby('col_name').aggregate_function().show() # If the column is not passed in groupby clause then it takes the total sum
            # Example
            df.groupby('type').sum('amt').show() # if amt is not passed all the integer columns are displayed
            df.groupby('type').min('amt').show()
            df.groupby('type').max('amt').show()
            df.groupby('type').avg('amt').show()
            
- We can perform all transformations and actions in Dataframe using .rdd

- Converting DataFrame to RDD:

         df.rdd.collect()
         
- Reading Json file:

      -  spark
         df = spark.read.json("/mnt/c/Users/miles/Documents/futurense_hadoop-pyspark/labs/dataset/people/people.json")
         df.printSchema()
         df.show() # acts as head in dataframe
         df.rdd # df is converted to RDD's but not assigned to any variable or RDD
         df.rdd.getNumPartitions() # we can access all RDD functions by converting dataframes to RDD explicitly by assigning .rdd.rdd_function_name
         df.count() # gives the number of records

- Reading CSV file:

      -  spark
         df = spark.read.csv("/mnt/c/Users/miles/Documents/futurense_hadoop-pyspark/labs/dataset/people/people.csv")
         df.printSchema()
         df.show() # acts as head in dataframe
         df.rdd # df is converted to RDD's but not assigned to any variable or RDD
         df.rdd.getNumPartitions() # we can access all RDD functions by converting dataframes to RDD explicitly by assigning .rdd.rdd_function_name
         df.count() # gives the number of records


