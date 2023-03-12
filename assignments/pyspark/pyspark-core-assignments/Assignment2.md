# Assignment #2
- 	Bank Marketing Campaign Data Analysis with RDD API
- 	a) Load Bank Marketing Dataset and create RDD		
- 	b) Give marketing success rate. (No. of people subscribed / total no. of entries)
- 	c) Give marketing failure rate
- 	d) Maximum, Mean, and Minimum age of the average targeted customer
- 	e) Check the quality of customers by checking the average balance, median balance of customers
- 	f) Check if age matters in marketing subscription for deposit
- 	g) Show AgeGroup [Teenagers, Youngsters, MiddleAgers, Seniors] wise Subscription Count.
- 	h) Check if marital status mattered for subscription to deposit.
- 	i) Check if age and marital status together mattered for subscription to deposit scheme



        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("BankMarket_Analysis").getOrCreate()
        
        rdd1 = spark.sparkContext.textFile("/mnt/c/Users/miles/Documents/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv")
        
        # Always while performing an action the return Type won't be another RDD, vice-versa for transformations.
        # Removing the Header to perform RDD Operations.
        
        rdd2= rdd1.collect() # The return type is a list of strings.
        
        # Creating an RDD from a collection
        rdd3 = spark.sparkContext.parallelize(rdd2[1:])
        
        # Split the data Based on seperator. (Now whole record is considered as 1 string) 
        rdd4 = rdd3.map(lambda row : row.split(';'))
        
        # Now the string has been splitted to 17 Individual Values i.e.. 17 Columns.
        # Success Rate
        success_count = rdd4.filter(lambda column : 'yes' in column[16]).count()
        total_count = rdd4.count()
        print("success_rate : " + str((success_count / total_count) * 100))
        
        # Failure Rate (1 - success_rate is not possible due to null records present in column)
        failure_count = rdd4.filter(lambda column : 'no' in column[16]).count()
        total_count = rdd4.count()
        print("failure_rate : " + str((failure_count / total_count) * 100))
        
        # Max, Min, Avg Age of Customers.
        max_age = rdd4.map(lambda x : int(x[0])).max()
        min_age = rdd4.map(lambda x : int(x[0])).min()
        avg_age = rdd4.map(lambda x : int(x[0])).mean()
        
        #
