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
        print(max_age, min_age, avg_age)
        
        # Avg and Median Balance of Customers.
        avg_balance = rdd4.map(lambda x : int(x[5])).mean()
        # Median Balance
        if rdd4.count() % 2 != 0:
                index = (rdd4.count() + 1) // 2
                print("Median Age : " + str(rdd4.map(lambda x : x[5])sortBy(lambda x: x, False).collect()[index]))
        else:
                index = (((rdd4.count() + 1) // 2) + (rdd4.count() // 2)) // 2  
                median_Age = rdd4.map(lambda x : x[5]).sortBy(lambda x : x, False).collect()[index]
                print("Median Age : " + str(rdd4.map(lambda x : x[5]).collect()[index]))
        
        
        #Check if age matters in marketing subscription for deposit
 	
        #Show AgeGroup [Teenagers, Youngsters, MiddleAgers, Seniors] wise Subscription Count.
        
        def AgeGroup(age):
             if age <= 20:
		group = "Teenagers"
	     elif age > 20 and age <= 40:
		group = "Youngsters"
	     elif age > 40 and age <= 60:
		group = "Middleagers"
	     else:
		group = "Seniors"
	     return group
        
         rdd5 = rdd4.filter(lambda x : 'yes' in x[16]).map(lambda x : (AgeGroup(int(x[0])), 1))
         rdd5.reduceByKey(lambda x,y : x+y).collect()

        # Check if marital status mattered for subscription to deposit.
        
        rdd6 = rdd4.filter(lambda x : 'yes' in x[16]).map(lambda x : (x[2], 1))
        rdd6.reduceByKey(lambda a,b : a+b).collect()
        
        # Check if age and marital status together mattered for subscription to deposit scheme
        
        rdd7 = rdd4.filter(lambda x : 'yes' in x[16]).map(lambda x : ((AgeGroup(int(x[0])),x[2]),1))
        rdd7.reduceByKey(lambda a,b : a+b).collect()
        
        
        
        
        
        
        
