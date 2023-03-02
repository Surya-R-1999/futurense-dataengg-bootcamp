# Cast Functions

            simpleData = [("James",34,"2006-01-01","true","M",3000.60),
            ("Michael",33,"1980-01-10","true","F",3300.80),
            ("Robert",37,"06-01-1992","false","M",5000.50)
            ]

            columns = ["firstname","age","jobStartDate","isGraduated","gender","salary"]
            df = spark.createDataFrame(data = simpleData, schema = columns)
            df.printSchema()
            df.show(truncate=False)

            from pyspark.sql.functions import col
            from pyspark.sql.types import StringType,BooleanType,DateType
            df2 = df.withColumn("age",col("age").cast(StringType())) \
                .withColumn("isGraduated",col("isGraduated").cast(BooleanType())) \
                .withColumn("jobStartDate",col("jobStartDate").cast(DateType()))
            df2.printSchema()
            
- Alternative way of Implementing cast function: (using selectExpr)
        
         df3 = df2.selectExpr("cast(age as int) age",
            "cast(isGraduated as string) isGraduated",
            "cast(jobStartDate as string) jobStartDate")
        df3.printSchema()
        df3.show(truncate=False)
        
- flatMap (Changes from multiple dimensions to single dimension) (Here we have to change the dataframe to rdd before using flatMap)
- flatMap function can't be applied directly to dataframe

        columns = ["name","languagesAtSchool","currentState"]
        data = [("James,,Smith",["Java","Scala","C++"],"CA"), \
            ("Michael,Rose,",["Spark","Java","C++"],"NJ"), \
            ("Robert,,Williams",["CSharp","VB"],"NV")]

        df = spark.createDataFrame(data=data,schema=columns)
        
        df.select(df.table_name).rdd.flatMap(lambda x : x).collect()
        df.select(df.table_name).rdd.flatMap(lambda x : x).flatMap(lambda x : x).collect() # multiple flatMaps are used in single dataframe
        
