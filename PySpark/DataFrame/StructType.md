# SparkSQL

- Dataset is not available in pyspark. 

- Type Safety -> It's a feature available in Dataset. (Allows homogenous datatype only) 

- Create a SparkSession to perform Spark Operations

        spark = SparkSession.builder.master("local[1]") \
                  .appName('pyspark-examples') \
                  .getOrCreate()

- Struct Type: It is used if we are using complex datatypes

- Before Using these datatypes, Import it from pyspark.sql.types

        import pyspark
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType,StructField, StringType, IntegerType,ArrayType,MapType

        data = [("James","","Smith","36636","M",3000),
          ("Michael","Rose","","40288","M",4000),
          ("Robert","","Williams","42114","M",4000),
          ("Maria","Anne","Jones","39192","F",4000),
          ("Jen","Mary","Brown","","F",-1)
        ]

      schema = StructType([ 
          StructField("firstname",StringType(),True), 
          StructField("middlename",StringType(),True), 
          StructField("lastname",StringType(),True), 
          StructField("id", StringType(), True), 
          StructField("gender", StringType(), True), 
          StructField("salary", IntegerType(), True) 
        ])

To Create a Spark DataFrame based on the above data,

        df = spark.createDataFrame(data= data, schema= schema)
        df.show()

Nested StructType:

         structureData = [
            (("James","","Smith"),"36636","M",3100),
            (("Michael","Rose",""),"40288","M",4300),
            (("Robert","","Williams"),"42114","M",1400),
            (("Maria","Anne","Jones"),"39192","F",5500),
            (("Jen","Mary","Brown"),"","F",-1)
                  ]
         structureSchema = StructType([
                StructField('name', StructType([
                     StructField('firstname', StringType(), True),
                     StructField('middlename', StringType(), True),
                     StructField('lastname', StringType(), True)
                     ])),
                 StructField('id', StringType(), True),
                 StructField('gender', StringType(), True),
                 StructField('salary', IntegerType(), True)
                 ])
                 
          df2 = spark.createDataFrame(data=structureData,schema=structureSchema)
          df2.printSchema()
- Filtering Command on Struct Type:

                df2.filter(df2.name.firstname == "James").show()
                
                df2.filter(df2.name.middlename == "").show() # middle name is empty
                
                df2.filter(df2.name.lastname.like("%s%")).show() # last name contains 's'
                
                from pyspark.sql.functions import lower   # to filter out the last name contains  s with case insensitive

                df2.filter(lower(df2.name.lastname).like("%s%")).show()

                from pyspark.sql.functions import concat_ws # concatenating firstname, middlename and fullname and using alias

                df2.select(concat_ws(" ",df2.name.firstname,df2.name.middlename,df2.name.lastname).alias("fullName")).show()

                # Adding an additional Column with the existing dataframe
                
                df2.withColumn("fullName",concat_ws(" ",df2.name.firstname,df2.name.middlename,df2.name.lastname)).show() 
                
                # Renaming a column in the existing dataframe
                
                df2.withColumnRenamed('fullName','fname').show()
                
                df2.drop('colName').show()

- Performing SQL Operations in a Dataframe using col() object, 

        updatedDF = df2.withColumn("OtherInfo", 
                    struct(col("id").alias("identifier"),
                    col("gender").alias("gender"),
                    col("salary").alias("salary"),
                    when(col("salary").cast(IntegerType()) < 2000,"Low")
                   .when(col("salary").cast(IntegerType()) < 4000,"Medium")
                   .otherwise("High").alias("Salary_Grade")
                  )).drop("id","gender","salary")

- In the above code, multiple operations such as alias , cast or when clause and otherwise clause is used in col("colName") function. Which all the columns comes inside Struct type.
- col is the function which will convert column name from string type to Column type. We can also refer column names as Column type using Data Frame name. 

- Structure type can also contain array and map types datas,

               arrayStructureSchema = StructType([
               StructField('name', StructType([
               StructField('firstname', StringType(), True),
               StructField('middlename', StringType(), True),
               StructField('lastname', StringType(), True)
               ])),
               StructField('hobbies', ArrayType(StringType()), True),
               StructField('properties', MapType(StringType(),StringType()), True)
            ])
- Difference between (when and otherwise) or (case when then else) using expr:

                from pyspark.sql.functions import col, when
                df2 = df.withColumn("new_gender", when(col("gender") == "M","Male")
                                                 .when(col("gender") == "F","Female")
                                                 .otherwise("Unknown"))
                df2.show(truncate=False)

- Here we have to import expr to implement case when then else statement. Also for every cases are added by '+'  and ends with end keyword: 
- For case statements we have to import expr
               
                from pyspark.sql.functions import expr
                df3 = df.withColumn("new_gender", expr("case when gender = 'M' then 'Male' " + 
                                       "when gender = 'F' then 'Female' " +
                                       "else 'Unknown' end"))
                df3.show(truncate=False)

- Example:

                data2 = [(66, "a", "4"), (67, "a", "0"), (70, "b", "4"), (71, "d", "4")]
                df5 = spark.createDataFrame(data = data2, schema = ["id", "code", "amt"])
                df5.show()  
                
                # Use paranthesis for every conditions
                
                df5.withColumn("new_column", when((col("code") == "a") | (col("code") == "d"), "A")
                .when((col("code") == "b") & (col("amt") == "4"), "B")
                .otherwise("A1")).show()

- Use case of lit function:

                
                spark = SparkSession.builder.appName('pyspark-examples').getOrCreate()
                data = [("111",50000),("222",60000),("333",40000)]
                columns= ["EmpId","Salary"]
                df = spark.createDataFrame(data = data, schema = columns)
                df.printSchema()
                df.show(truncate=False)

- we have to import lit (literal) referes to constant values. In this case lit_value1 will have 1 as value for all records.
                
                from pyspark.sql.functions import col,lit
                df2 = df.select(col("EmpId"),col("Salary"),lit("1").alias("lit_value1"))
                df2.show(truncate=False)

- lit_value2 contains 100 or 200 based on the given conditions using when otherwise or case statements (case when then + else end)

                from pyspark.sql.functions import when
                df3 = df2.withColumn("lit_value2", when(col("Salary") >=40000 & col("Salary") <= 50000,lit("100")).otherwise(lit("200")))
                df3.show(truncate=False)

- literals can be integers, strings and also special characters.
