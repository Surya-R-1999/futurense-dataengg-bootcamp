#!/usr/bin/env python
# coding: utf-8

# In[5]:


import findspark
findspark.init()


# In[6]:


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]")                     .appName('pyspark-examples')                     .getOrCreate()


# In[7]:



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


# In[8]:


df = spark.createDataFrame(data= data, schema= schema)


# In[9]:


df.show()


# In[10]:


df.printSchema()


# In[32]:


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


# In[33]:


df2 = spark.createDataFrame(data=structureData,schema=structureSchema)
df2.printSchema()


# In[26]:


df2.show()


# In[14]:


df2.filter(df2.salary > 3000).show()


# In[15]:


df2.filter(df2.name.firstname == "James").show()


# In[16]:


df2.filter(df2.name.middlename == "").show()


# In[17]:


df2.filter(df2.name.lastname.like("%s%")).show()


# In[18]:


from pyspark.sql.functions import lower

df2.filter(lower(df2.name.lastname).like("%s%")).show()


# In[19]:


from pyspark.sql.functions import concat_ws
df2.select(concat_ws(" ",df2.name.firstname,df2.name.middlename,df2.name.lastname).alias("fullName")).show()


# In[27]:


df3 = df2.withColumn("fullName",concat_ws(" ",df2.name.firstname,df2.name.middlename,df2.name.lastname)).show()


# In[29]:


df4 =df2.withColumnRenamed('name','fname').show()


# In[ ]:


from pyspark.sql.functions import *


# In[30]:


df2.drop('name').show()


# In[35]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,ArrayType,MapType
from pyspark.sql.functions import col,struct,when

spark = SparkSession.builder.master("local[1]")                     .appName('pyspark-examples')                     .getOrCreate()

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
 
df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)

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
df2.show(truncate=False)


updatedDF = df2.withColumn("OtherInfo", 
    struct(col("id").alias("identifier"),
    col("gender").alias("gender"),
    col("salary").alias("salary"),
    when(col("salary").cast(IntegerType()) < 2000,"Low")
      .when(col("salary").cast(IntegerType()) < 4000,"Medium")
      .otherwise("High").alias("Salary_Grade")
  )).drop("id","gender","salary")

updatedDF.printSchema()


# In[36]:


updatedDF.show()


# In[37]:


data = [("James","","Smith","36636","M",60000),
        ("Michael","Rose","","40288","M",70000),
        ("Robert","","Williams","42114","",400000),
        ("Maria","Anne","Jones","39192","F",500000),
        ("Jen","Mary","Brown","","F",0)]

columns = ["first_name","middle_name","last_name","dob","gender","salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)


# In[38]:


from pyspark.sql.functions import col, when
df2 = df.withColumn("new_gender", when(col("gender") == "M","Male")
                                 .when(col("gender") == "F","Female")
                                 .otherwise("Unknown"))
df2.show(truncate=False)


# In[39]:


from pyspark.sql.functions import expr
df3 = df.withColumn("new_gender", expr("case when gender = 'M' then 'Male' " + 
                       "when gender = 'F' then 'Female' " +
                       "else 'Unknown' end"))
df3.show(truncate=False)


# In[43]:


data2 = [(66, "a", "4"), (67, "a", "0"), (70, "b", "4"), (71, "d", "4")]
df5 = spark.createDataFrame(data = data2, schema = ["id", "code", "amt"])
df5.show()         


# In[48]:


df5.withColumn("new_column", when((col("code") == "a") | (col("code") == "d"), "A")
      .when((col("code") == "b") & (col("amt") == "4"), "B")
      .otherwise("A1")).show()


# In[52]:


columns = ["name","languagesAtSchool","currentState"]
data = [("James,,Smith",["Java","Scala","C++"],"CA"),       ("Michael,Rose,",["Spark","Java","C++"],"NJ"),       ("Robert,,Williams",["CSharp","VB"],"NV")]

df = spark.createDataFrame(data=data,schema=columns)
  
df.select(df.languagesAtSchool).rdd.flatMap(lambda x : x).collect()
df.select(df.languagesAtSchool).rdd.flatMap(lambda x : x).flatMap(lambda x : x).collect() # multiple flatMaps are used in single dataframe


# In[57]:


from pyspark.sql.functions import explode
df2 = df.select(df.name, explode(df.languagesAtSchool))


# In[58]:


df2.printSchema()


# In[59]:


df2.show()


# In[60]:


df.show()


# In[61]:


arrayStructureData = [
        (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
        (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
        (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
        (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
        (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
        (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
        ]
        
arrayStructureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('languages', ArrayType(StringType()), True),
         StructField('state', StringType(), True),
         StructField('gender', StringType(), True)
         ])


df = spark.createDataFrame(data = arrayStructureData, schema = arrayStructureSchema)
df.printSchema()


# In[62]:


df.show()


# In[65]:


df.select(explode(df.languages).alias("lang")).show()


# In[64]:


df.select(explode(df.languages)).filter((df.state == "OH") & (df.languages[0] == "Java")).show()


# In[66]:


df.select(col('*'), explode(df.languages).alias("lang")).show()


# In[80]:


df.select(col('*'), explode(df.languages).alias("lang")).filter((df.state == 'OH') & (array_contains(df.languages, 'Java'))).show()


# In[77]:


from pyspark.sql.functions import array_contains
df.filter((df.state == 'OH') & (array_contains(df.languages, 'Java'))).show()


# In[146]:


from pyspark.sql.types import StructType,StructField, StringType, IntegerType,ArrayType,MapType

arrayStructureData = [
        (("James","","Smith"),["Java","Scala","C++"],"OH","M",{"home":"City Center", "work": "City Tech Park"}),
        (("Anna","Rose",""),["Spark","Java","C++"],"NY","F",{"home":"Manhattan", "work": "Wall Street"}),
        (("Julia","","Williams"),["CSharp","VB"],"OH","F",{"home":"City Center", "work": "City Tech Park"}),
        (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M",{"home":"Manhattan", "work": "Wall Street"}),
        (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M",{}),
        (("Mike","Mary","Williams"),["Python","VB"],"OH","M",{})
        ]
        
arrayStructureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('languages', ArrayType(StringType()), True),
         StructField('state', StringType(), True),
         StructField('gender', StringType(), True),
    	 StructField('address',MapType(StringType(), StringType()), True)
         ])


df = spark.createDataFrame(data = arrayStructureData, schema = arrayStructureSchema)
df.printSchema()


# In[102]:


df.show()


# In[103]:


df.filter(df.address.home == "City Center").show()


# In[104]:


df.filter(df.address['home'] == "City Center").show()


# In[116]:


df.filter((df.address['work'] == "Wall Street") & (array_contains(df.languages, 'Java')) & (df.name.firstname == "Anna")).show()


# In[128]:


df.filter((df.address['work'] == "Wall Street") & (array_contains(df.languages, 'Java')) & ()).show()


# In[132]:


df.select(concat_ws(" ",df.name.firstname, df.name.middlename, df.name.lastname).alias("name"))


# In[141]:


df = df.withColumn("name", concat_ws(" ",df.name.firstname, df.name.middlename, df.name.lastname))


# In[142]:


df.show()


# In[145]:


df.filter((df.address['work'] == "Wall Street") & (array_contains(df.languages, 'Java')) & (df.name.like("%Jen%"))).show()


# In[151]:


from pyspark.sql.functions import to_csv
df.filter((df.address['work'] == "Wall Street") & (array_contains(df.languages, 'Java')) & (to_csv(df.name).like("%Ann%"))).show()


# In[152]:


df.show()


# In[160]:


simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)


# In[166]:


from pyspark.sql.functions import *


# In[167]:


df.groupby("department").agg(avg("salary").alias("avg_salary")).filter(col("avg_salary") > 80000).show()


# In[173]:


df.select(col('*')).groupby('department', 'state').sum('salary').where(col('sum(salary)') > 80000).show()


# In[174]:


df.distinct().count()


# In[175]:


df.count()


# In[177]:


df.distinct().show()


# In[184]:


df.distinct().show()


# In[189]:


df.select('salary').distinct().count()


# In[187]:


df.select(count_distinct(df.salary)).show()


# In[190]:


df.select(approx_count_distinct(df.salary)).show()


# In[197]:


df.select(collect_list('salary')).show()


# In[199]:


df.select(collect_set('salary')).show(truncate = False)


# In[200]:


df.select(mean('salary')).show()


# In[204]:


df.select(percentile_approx('salary',0.5)).show()


# In[207]:


df.orderBy('salary').show()


# In[260]:


a = df.groupby().avg('salary').rdd.collect()[0][0]
df.select('salary', a).show()
type(lit(a).cast(IntegerType()))


# In[ ]:


df.select(col('*')).groupby('department', 'state').sum('salary').where(col('sum(salary)') > 80000).show()

