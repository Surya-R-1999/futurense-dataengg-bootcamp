# Filtering the Null Values:

      data = [
          ("James",None,"M"),
          ("Anna","NY","F"),
          ("Julia",None,None)
      ]

      columns = ["name","state","gender"]
      df =spark.createDataFrame(data,columns)

- Filtering the dataset whose values are null: (3 ways to filter out)

      df.filter("state is NULL").show()
      df.filter(df.state.isNull()).show()
      df.filter(col("state").isNull()).show()

- Filtering the dataset whose values are not null: (5 ways to filter out)


      df.filter("state is not NULL").show()
      df.filter("NOT state is NULL").show()
      df.filter(df.state.isNotNull()).show()
      df.filter(col("state").isNotNull()).show()
      
- Droppping the record if the value is null

      df.na.drop(subset=["state"]).show() # In this case it drops the records of state column
      
      df.na.drop().show(truncate=False) # In this case it drops the records of all columns

      df.na.drop(how="any").show(truncate=False) 
      
      df.na.drop(how="any",thresh = 2).show(truncate=False) 
      
- how parameter takes multiple arguments like "any" -> drops the whole record if any one column has null values and
                                             "all" -> drops the whole record if only all columns has null values

- By default how parameter takes "any"
- We can also pass thresh parameter - > if thresh = 1 , if 1 non value exist then it drops the record


      df.na.drop(subset=["population","type"]) \  
         .show(truncate=False)                   # Here it drops the records containing null values for multiple columns with subset keyword

      df.dropna().show(truncate=False)           # Here na is not used, still gives the same result.
      
      df.dropna(col('*'))  # Drops all the columns

- Creating a view based on the existing dataframe to perform Sql operations using spark.sql: 

      df.createOrReplaceTempView("DATA")
      spark.sql("SELECT * FROM DATA where STATE IS NULL").show()
      spark.sql("SELECT * FROM DATA where STATE IS NULL AND GENDER IS NULL").show()
      spark.sql("SELECT * FROM DATA where STATE IS NOT NULL").show()

- Populating the Null Values:

- Here null values are replaced with 0

      df.fillna(value=0).show()
      df.fillna(value=0,subset=["population"]).show()
      df.na.fill(value=0).show()
      df.na.fill(value=0,subset=["population"]).show()

- Here filling different values for null values for multiple columns using dictionary.

      df.na.fill({"city": "unknown", "type": ""}).show()
