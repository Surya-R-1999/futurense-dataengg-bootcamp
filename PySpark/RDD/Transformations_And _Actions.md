# Transformations and Actions:

- >>> sc
  <SparkContext master=spark://MILE-BL-4824-LAP.localdomain:7077 appName=PySparkShell>
  
- >>> rdd1 = sc.parallelize([1,2,3,4,5])
- >>> rdd1.getNumPartitions()
Result : 8
- >>> rdd1 = sc.parallelize([1,2,3,4,5],4)
- >>> rdd1.getNumPartitions()
Result : 4
- >>> out = rdd1.count()
- >>> rdd1.collect()
Result : [1, 2, 3, 4, 5]
- >>> out
Result : 5
  
- Multiple actions can be performed on a same RDD.
- Multiple transformations can be performed on same RDD
  
- >>> type(rdd1)
<class 'pyspark.rdd.RDD'>
- >>> type(out)
<class 'int'>
- >>> rdd2 = rdd1.repartition(5)
- >>> rdd1.getNumPartitions()
Output : 4
- >>> rdd2.getNumPartitions()
Output : 5

# Spark Documentation 
- https://spark.apache.org/docs/latest/rdd-programming-guide.html 

# PySpark Documentation 

- https://spark.apache.org/docs/latest/api/python/index.html

# Squaring the elements in a list:

- >>> num = [1,2,4,8,16]
- >>> out = num.map(lambda x : x**2)
- >>> out = sc.parallelize(num).map(lambda x : x**2)
- >>> out.collect()
- Output : [1, 4, 16, 64, 256]
 
  
# FlatMap:

- >>> data = ["Project's Guttenberg's", "Alice's Adventures in Wonderland", "Project's Guttenberg's"]
- >>> rdd2 = sc.parallelize(data)
- >>> rdd2.collect()
- Output : ["Project's Guttenberg's", "Alice's Adventures in Wonderland", "Project's Guttenberg's"]
- >>> rdd3  = rdd2.map(lambda x : x.split())
- >>> rdd3.collect()
- Output : [["Project's", "Guttenberg's"], ["Alice's", 'Adventures', 'in', 'Wonderland'], ["Project's", "Guttenberg's"]]
- >>> rdd4  = rdd2.flatMap(lambda x : x.split())
- >>> rdd4.collect()
- Output: ["Project's", "Guttenberg's", "Alice's", 'Adventures', 'in', 'Wonderland', "Project's", "Guttenberg's"]

# Example of flatMap (Converting 2D to 1D):

- data = [[1,2,3],[4,5,6],[7,8,9]]  
- >>> rdd1 = sc.parallelize(data)
- >>> rdd1.collect()
- Output : [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
- >>> rdd2 = rdd1.flatMap(lambda x : x)
- >>> rdd2.collect()
- Output : [1, 2, 3, 4, 5, 6, 7, 8, 9]
  
 # Filter Transformation:
  
- >>> rdd2.collect()
- Output : [1, 2, 3, 4, 5, 6, 7, 8, 9]
- >>> rdd3 = rdd2.filter(lambda x : x in [7,8,9])
- >>> rdd3.collect()
- Output : [7, 8, 9]
- >>> rdd4 = rdd2.filter(lambda x : x > 3 and x < 7)
- >>> rdd4.collect()
- Output : [4, 5, 6]

# Filtering Odd Numbers and Squaring the odd numbers
- >>> rdd5 = rdd2.filter(lambda x : x % 2 != 0)
- >>> rdd5 = rdd5.map(lambda x : x ** 2)
- >>> rdd5.collect()
- Output : [1, 9, 25, 49, 81]

# Filtering Odd Numbers and Squaring the odd numbers from 2D list 
  
- >>> data
- [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
- >>> sc.parallelize(data).flatMap(lambda x : x).filter(lambda x : x % 2 != 0).map(lambda x : x ** 2)
- Output : PythonRDD[28] at RDD at PythonRDD.scala:53
- >>> sc.parallelize(data).flatMap(lambda x : x).filter(lambda x : x % 2 != 0).map(lambda x : x ** 2).collect()
- Output : [1, 9, 25, 49, 81]
  
 # Transactional file with Debit above 3000
  
- >>> transaction_file = [[100,'Debit',2000.0],[101,'Credit',3000.0],[102,'Debit',4000.0],[103,'Credit',5000.0]]
- >>> rdd1 = sc.parallelize(transaction_file)
- >>> rdd2 = rdd1.filter(lambda x : 'Debit' in x)
- >>> rdd3 = rdd2.filter(lambda x : x[2] > 3000)
- >>> rdd3.collect()
- Output : [[102, 'Debit', 4000.0]]
  
 # Transactional file with Debit above 3000 from a file 
  
- >>> tran_file = sc.textFile("/mnt/c/Users/miles/Documents/pyspark/Pyspark/transaction.txt")
- >>> rdd1 = tran_file.map(lambda x : x.split(','))
- >>> rdd2 = rdd1.filter(lambda x : 'DEBIT' in x[1] and  float(x[2]) > 3000)
- >>> rdd2.collect()
- Output : [['102', 'DEBIT', '4000.0'], ['104', 'DEBIT', '6000.0']]
  
# Filtering Transcation ID and Amount from the previous problem's output 
 
- >>> rdd3 = rdd2.map(lambda x : (x[0] , x[2]))
- >>> rdd3.collect()
- Output : [('102', '4000.0'), ('104', '6000.0')]
  
# Export the output to a file:

 - >>> rdd3.saveAsTextFile("/mnt/c/Users/miles/Documents/pyspark/Pyspark/output")
 - Creates a 2 files since 2 partitions are created as output.
 - Output Directory:
 - miles@MILE-BL-4824-LAP:/mnt/c/Users/miles/Documents/pyspark/Pyspark/output$ ls
 - Output : _SUCCESS  part-00000  part-00001

 # Importing Multiple files and save the content to a RDD:
  
- >>> input = sc.textFile("/mnt/c/Users/miles/Documents/pyspark/Pyspark/output/part*")
- >>> input.collect()
- Output : ["('102', '4000.0')", "('104', '6000.0')"]
 
# Exporting the output by changing the partitions :
 
 - Here the input RDD contains 2 partitions.
  
 - input.repartition(1).saveasTextFile("/mnt/c/Users/miles/Documents/pyspark/Pyspark/new_output/")
  
# mapPartitions : Similar to map, but runs on each partitiosn

  - >>> rdd = sc.parallelize([1,2,3,4],2)
  - >>> rdd.getNumPartitions()
  - Output : 2
  - >>> rdd.collect()
  - Output : [1, 2, 3, 4]
  - >>> def f(iterator) : yield sum(iterator)
  - ...
  - >>> rdd.mapPartitions(f).collect()
  - Output : [3, 7]
  - >>> rdd = sc.parallelize([1,2,3,4],3)
  - >>> def f(iterator) : yield sum(iterator)
  - ...
  - >>> rdd.mapPartitions(f).collect()
  - Output : [1, 2, 7]
  - >>> rdd = sc.parallelize([1,2,3,4],4)
  - >>> def f(iterator) : yield sum(iterator)
  - ...
  - >>> rdd.mapPartitions(f).collect()
  - Output : [1, 2, 3, 4]
  - In transformation, Narrow Partitions vs Wide Partitions : If shuffle takes place in partitioning then it is narrow partition  else wide partition.

  - In Repartition, shuffling takes place therefore it's a Wide Partition.

  - If Repartitioning from higher to lower, repartition is not advisable use coalesce.

  - The only difference between coalesce and repartition , performance in reducing the partitions. In coalesce, the shuffling won't happen.

  - >>> rdd = sc.parallelize(range(100),4)

  - >>> rdd.getNumPartitions()

  - Output : 4 

  - >>> rdd.repartition(2).getNumPartitions()

  - Output : 2

  - >>> rdd.repartition(5).getNumPartitions()

  - Output : 5

  - >>> rdd.coalesce(5).getNumPartitions()

  - Output : 4

  # Union:
  
- Merging 2 RDDS, and the duplicates won't be removed.
- >>> rdd1 = sc.parallelize([1,2,3,4,5])
- >>> rdd2 = sc.parallelize([5,6,7,8,9])
- >>> rdd3 = rdd1.union(rdd2)
- >>> rdd3.collect()
- Output : [1, 2, 3, 4, 5, 5, 6, 7, 8, 9]
  
# Distinct (Wide partitioning since the result is shuffled)

- Merging 2 RDDS, and the duplicates will be removed.
- >>> rdd1 = sc.parallelize([1,2,3,4,5])
- >>> rdd2 = sc.parallelize([5,6,7,8,9])
- >>> rdd3 = rdd1.union(rdd2)
- >>> rdd3.distinct().collect()
- Output : [1, 2, 3, 4, 5, 6, 7, 8, 9]
 
# Using union between two different partitioned RDDS:

- >>> rdd1 = sc.parallelize([1,2,3,4,5],4)
- >>> rdd2 = sc.parallelize([5,6,7,8,9],2)
- >>> rdd3 = rdd1.union(rdd2)
- >>> rdd3.getNumPartitions()
- Output : 6
- >>> rdd3.distinct().getNumPartitions()
- Output : 6
- >>> rdd3.distinct(4).getNumPartitions()
- Output : 4
 
 # Intersection :
  
- >>> rdd3 = rdd1.intersection(rdd2)
- >>> rdd3.collect()
- Output : [5]
- >>> rdd3.getNumPartitions()
- Output : 6

 # Joins: (default : Inner join based on keys)
 
- >>> x = sc.parallelize([("a",1),("b",4)])
- >>> y = sc.parallelize([("a",2),("a",3)])
- >>> z = x.join(y)
- >>> z.collect()
- Output : [('a', (1, 2)), ('a', (1, 3))]
- >>> z = x.join(y,4)
- >>> z.getNumPartitions()
- output : 4

# Cogroups and Joins require key value pair, it can't apply the function in a linear.  
  
# Cogroup:

- Grouping locations based on Keys
- >>> x = sc.parallelize([("a",1),("b",4)],4)
- >>> y = sc.parallelize([("a",2),("a",3)],2)
- >>> z = x.cogroup(y)
- >>> z.collect()
- Output : [('b', (<pyspark.resultiterable.ResultIterable object at 0x7f77b4bf6bc0>, <pyspark.resultiterable.ResultIterable object at 0x7f77b4a3cdf0>)), ('a', (<pyspark.resultiterable.ResultIterable object at 0x7f77b4a84b20>, <pyspark.resultiterable.ResultIterable object at 0x7f77b4a84b80>))]

  - >>> type(z)
- Output : <class 'pyspark.rdd.PipelinedRDD'>
- >>> x = sc.parallelize([("a",1),("b",4)])
- >>> y = sc.parallelize([("a",2),("b",3)])
- >>> [(x, tuple(map(list,y))) for x,y in sorted(list(x.cogroup(y).collect()))]
- Output : [('a', ([1], [2])), ('b', ([4], [3]))]
- >>> y = sc.parallelize([("a",2),("a",1),("b",3)])
- >>> [(x, tuple(map(list,y))) for x,y in sorted(list(x.cogroup(y).collect()))]
- Output : [('a', ([1], [2, 1])), ('b', ([4], [3]))]
  
# Cartesian Product : 
  
- 
# Wide transformations:
  
# groupByKey (Doesn't perform well compared to reduceByKey and aggregateByKey)
  
- >>> rdd = sc.parallelize([("a",1),("b",1),("a",1)])
- >>> rdd.groupByKey().collect()
- Output : [('a', <pyspark.resultiterable.ResultIterable object at 0x7f77b4a3e530>), ('b', <pyspark.resultiterable.ResultIterable object at 0x7f77b4a84910>)]
- >>> rdd.groupByKey().mapValues(len).collect()
- Output : [('a', 2), ('b', 1)]
- >>> rdd.groupByKey().mapValues(list).collect()
- Output : [('a', [1, 1]), ('b', [1])]
  
# reduceByKey

-  >>> rdd.collect()
- Output : [('a', 1), ('b', 1), ('a', 1)]
- >>> rdd.reduceByKey(lambda a,b : a + b ).collect()
- Output : [('a', 2), ('b', 1)]
  
# aggregateByKey 
- (Difference between reduceByKey and aggregateByKey, In aggregateByKey, the aggregate logic can be independent among partitions level and across partition level or resultant level)

- rdd.reduceByKey(0, lambda a,b : a + b, lambda a,b : a + b + 1 ).collect()  
- first argument -> partition level
- second argument -> across partition level
  
# sortByKey
  
# Actions:
  
- count()
- collect()
- reduce(function) => 1 arg => reduce(lambda a,b : a+b)
- sum()
- forEach() -> def f(x) : print(x) => sc.parallelize([1,2,3,4,5]).foreach(f) => iterate each element to the function.
- saveasTextFile('path')
- first()
- min()
- max()
- mean()
- aggregate() => 3 args => aggregate(0, lambda a,b : a+b, lambda a,b : a+b+1) -> initial value of a , aggregate fn at partitional level , aggregate fn across partition level
  - eg : rdd.aggregate(0, lambda a,b : a+b, lambda a,b : a+b+1) -> 47 (No Partitions so 3rd argument is not used)
  - eg : rdd.repartitions(5).aggregate(0, lambda a,b : a+b, lambda a,b : a+b+1) -> 50
- take(n) -> first n elements
- sample() => 2 args
- forEachPartition -> similar to mapPartition and functions as forEach
- takeSample
- countByKey => returns a dictionary => rdd.countByKey().items()
