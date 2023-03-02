# Apache Spark Installation

- Using Spark installation only in Linux or windows in any one or multiple node is called Standalone Mode. Without using Hadoop Architecture or any other Processing layers like meesos or yarn etc....

- A Cluster can be single or multiple Nodes.

- Java Environment is mandatory to work in Spark, So First we have to install Java Development Kit before installing Spark.

- bashrc -> Hidden file, which contains all environmental variables. Set Java Home and Path. And to execute the changes we have to provide source .bashrc cmd.

- So we have to specify java home in two areas, one in bashrc and other in spark-env.sh.template.

- Spark suppotrs two shells referred as REPL, one for scala and other is for python. (Scala - spark-shell) and (python shell - pyspark)

- Spark supports 4 programming Languages, Java, Python, Scala, R. And only for Python and Scala has shell availabilities. And whatever code we have to implement we have to convert it to a jar file and then execute it finally.

- Here in Spark also has 2 Daemon process which runs in background, Master and Worker.

- We can use Spark in 2 modes, Client Mode and Cluster Mode. If we are using Shell then it's Client Mode. If we are going with huge programs or IDE's then those background process should run, So we have to start both daemons and create a jar file before execution.

- Start and Stop cmds are present in sbin, so that to start those daemons we can use sbin/start-all.sh

- JPS -> Java Process Status

- There are 2 port Numbers available , RPC and web port. So to connect any daemon via code, then they have to connect it with code else web port Number.

- To start master daemon, start-master.sh  , port number - localhost:8080 and RPC : 7077 

- To start slave daemon, start-slave.sh spark://MILE-BL-4824-LAP.localdomain:7077 , port number - localhost:8081. So while connecting slave node with master via code we have to use RPC.

# PySpark WordCount Program


# Spark RDD:

- Resilient Distribution Dataset

- Spark does the coordination of all the executors. 

- Lineage is maintained in DAG.

- DAG is maintained in Spark Context.

- Action produces an output whereas transformation creates another RDD as previous RDD's output. 

# Overview of Spark Working

- Execution of program or job can be done in two ways, spark-submit or using interactive spark shell.

- In spark Context, both the DAG and Task scheduler are present.

- DAG Scheduler splitted into stages and submit each stages to task scheduler.

- Task scheduler splits each job to multiple tasks and sends to executor in worker Node.

- Once the task execution is done, there will be multiple partitions output which will be aggregated and sends back to driver program.

- Task can vary, like count or collect etc... So when we apply collect() the total sum data from all partitions to the driver program, so if the memory of driver program is lesser compared to the aggregated output it may throw an error. So always send the reduced output to driver.

# Creating a Spark Environment

- start-master.sh

- start-slave.sh spark://MILE-BL-4824-LAP.localdomain:7077

- Launching a Pyspark shell, 
  - pyspark --driver-memory 2G --driver-cores 2 --master spark://MILE-BL-4824-LAP.localdomain:7077

- Syntax :  spark-submit --executor-memory = 2g wordCount.py
  Here --executor-memory in above syntax can be replaced by other functions for configuration.
    - --deploy-mode -> Here it refers to whether the driver runs outside the cluster or inside the cluster. (default it is going to be client mode ,else cluster mode based on the deploy-mode)
    - --driver-memory -> by default it's 1GB for driver memory, and it can be customized.
    - --executor-memory -> by default it's 1 GB for executor memory, and can be customizable.
    - --executor-cores -> number of cores assigned to each executor.
    - --num-executors -> number of executors can also be specified, and this is possible only in kubernetes mode and yarn mode.
    - --driver-cores -> number of cores assigned to driver.

# Spark Modules
- In Spark Modules there are 4 types:
  - In Spark Core 
    - SparkSession 
    - SparkContext 
    - RDD -> transformations and Actions 
      - Different ways to create RDD is parallelization and from external file.
    - Shared Variable -> Broadcast and accumulator
    - DAG
    
 - The following comes on top of Spark Core, So whatever techs used in the following, atlast it is converted to RDD for transformations and actions.
      - SQL (In SQl the dataframes are used, later it's converted to RDD and used.)
        - The sources are dataframes (Files , tables, dictionary etc.. can be used as input Sources which is converted to dataframes then to RDD)  
      - Streaming
        - Structured Streaming 
      - Graph X
      - MLIB

    - Shared Variables:
       -  BroadCast variable : It is similar to ditributed Cache, So whatever data is stored in Broadcast variable, it will be stored in Cache memory inside the executor.
          -  So that the Broadcast variable data acts as a lookup table or reference data where the mappers can reference the lookup table for performing some actions or transformations. 
          - Broadcast variables are immutable
          -     transactions = [[100,"DEBIT",1000.0,"IND"],[101,"CREDIT",2000.0,"IND"],[102,"DEBIT",3000.0,"AUS"],[103,"CREDIT",4000.0,"JPN"],[104,"DEBIT",5000.0,"IND"],[105,"CREDIT",6000.0,"AUS"]]
                countries = sc.broadcast({'IND':'INDIA','AUS':'AUSTRALIA','JPN':'JAPAN'})
                rdd1 = sc.parallelize(transactions)
                def countries_convert(code):
                    return countries.value[code]
          
                rdd2 = rdd1.map(lambda x : (x[1],countries_convert(x[3])))
                rdd3 = sc.parallelize(rdd2.map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b).collect())
                output = rdd3.collect()
                for i,j in output:
                    print(i[0],i[1],j)
          ...
          - Output :
          - CREDIT JAPAN 1
          - CREDIT AUSTRALIA 1
          - CREDIT INDIA 1
          - DEBIT AUSTRALIA 1
          - DEBIT INDIA 2
          
      - Accumulator : Acts as Counter. It is mutable.
      -       from pyspark import SparkContext 
              sc = SparkContext("local", "Accumulator app") 
              num = sc.accumulator(10) 
              def f(x): 
                  global num 
                  num+=x 
              rdd = sc.parallelize([20,30,40,50]) 
              rdd.foreach(f) 
              final = num.value 
              print "Accumulated value is -> %i" % (final)
