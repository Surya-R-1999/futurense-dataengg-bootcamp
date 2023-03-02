# StandAlone Mode of Pyspark

- Spark has various deployment Modes like Spark with Mesos or Spark with Yarn and Spark StandAlone
- If we install Spark as a cluster as single or multiple Nodes without any other technologies like Yarn, or cluster management or hadoop etc.. it is StandAlone Architecture.

- To start the pyspark shell in Standalone mode then connect the pyspark with master URL
- pyspark --master spark://MILE-BL-4824-LAP.localdomain:7077
   
- Terminologies:
   - Daemons - background Process
   - Master and Worker are the two daemons in Spark.
   - Driver Program : The program what we write in any programming language, where we declare spark Context as entry point of the compiler to start the execution, 
  which is responsible for all activities like job and job to tasks.
   
- The flow of job submition and execution,
   - In Client Node when we send a jar file with Spark-submit request, the request goes to spark master machine. 
   
   - In Spark Master, a driver program will start. So Number of jobs created by each clients = Number of driver programs will start in spark master of the cluster.
   
   - Driver Program will communicate with cluster manager, which is a service which runs inside Spark itself, It is responsible for resource allocation like number of cores 
   and memory usage etc... These informations will be provided by the programmer while creating the architecture itself.
   
   - Cluster Manager allocates the resources for each worker Node requested by the driver program.
  
   - Cluster Manager is also responsible for creating executors in worker Nodes.

   - The executors are created near the data resides in the corresponding worker Node.
   
   - Note : Distribution of data should be done before parallel processing. (Data has been distributed as RDD before spark-submit)
   
   - Cluster Manager will identify on which node the data resides . This is referred as data locality.
   
   - Once the executor is created, it sends heart beats to the driver program frequently.
   
   - Driver Program will assign jobs whatever written in jar file to the executors in different Nodes at the same time and the process will start by executors once the job is assigned.
   
   - Same job has been implemented on different worker Nodes and atlast all results are grouped together.
   
   - The output of each executors (which is refered as Intermediate data) are stored in Persist.
   
   - Persist has 3 types, memory only, disk only and disk and memory
   
   - Grouping can be done atlast like groupByKey or reduceByKey or some other transformtion by shuffling based on the programmer needs.......
   
   - For Grouping(the final result), another executor is created on same node or different node by the Cluster Manager to store the result of final result and stored in Persist
   
   - If Executor fails for any Node or the whole node faces downtime during process, it won't affect the other executors of same job. Because of DAG or Persist replications in memory itself.
   
   - Driver program will recreate the executor with the help of cluster manager and recreate the whole process if replication isn't available.
   
   - If driver program fails, every executor has to be recreated and all the process has to be recreated.
   
   - What if Master itself faces downtime, Since it's a single point of Contact ?
   
   -Ans :  So With the help of Zoo Keeper daemon, the passive master will takes place of the active master
   
   - All passive master will get heart beats from all worker Nodes, so at time of single point of failure, when a passive master takes place of active master then it can be stil in sync with slave nodes.
   
# Execution of hive queries in a script File Format from Linux shell

- place all the hive queries in a .q file and execute,
   -  hive -f directory_path   
- Non-Interactive Mode of hive query execution:
   - hive -e "select * from database.table_name"
