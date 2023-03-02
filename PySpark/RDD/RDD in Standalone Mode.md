# RDD Functionality:

- Resilient Distribution Dataset (Reliability, Distribution and Fault Intolerance)

- Dataframe and dataset are the next versions of RDD.

- Actions and Transformations are the two operations used in RDD's.

- The distribution of data among the Worker Nodes takes place before parallel processing.

- The Spark read the input file and creates a logical partitions and stores in the worker Nodes.

- The Number of logical partitions may or may not be equal with the Worker Nodes. Eg : 4 partitions of data can be stored in 3 worker Nodes is a possibility.

- And the data in Worker Node is stored in Memory (RAM) not in the disc.

- In Spark we can create N. Number of transformations. All the transformations and it's intermediate results are stored in persist, This maintanence is called as Data Lineage.

- DAG is maintaining the data lineage. And DAG data is stored in Spark Master Node.

- So if any one of the task is failed, Spark won't fail the whole job, with the help of lineage it will recompute the whole procees for that corresponding task.

- Replication is also solution for the above issue instead of Recomputing.

- Replication can be stored in any persist storage. Sometimes Replication provides faster results and sometimes recomputing with the help of lineage gives faster results.

# RDD Transformation and Actions:

- Difference between transformation and action ? 

- Ans : For every transformation Spark creates another RDD and action sends the output to user.

- Lazy Evaluation : In Spark the compiler, when it finds any action in the code, then only it will start the job.

- The compiler won't start transformations unless a action is foundin job, because if no action is found, it result's in wastage of resources for transformation.

- There are two types of transformation, Wide and Narrow transformation.

- So if shuffling is required in any transformations it's a wide transformation else narrow transformation.

- For Every Jobs, it creates many intermediate stages based on transformations which we used in the code.

- Here if we are using 5 narrow transformations all the transformations are performed in the single stage, let's assume stage 1. And if we are using any 
wide transformation then the transformation is moved to Stage 2. 

- The stages can be seen visually using DAG in master UI port: 8080

# Distribution of Data to Worker Nodes.

- Parallelize : Rdd is a simple data structure which is spliited into partitions and sends to various worker Nodes at same time.

- Spark offers a method called Spark.parallelize(data,n) which converts the data type to RDD to parallelizing the data (refers partitions). And n refers the number of partitions, which can be assigned explicitly.

- Rdd's can be created from list, set or dictionary or other file formats. And can also be created from another RDD.
