# Storage levels

-  In RDD there are 3 storage levels,
-  MEMORY_ONLY
-  MEMORY_AND_DISK
-  DISK_ONLY
-  MEMORY_ONLY_2, MEMORY_AND_DISK_2, DISK_ONLY_2 -> refers the number of replications. 
-  rdd.getStorageLevel() -> refers the storage level

# Persistent levels and Replications

-  rdd.unpersist() -> to remove the storage level
-  rdd.persist() -> sets the storag elevel with replications also.

- Here these replications are used over DAG because, we can avoid the DAG lineage computation and use replication so the time reduces.
- In the above case, if data is stored in memory storage level, it will take lesser time.
- Sometimes the recomputation takes lesser time than extracting data from disc storage level.

# Memory Calculation for Cluster Management, Executors and Driver Program.

- Cores or (Multi Threading)-> It refers to the number of parallel tasks for each executors.
- Let's assume there are 6 nodes, and each node has 15 cores and 64 GB.
- So that, We can give 5 parallel task for each executor based on developer knowledge. Therefore 3 executors can be created for each Node. 
- And The memory allocation will be as follows,
    - Each exceutor will have 64 / 3 = 21 GB, But considering the resource management and the driver program, Let's take 15 GB per executor.
    - 5 Gb min for cluster Management.
    - 15 Gb for Driver Program, same as the executor.
