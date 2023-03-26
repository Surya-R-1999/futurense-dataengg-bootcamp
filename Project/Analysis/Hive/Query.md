# Analysis using Hive in Cloudera:

- Processed Dataset: /mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/Project/disney_processed_file.csv

- The whole analysis is done in Cloudera.

# Client DB:

- The dataset is present in local file system. 
- The dataset is moved to cloudera via drag and drop.
- Using MySQL CLI in cloudera, database and tables are created.

        create database ott_platform;
        use ott_platform;
        
        
        Create table disney_hotstar(title varchar(90),yr int,director varchar(40),awards varchar(60),votes int,release_year int,Total_win int,Total_Nominated int);
        
        -- The path referenced to cloudera.
        
        LOAD DATA INFILE '/home/cloudera/Desktop/disney_processed_file.csv'
        INTO TABLE disney_hotstar
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES; 

- Total of 992 Records have been Inserted to Client DB.

# Importing the data from ClientDB to Hive Data Warehouse.

- Hive default Storage is HDFS. Therefore importing Structured data from anywhere, outside the hadoop eco-system is done via SQOOP.

- Using Sqoop import, we are importing the table from ClientDB to Hive warehouse for analysis, once the analysis is done, the results are stored in external table and sent to clinetDB once again.

        sqoop import \
        --connect jdbc:mysql://localhost/ott_platform \
        --username root \
        --password cloudera \
        --table disney_hotstar \
        --hive-import \
        --hive-overwrite \
        --create-hive-table \
        --hive-table disney_hotstar \
        --num-mappers 1

- The table has been imported Successfully to Hive Warehouse.

# Analysis:

![image](https://user-images.githubusercontent.com/121089254/227793116-6fa6375e-a6ef-43ef-a9c1-59dfc451fd1b.png)


