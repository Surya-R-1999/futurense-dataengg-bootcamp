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

- Hive only supports subqueries in the FROM clause, not in the SELECT or WHERE clauses.
- Hive supports only scalar subqueries, which return a single value. 
- Also in Limit Clause don't use numbers representing the order of the column, just use the alias name of the column.

![image](https://user-images.githubusercontent.com/121089254/227793116-6fa6375e-a6ef-43ef-a9c1-59dfc451fd1b.png)

        select d1.director, d1.yr, d1.votes from disney_hotstar d1 
        join (SELECT d2.director as director, MAX(d2.votes) AS max_votes FROM disney_hotstar d2 
        GROUP BY d2.director order by max_votes desc limit 1) t2 
        on d1.director = t2.director 
        order by d1.votes desc 
        limit 1;
    
![solution1](https://user-images.githubusercontent.com/121089254/227826881-e13f1499-4855-4a8f-aae8-4186bc36565e.png)
  


![image](https://user-images.githubusercontent.com/121089254/227825893-9e48000f-97e2-4e1c-96e3-5627f38c577d.png)

        select director as director, count(title) as movie_cnt
        from disney_hotstar 
        where release_year = 1999 and director != 'N/A'
        group by director

![Solution2](https://user-images.githubusercontent.com/121089254/227826916-1d3c61e1-c522-4595-bd2b-9a1971f3fbd0.png)


![image](https://user-images.githubusercontent.com/121089254/227825924-ff37f129-8577-4488-b5a0-34a6acb02b13.png)

        select d2.director 
        from disney_hotstar d2 
        join (
            select d1.title as title, d1.yr, count(d1.awards) as awards_count
            from disney_hotstar d1
            where d1.awards != 'N/A' and d1.title != 'N/A' and d1.title != 'null'
            group by d1.title ,d1.yr
            order by awards_count desc
            limit 1
        ) subq
        on d2.title = subq.title
        where d2.director != 'N/A';
        
![solution3](https://user-images.githubusercontent.com/121089254/227829073-aa500a23-d4a1-4515-9d0f-32fd2367a55a.png)

![image](https://user-images.githubusercontent.com/121089254/227825941-f02ff5bb-ab68-44c5-bc8c-e83c83027f55.png)

        select count(title) as award_nominated_movie_cnt from disney_hotstar 
        where yr between 1998 and 2018 and awards != 'N/A';
       
![Solution4](https://user-images.githubusercontent.com/121089254/227826964-b6bcf125-6b9f-46ee-8397-d829f81f9181.png)

