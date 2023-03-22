# SNOWFLAKE Warehouse 

- We have to create a warehouse, then choose the corresponding warehouse, then create databases and tables accordingly.

        - create warehouse MYEBAYANALYTICS;

- Snowflake warehouse is not case sensitive
- We use Warehouses as intermediate to integrate wth visyualization tools for reports if the data is stored in Local system.

        - show databases;

# Case:1 Tables can be created on Snowflake Web Ui platform:

- In Hive Warehouse the size is not mandatory for datatypes, same goes for snowflake.
        
        - create table mytable1(id int, city varchar(33), country varchar);

        - insert into mytable1 values(123, 'hyder', 'india');

        - select * from mytable1;


- In Hive Warehouse the size is not mandatory for datatypes, same goes for snowflake.

# Case:2 Tables are imported from Local to Snowflake WareHouse

         CREATE OR REPLACE TABLE sales(
         Region varchar(80),
         Country varchar(80),Item_Type varchar(80),Sales_Channel varchar(80),Order_Priority varchar(80),Order_Date varchar(25),Order_ID varchar(80),Ship_Date varchar(25),Units_Sold int,Unit_Price int,Unit_Cost int,Total_Revenue int,Total_Cost int,Total_Profit int
         );
        
- Files can be insrerted from local to Snowflake warehouse using cli, web UI doesn't support insertion. 
- Initially move the local file to staging area and then move the file to staging area to warehouse.
        
        put file://mnt/c/Users/miles/Desktop/Sales_Records.csv @MILES.PUBLIC.%sales;
        
        COPY INTO sales FROM '@~/staged/Sales_Records.csv.gz' FILE_FORMAT=(TYPE=CSV, COMPRESSION = GZIP, SKIP_HEADER=1, FIELD_DELIMITER=',');

        select * from sales;
