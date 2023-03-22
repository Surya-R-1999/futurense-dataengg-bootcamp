- We have to create a warehouse, then choose the corresponding warehouse, then create databases and tables accordingly.

        - create warehouse MYEBAYANALYTICS;

- Snowflake warehouse is not case sensitive
- We use Warehouses as intermediate to integrate wth visyualization tools for reports if the data is stored in Local system.

        - show databases;

- In Hive Warehouse the size is not mandatory for datatypes, same goes for snowflake.
        
        - create table mytable1(id int, city varchar(33), country varchar);

        - insert into mytable1 values(123, 'hyder', 'india');

        - select * from mytable1;

