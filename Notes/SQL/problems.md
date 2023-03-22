- 1) Write an SQL query to find all numbers that appear at least three times consecutively. Return the result table in any order.
      
      
            -- Here lead function is used to retrieve the next succeeding records.
            select distinct num1 as ConsecutiveNums from
            (select id , num as num1, lead(num,1) over(order by id) as num2, lead(num,2) over(order by id) as num3 from logs) derived_table
            where num1 = num2 and num2 = num3 

 -2) Write a SQL query to delete all duplicate email entries in a table named Person, keeping only unique emails based on its smallest Id. use delete statement.
      
            -- Here partition by is used to avoid duplicates in delete statement, and order by id is used since the smallest Id is required.
            with cte as (select id , email, row_number() over(partition by email order by id) as rnk from Person)
            delete from person where id in (select id from cte where rnk > 1)

 -3) 
