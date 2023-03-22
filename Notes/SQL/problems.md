- 1) Write an SQL query to find all numbers that appear at least three times consecutively. Return the result table in any order.
      
            -- Here lead function is used to retrieve the next succeeding records.
            select distinct num1 as ConsecutiveNums from
            (select id , num as num1, lead(num,1) over(order by id) as num2, lead(num,2) over(order by id) as num3 from logs) derived_table
            where num1 = num2 and num2 = num3 

- 2) Write a SQL query to delete all duplicate email entries in a table named Person, keeping only unique emails based on its smallest Id. use delete statement.

            -- Here partition by is used to avoid duplicates in delete statement, and order by id is used since the smallest Id is required.
            with cte as (select id , email, row_number() over(partition by email order by id) as rnk from Person)
            delete from person where id in (select id from cte where rnk > 1)

- 3) Write a SQL query to get the Second highest salary from the Employee table. If the second highest salary is not present it should return null.

            -- We use aggregate function in both outer and inner query to filter out the first element, and return's null if there is no record satisfying the inner                    query.
            select max(salary) as SecondHighestSalary from Employee where salary < (select max(salary) from Employee)

- 4)  A company’s executives are interested in seeing who earns the most money in each of the company’s departments. A high earner in a department is an employee who has a salary in the top three unique salaries for that department. Write an SQL query to find the employees who are high earners in each of the departments. 
   
            select Department, Employee, Salary  from
            (select d.name as Department, e.name as Employee, e.salary as Salary, dense_rank() over(partition by e.departmentId order by e.salary desc) as rnk
            from Employee e 
            join Department d 
            on e.departmentId = d.id) derived_table
            where rnk <= 3
            order by Salary desc
            
- 5) The Employee table holds all employees including their managers. Every employee has an Id, and there is also a column for the manager Id.

            select e2.name as Employee
            from Employee e1 
            join Employee e2
            on e1.id = e2.managerId 
            where e1.salary < e2.salary
 
- 6) Suppose that a website contains two tables, the Customers table, and the Orders table. Write a SQL query to find all customers who never order anything.

           select name as Customers from customers c where c.id not in (select customerId from Orders)
 
- 7) Write a SQL query to rank scores. If there is a tie between two scores, both should have the same ranking. Note that after a tie, the next ranking number should be the next consecutive integer value. In other words, there should be no “holes” between ranks.

           select score, dense_rank() over(order by score desc) as 'rank'
           from Scores
           order by score desc
- 8) Write an SQL query to find all dates’ id with higher temperature compared to its previous dates (yesterday).

            select w1.id
            from Weather w1 
            join Weather w2 
            on datediff(w1.recorddate,w2.recordDate) = 1 
            where w1.temperature > w2.temperature

- 9) Write an SQL query that reports the first login date for each player.

            select player_id, event_date as first_login from
            (select player_id, event_date, row_number() over(partition by player_id order by event_date) as 'rnk' from Activity ) derived_table
            where rnk = 1
            
- 10) Write a SQL query that reports the device that is first logged in for each player.

           select player_id, device_id from (select player_id, device_id, row_number() over(partition by player_id order by event_date) as 'rnk' from Activity)                    derived_table where rnk = 1

- 11) Write an SQL query that reports for each player and date, how many games played so far by the player. That is, the total number of games played by the player until that date. Check the example for clarity.
            
            -- aggregate functions are also used as window functions, and to sum based on particular key we use partition by and the order by is used to specify the               summation order
            
            select player_id, event_date, games_played_so_far
            from (select player_id, event_date, games_played, 
            sum(games_played) over(partition by player_id order by event_date) as 'games_played_so_far' from Activity) derived_table

- 12) Write an SQL query that reports the fraction of players that logged in again on the day after the day they first logged in, rounded to 2 decimal places. In other words, you need to count the number of players that logged in for at least two consecutive days starting from their first login date, then divide that number by the total number of players.
            
URL: https://lifewithdata.com/2021/08/03/sql-interview-questions-leetcode-550-game-play-analysis-iv/

            WITH cte AS (
            SELECT player_id, MIN(event_date) as first_login
            FROM Activity
            GROUP BY player_id
            )

            SELECT ROUND(SUM(CASE WHEN DATEDIFF(event_date, first_login)=1 THEN 1 ELSE 0  END) / COUNT(DISTINCT cte.player_id), 2) as fraction
            FROM Activity as a
            JOIN cte 
            ON a.player_id = cte.player_id

   * Here the cte contains the player_id and the first login date of each player  with the help of min agg fn and group by clause of player_id
   * Join query is for finding consecutive logins and used a case fn if it's true else 0, and sum() agg fn is used to sum up the case column
   * Atlast to find the total players, count(distinct player_id) is used and divided to get the resultant output.

- 13)   Write a SQL query to find the median salary of each company. Bonus points if you can solve it without using any built-in SQL functions.

URL: https://lifewithdata.com/2021/08/03/google-sql-interview-questions-leetcode-569-median-employee-salary/

            WITH cte AS (
            SELECT 
                  id,
                  company,
                  Salary,
                  ROW_NUMBER() OVER(PARTITION BY company ORDER BY Salary) as rn,
                  COUNT(*) OVER(PARTITION BY company ) as rc 
            FROM Employee
            )

            SELECT Id, company, salary 
            FROM cte 
            WHERE rn IN ( (rc+1 ) DIV 2 , (rc+2) DIV 2 )

   * row_number window fn is used based on salary (asc) (rn) and count(*) is used calculate total count of corresponding company. (rc)
   * In where clause, if it's even count rc + 2 else if it's odd count rc + 1, and DIV is used for integer division.

- 14)  


