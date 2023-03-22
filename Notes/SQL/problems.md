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
 
- 6) 
