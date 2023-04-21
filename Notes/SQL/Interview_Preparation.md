- 1) To fetch the third highest age (Always go with dense_rank to find highest or lowest nth record )

![1](https://user-images.githubusercontent.com/121089254/233547445-e49df70d-d27d-44f0-abd9-82110be2903b.png)

- 2) To delete Duplicate Records: (Always go with row_number if duplicate records comes into picture) use partition by key and use rn > 1

![2](https://user-images.githubusercontent.com/121089254/233547977-a0e62295-37c3-4d3c-b3eb-f90107e8e542.png)

- Using inner join

- Here I'm deleting from users1 table so < sign is used.

![2 1](https://user-images.githubusercontent.com/121089254/233559051-6bb9dae7-1054-4a05-b721-4e8e88678c5b.png)


- 3) class wise top3 ranks (Go With dense_rank or row_number, if duplicates needs to be taken into account use dense_rank else row_number)

![3](https://user-images.githubusercontent.com/121089254/233549027-31098c0f-83f6-464b-b48c-fe5d8c4309fe.png)

![3 1](https://user-images.githubusercontent.com/121089254/233549050-ef006545-82b1-43e8-a160-e18e28c6579d.png)

- Correlated SubQuery:

- This query uses a correlated subquery to count the number of students with a higher score than each student in the same class, and then selects only the top 3 students in each class based on this count. The result is sorted by class ID and score in descending order. Note that this query assumes that there are no ties in scores, as in the previous query.

- Consider 
![3 2](https://user-images.githubusercontent.com/121089254/233551668-43a840bd-dc76-4753-b98b-bab7ccffdcb0.png)

- In this query, the outer query is executed after the inner query. The subquery in the WHERE clause is executed for each row in the outer query, and the result is used to filter the rows in the outer query. This is known as a correlated subquery, and it is executed for each row in the outer query.

- Here is the order of execution of the query:

- The subquery in the WHERE clause is executed for each row in the outer query. For each row, the subquery counts the number of students with a higher score than the current row in the same class.

- The count result from the subquery is compared to 3, and if it is less than 3, the row is included in the final result set.

- The rows that pass the filter in the WHERE clause are sorted by class_id and score in descending order.

- The final result set is returned to the user.

- So, in summary, the inner query is executed first, for each row in the outer query, and then the outer query is executed using the results of the inner query to filter the rows.

- 4) Find the age from dob

![4](https://user-images.githubusercontent.com/121089254/233552930-8a5e5d47-cfb4-44ee-a354-210784e989f1.png)

- dob must be in the default date format and enclosed with quotes . ('2049/12/25')

- 5) Extract the middle name from FullName using Mysql : (We are using substring_index function takes 3 arguments , colname, delimiter and the count of elements)

![5](https://user-images.githubusercontent.com/121089254/233556598-978d17fe-ab79-475e-a804-f0c8390b81aa.png)

- 6) highest score for each student  

- Window function is used.
![6](https://user-images.githubusercontent.com/121089254/233560449-f50097a4-d552-4fa5-b107-1699e5ebb1d1.png)

- Without window Function:

![6 2](https://user-images.githubusercontent.com/121089254/233560770-82edcdbd-755a-4b33-a2c1-cf0045dfb72c.png)

- 7) print duplicate rows in a table 

- 8) print records other than duplicated rows

![7,8](https://user-images.githubusercontent.com/121089254/233656003-f7cf9d32-e5f1-47d3-ae2b-e1510c704183.png)

- 9) Skip

- 10) Delete duplicate records from a file, --> (Is mostly same as deleting records from table(referred as file) refer q:2)

- 15) Query max of salary of each department with the emp_id and emp_name also.

![15](https://user-images.githubusercontent.com/121089254/233670706-33b6fb43-2109-4048-9acd-ec3a2c05424a.png)

- 16) 
