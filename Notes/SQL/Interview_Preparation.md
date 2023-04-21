- 1) To fetch the third highest age (Always go with dense_rank to find highest or lowest nth record )

![1](https://user-images.githubusercontent.com/121089254/233547445-e49df70d-d27d-44f0-abd9-82110be2903b.png)

- 2) To delete Duplicate Records: (Always go with row_number if duplicate records comes into picture) use partition by key and use rn > 1

![2](https://user-images.githubusercontent.com/121089254/233547977-a0e62295-37c3-4d3c-b3eb-f90107e8e542.png)

- 3) class wise top3 ranks (Go With dense_rank or row_number, if duplicates needs to be taken into account use dense_rank else row_number)

![3](https://user-images.githubusercontent.com/121089254/233549027-31098c0f-83f6-464b-b48c-fe5d8c4309fe.png)

![3 1](https://user-images.githubusercontent.com/121089254/233549050-ef006545-82b1-43e8-a160-e18e28c6579d.png)

- 4) 
