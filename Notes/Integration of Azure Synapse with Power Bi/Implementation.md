# Order Of Implementation

- Create a resource Group.
- Create a Storage Account.
- Create Azure Synapse Workspace
- Configure the Azure Synapse Workspace using SQL Authentication.
- Create SQL Pool
- Create table and Insert records inside SQL Script.
- Copy the sql pool endpoint from synapse workspace and paste it in PowerBI and provide the credentials of Workspace SQL Authentication.

![resource group](https://user-images.githubusercontent.com/121089254/227128744-cbea63c3-b0a3-4feb-9016-e24601e83ddf.png)

Here in the Resource Group, 
  - storage account
  - synapse workspace
  - sql pool
  - ADLS storage (since the ADLS is default storage space for synapse workspace, it is created while creating synapse workspace)
 
![Synapse Workspace](https://user-images.githubusercontent.com/121089254/227129176-d9fd1ee2-2bfc-483d-82f8-9ea9c9f947a7.png)

Then copy the sql endpoint to connect with powerBi,

![workspace sql endpoint](https://user-images.githubusercontent.com/121089254/227129498-294bd114-d97e-41b7-b678-9c3988394030.png)

- Once the credentials are provided in database, All the tables present in the SQL Pool will be loaded to PowerBI.

![powerBi Desktop ](https://user-images.githubusercontent.com/121089254/227129929-b450cec3-b837-4d90-aaed-fa8f884a0c6b.png)
