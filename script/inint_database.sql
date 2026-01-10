/* 
=========================================================
Create Database and Schemas
=========================================================
Script Purpose:
    This script creates a new database named 'Datawarehouse' after checking if it already exists.
    If the database exists, it is dropped and recreated. Addtionaly, the script setsup three schemas
    within the database: 'bronze', 'Silver' and 'Gold'.

*/
  
USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS(SELECT 1 FROM sys.databases WhERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
End;
GO

-- Create the 'DataWarehouse' database

CREATE DATABASE DataWarehouse;
USE DataWarehouse;


-- Create the Schemas 
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
