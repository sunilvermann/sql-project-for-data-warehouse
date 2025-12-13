/*
================================
Create Database and Schemas
================================
Script Purpose:
	This script creates a new databse named 'DataWarehouse' after checking if it already exists.
	If the database exits, it is dropped and recreated. Additionally, the script sets up three schemas 
	within the database: 'bronze', 'silver', and 'gold'.

WARNING:
	Running this script will drop the entire 'DataWarehouse' database if it exists.
	All data in the database will be permanently deleted. with caution
	and ensure you have proper backups before running this scripts.
*/
USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
  /*
	IF EXITS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
	BEGIN
		ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
		DROP DATABASE DataWarehouse;
	END;
	GO
  */

-- CREATE DATABASE 'DataWarehouse'  

CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;

--Create Schema 
CREATE SCHEMA bronze;
GO  -- Go use as a seperater when you execute multiple statements

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
