/*
===================================================================================
DDL Script: Create Bronze Tables
===================================================================================
Script Purpose:
  This Script creates tables in the 'bronze' schema, dropping existing tables if they already exits.
Run this script to re-define the DDL structure if 'bronze' Tables
===================================================================================
*/

-- Drop the table if exits and create a new table
IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	  cst_id INT,
	  cst_key NVARCHAR(50),
	  cst_firstname NVARCHAR(50),
	  cst_lastname NVARCHAR(50),
	  cst_marital_status NVARCHAR(50),
	  cst_gndr NVARCHAR(50),
	  cst_create_date DATE
);

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
sls_ord_num	NVARCHAR(50),
sls_prd_key	NVARCHAR(50),
sls_cust_id	INT,
sls_order_dt INT,
sls_ship_dt	INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);


IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)
);


IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
cid NVARCHAR(50),
cntry NVARCHAR(50)
);


IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50)
);
--=========================================================


--===============================================================
--Create Stored Procedure
-- HINT - Save frequently used SQL code in stored procedures in database
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; -- use get the how time to execute the table 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '====================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '====================================================================';

		PRINT '--------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------------------------------';
	
	
		-- INSERT BULK DATA from Customer_information file into DB
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;  -- here we are doing the FULL LOAD becosue first truncate the table and load the data into the table.
	
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Analytics With Anand\SnowSQL\files and docs\sql-ultimate-course-main_with BARA\Projects DataSets\Data_Warehouse_Project\source_crm\cust_info.csv'
		WITH (
		   FIRSTROW = 2, -- firstraw is a header so data start from 2 
		   FIELDTERMINATOR = ',', -- data is sepearted by comma so we use FIELDTERMINATOR
		   TABLOCK --Improve performance for bulk operations
		);
		SET @end_time = GETDATE();
		PRINT '<< Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------------------------------------------';
		-- Check the inserted data is correct or not
		-- SELECT * FROM bronze.crm_cust_info
		-- SELECT COUNT(*) FROM bronze.crm_cust_info

		-- INSERT BULK DATA from Product_information file into DB
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;  -- here we are doing the FULL LOAD becosue first truncate the table and load the data into the table.
	
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Analytics With Anand\SnowSQL\files and docs\sql-ultimate-course-main_with BARA\Projects DataSets\Data_Warehouse_Project\source_crm\prd_info.csv'
		WITH (
		   FIRSTROW = 2, -- firstraw is a header so data start from 2 
		   FIELDTERMINATOR = ',', -- data is sepearted by comma so we use FIELDTERMINATOR
		   TABLOCK  -- Improve performance for bulk operations
		);
		SET @end_time = GETDATE();
		PRINT '<< Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------------------------------------------';
		-- Check the inserted data is correct or not
		-- SELECT * FROM bronze.crm_prd_info
		-- SELECT COUNT(*) FROM bronze.crm_prd_info

		-- INSERT BULK DATA from Sales_Details file into DB
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;  -- here we are doing the FULL LOAD becosue first truncate the table and load the data into the table.
	
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Analytics With Anand\SnowSQL\files and docs\sql-ultimate-course-main_with BARA\Projects DataSets\Data_Warehouse_Project\source_crm\sales_details.csv'
		WITH (
		   FIRSTROW = 2, -- firstraw is a header so data start from 2 
		   FIELDTERMINATOR = ',', -- data is sepearted by comma so we use FIELDTERMINATOR
		   TABLOCK  -- Improve performance for bulk operations
		);
		SET @end_time = GETDATE();
		PRINT '<< Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------------------------------------------';
		-- Check the inserted data is correct or not
		-- SELECT * FROM bronze.crm_sales_details
		-- SELECT COUNT(*) FROM bronze.crm_sales_details


		PRINT '--------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------------------------------------------';
	
		-- INSERT BULK DATA from ERP Customer AZ12 file into DB
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;  -- here we are doing the FULL LOAD becosue first truncate the table and load the data into the table.
	
	
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\Analytics With Anand\SnowSQL\files and docs\sql-ultimate-course-main_with BARA\Projects DataSets\Data_Warehouse_Project\source_erp\CUST_AZ12.csv'
		WITH (
		   FIRSTROW = 2, -- firstraw is a header so data start from 2 
		   FIELDTERMINATOR = ',', -- data is sepearted by comma so we use FIELDTERMINATOR
		   TABLOCK  -- Improve performance for bulk operations
		);
		SET @end_time = GETDATE();
		PRINT '<< Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------------------------------------------';
		-- Check the inserted data is correct or not
		-- SELECT * FROM bronze.erp_cust_az12
		-- SELECT COUNT(*) FROM bronze.erp_cust_az12


		-- INSERT BULK DATA from ERP Customer Location A101 file into DB
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;  -- here we are doing the FULL LOAD becosue first truncate the table and load the data into the table.
	
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\Analytics With Anand\SnowSQL\files and docs\sql-ultimate-course-main_with BARA\Projects DataSets\Data_Warehouse_Project\source_erp\LOC_A101.csv'
		WITH (
		   FIRSTROW = 2, -- firstraw is a header so data start from 2 
		   FIELDTERMINATOR = ',', -- data is sepearted by comma so we use FIELDTERMINATOR
		   TABLOCK  -- Improve performance for bulk operations
		);
		SET @end_time = GETDATE();
		PRINT '<< Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------------------------------------------';
		-- Check the inserted data is correct or not
		-- SELECT * FROM bronze.erp_loc_a101
		-- SELECT COUNT(*) FROM bronze.erp_loc_a101


		-- INSERT BULK DATA from ERP Product Category g1v2 file into DB
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;  -- here we are doing the FULL LOAD becosue first truncate the table and load the data into the table.
	
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\Analytics With Anand\SnowSQL\files and docs\sql-ultimate-course-main_with BARA\Projects DataSets\Data_Warehouse_Project\source_erp\PX_CAT_G1V2.csv'
		WITH (
		   FIRSTROW = 2, -- firstraw is a header so data start from 2 
		   FIELDTERMINATOR = ',', -- data is sepearted by comma so we use FIELDTERMINATOR
		   TABLOCK  -- Improve performance for bulk operations
		);
		SET @end_time = GETDATE();
		PRINT '<< Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------------------------------------------';
		
		SET @batch_end_time = GETDATE();
		PRINT '======================================================';
		PRINT 'Loading Bronze Layer is completed';
		PRINT '  - Total Load Duration: ' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '======================================================';
		-- Check the inserted data is correct or not
		-- SELECT * FROM bronze.erp_px_cat_g1v2
		-- SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2


	END TRY

	BEGIN CATCH
		PRINT '=========================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS VARCHAR);
		PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS VARCHAR);
		PRINT '=========================================================================';
	END CATCH
END;

-- Execute the Stored Procedure
EXEC bronze.load_bronze
