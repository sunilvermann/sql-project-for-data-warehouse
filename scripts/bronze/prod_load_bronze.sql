/*
=================================================================
Store Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================
Script Purpose:
  This stored Procedire loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
  - Truncate the bronze tables before loading data.
  - Uses the `BULK INSERT` command to load data from csv files to bronze tables.

PParameters:
  None.
  This stored procedure does not accepts any parameters or return any values.

Usage Examples:
  EXEC bronze.load_bronze;
*/

-- Create Stored Procedure
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
