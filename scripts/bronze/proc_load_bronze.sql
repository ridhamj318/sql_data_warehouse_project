/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

Create or Alter Procedure bronze.load_bronze as 
Begin
	declare @start_time datetime, @end_time Datetime;
    DECLARE @batch_start_time DATETIME;
    DECLARE @batch_end_time DATETIME;
	Begin Try
		set @batch_start_time = GETDATE();
		Print '==================';
		Print 'Loading Bronze Layer';
		Print '==================';

		Print 'Loading CRM Tables';

		set @start_time = GETDATE();
		Print '>> Truncating Table: bronze.crm_cust_info';
		Truncate table bronze.crm_cust_info;
		Bulk Insert bronze.crm_cust_info
		from 'C:\Users\Ridham Jain\Desktop\data_engineer\project\data warehouse & Modelling project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with ( FirstRow = 2, Fieldterminator = ',' , Tablock );
		set @end_time = GETDATE();
		print '>>LoadDuration : ' + CAST(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

		set @start_time = GETDATE();
		Print '>> Truncating Table: bronze.crm_prd_info';
		Truncate table bronze.crm_prd_info;
		Bulk Insert bronze.crm_prd_info
		from 'C:\Users\Ridham Jain\Desktop\data_engineer\project\data warehouse & Modelling project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with ( FirstRow = 2, Fieldterminator = ',' , Tablock );
		set @end_time = GETDATE();
		print '>>LoadDuration : ' + CAST(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

		set @start_time = GETDATE();
		Print '>> Truncating Table: bronze.crm_sales_details';
		Truncate table bronze.crm_sales_details;
		Bulk Insert bronze.crm_sales_details
		from 'C:\Users\Ridham Jain\Desktop\data_engineer\project\data warehouse & Modelling project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with ( FirstRow = 2, Fieldterminator = ',' , Tablock );
		set @end_time = GETDATE();
		print '>>LoadDuration : ' + CAST(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

		set @start_time = GETDATE();
		Print 'Loading ERP Tables';
		Print '>> Truncating Table: bronze.erp_cust_az12';
		Truncate table bronze.erp_cust_az12;
		Bulk Insert bronze.erp_cust_az12
		from 'C:\Users\Ridham Jain\Desktop\data_engineer\project\data warehouse & Modelling project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with ( FirstRow = 2, Fieldterminator = ',' , Tablock );
		set @end_time = GETDATE();
		print '>>LoadDuration : ' + CAST(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

		set @start_time = GETDATE();
		Print '>> Truncating Table: bronze.erp_loc_a101';
		Truncate table bronze.erp_loc_a101;
		Bulk Insert bronze.erp_loc_a101
		from 'C:\Users\Ridham Jain\Desktop\data_engineer\project\data warehouse & Modelling project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with ( FirstRow = 2, Fieldterminator = ',' , Tablock );
		set @end_time = GETDATE();
		print '>>LoadDuration : ' + CAST(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

		set @start_time = GETDATE();
		Print '>> Truncating Table: bronze.erp_px_cat_g1v2';
		Truncate table bronze.erp_px_cat_g1v2;
		Bulk Insert bronze.erp_px_cat_g1v2
		from 'C:\Users\Ridham Jain\Desktop\data_engineer\project\data warehouse & Modelling project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with ( FirstRow = 2, Fieldterminator = ',' , Tablock );
		set @end_time = GETDATE();
		print '>>LoadDuration : ' + CAST(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		
		set @batch_end_time = GETDATE();
		print '--------------'
		print 'Loading Bronze Layer is Completed'
		print ' Total Load Duration: ' + Cast(Datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
		print ' ------------ '
	END Try
	Begin Catch
		print'---------------------';
		print'error occur During loading';
		print'Error Message' + Error_message();
		print'Error Number' + Cast (Error_number() as nvarchar);
		print'Error State' + Cast (Error_state() as nvarchar);
		print'---------------------';
	End Catch
END
