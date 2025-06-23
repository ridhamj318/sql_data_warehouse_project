/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

Create or Alter Procedure silver.load_silver as 
Begin
	declare @start_time datetime, @end_time Datetime;
    DECLARE @batch_start_time DATETIME;
    DECLARE @batch_end_time DATETIME;
	Begin Try
		set @batch_start_time = GETDATE();
		Print '==================';
		Print 'Loading silver Layer';
		Print '==================';

		Print 'Loading CRM Tables';

		set @start_time = GETDATE();
		Print '>> Truncating Table: silver.crm_cust_info';
		Truncate table silver.crm_cust_info;
        print '>> Inserting data into silver.crm_cust_info'
			INSERT INTO silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE flag_last = 1; -- Select the most recent record per customer
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		
		set @start_time = GETDATE();
		Print '>> Truncating Table: silver.crm_prd_info';
		Truncate table silver.crm_prd_info;
        Print '>> Insert into Table: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract product key
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, -- Map product line codes to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
				AS DATE
			) AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;
		set @end_time = GETDATE();
		print '>>LoadDuration : ' + CAST(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

		set @start_time = GETDATE();
		Print '>> Truncating Table: silver.crm_sales_details';
		Truncate table silver.crm_sales_details;
		Print '>> Insert Into Table: silver.crm_sales_details';
        insert into silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
        )
        select 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
            else cast(cast(sls_order_dt as varchar) as date)
        End as sls_order_dt,
        case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
            else cast(cast(sls_ship_dt as varchar) as date)
        End as sls_ship_dt,
        case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
            else cast(cast(sls_due_dt as varchar) as date)
        End as sls_due_dt,
        Case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
                then sls_quantity * abs(sls_price)
            else sls_sales
        end as sls_sales,
        sls_quantity,
        Case when sls_price is null or sls_price <=0
                then sls_sales / nullif(sls_quantity,0)
            else sls_price
        end as sls_price
        from bronze.crm_sales_details
		set @end_time = GETDATE();
		print '>>LoadDuration : ' + CAST(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

		set @start_time = GETDATE();
		Print 'Loading ERP Tables';
		Print '>> Truncating Table: silver.erp_cust_az12';
		Truncate table silver.erp_cust_az12;
        Print '>> Inserting into Table: silver.erp_cust_az12';

		insert into silver.erp_cust_az12 (cid,bdate,gen)
        select
        case when cid like 'Nas%' then substring(cid, 4, len(cid))
            else cid
        end as cid,
        case when bdate > GETDATE() then null
            else bdate
        end as bdate,
        case when upper(trim(gen)) in ('F' , 'Female') then 'Female'
            when upper(trim(gen)) in ('M' , 'Male') then 'Male'
            Else 'n/a'
        end as gen
        from bronze.erp_cust_az12
		set @end_time = GETDATE();
		print '>>LoadDuration : ' + CAST(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

		set @start_time = GETDATE();
		Print '>> Truncating Table: silver.erp_loc_a101';
		Truncate table silver.erp_loc_a101;
        Print '>> Inserting into Table: silver.erp_loc_a101';
		insert into silver.erp_loc_a101(cid,cntry)
        select 
        replace(cid, '-', '') as cid, 
        case when trim(cntry) = 'DE;' then 'Germany'
            when trim(cntry) in ('USA' , 'US') then 'United States'
            when trim(cntry) = '' or cntry is null then 'n/a'
            else trim(cntry)
        end as cntry
        from bronze.erp_loc_a101
		set @end_time = GETDATE();
		print '>>LoadDuration : ' + CAST(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

		set @start_time = GETDATE();
		Print '>> Truncating Table: silver.erp_px_cat_g1v2';
		Truncate table silver.erp_px_cat_g1v2;
        Print '>> Inserting into Table: silver.erp_px_cat_g1v2';
        insert into silver.erp_px_cat_g1v2 (id ,cat, subcat, maintenance)
        select
        id,
        cat,
        subcat,
        maintenance
        from bronze.erp_px_cat_g1v2		
        set @end_time = GETDATE();
		print '>>LoadDuration : ' + CAST(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		
		set @batch_end_time = GETDATE();
		print '--------------'
		print 'Loading silver Layer is Completed'
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
