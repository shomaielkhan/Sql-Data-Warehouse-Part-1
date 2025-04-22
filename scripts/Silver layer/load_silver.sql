exec silver.load_silver

CREATE or ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
	set @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
	print '>> Truncating Table silver.crm_cust_info';
	Truncate table silver.crm_cust_info
	print'=========================';
	print '>> Inserting Data into: silver.crm_cust_info';
	print'=========================';

	insert into silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)

	select 
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	case when cst_marital_status = 'S' then 'Single'
		when cst_marital_status = 'M' then 'Married'
		else 'n/a'
	end as cst_marital_status,
	case when cst_gndr = 'F' then 'Female'
		when cst_gndr = 'M' then 'Male'
		else 'n/a'
	end as cst_gndr,
	cst_create_date
	from
	( 
	select *,
	row_number() over(partition by cst_id order by cst_create_date) as flag_last
	from bronze.crm_cust_info) t
	where flag_last = 1 ;
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	------------
	SET @start_time = GETDATE();
	print '>> Truncating Table silver.crm_prd_info';
	Truncate table silver.crm_prd_info;
	print'========================='
	print '>> Inserting Data into: silver.crm_prd_info';
	print'=========================';

	insert into silver.crm_prd_info(
		prd_id,
		prd_key,
		cat_id,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_date,
		prd_end_date
	)

	select  
		prd_id,
		SUBSTRING(prd_key, 1, 13 ) as prd_key,
		replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
		prd_nm,
		isnull(prd_cost, 0) as prd_cost, 
		case when prd_line = 'M' then 'Mountain'
			when prd_line = 'R' then 'Road'
			when prd_line = 'S' then 'Other Sales'
			when prd_line = 'T' then 'Touring'
			else 'n/a'
	end as prd_line,
		prd_start_date,
		LEAD(prd_start_date) over( partition by prd_key order by prd_start_date) as prd_end_date
	from
	bronze.crm_prd_info ;
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	--------------
			SET @start_time = GETDATE();
	print '>> Truncating Table silver.crm_sales_details';
	Truncate table silver.crm_sales_details
	print'=========================';
	print '>> Inserting Data into: silver.crm_sales_details';
	print'=========================';

	insert into silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_date,
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
	end sls_order_dt,

	case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
		 else cast(cast(sls_ship_dt as varchar) as date)
	end sls_ship_dt,
	case when sls_due_date = 0 or len(sls_due_date) != 8 then null
		 else cast(cast(sls_due_date as varchar) as date)
	end sls_due_date,
	case when sls_sales is null or sls_sales <= 0  or sls_sales != sls_quantity * abs(sls_price)
			then  sls_quantity * abs(sls_price)
		 else sls_sales
	end as sls_sales,

	sls_quantity,

	case when sls_price is null or sls_price <= 0
			then sls_sales / nullif(sls_quantity,0)
		else sls_price
	end as sls_price
	from bronze.crm_sales_details;
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	-------------------------
		SET @start_time = GETDATE();
	print '>> Truncating Table silver.erp_cust_az12';
	Truncate table silver.erp_cust_az12
	print'=========================';
	print '>> Inserting Data into: silver.erp_cust_az12';
	print'=========================';
	insert into silver.erp_cust_az12(
		cid,
		bdate,
		gen)

	select 
	case when cid like '%NAS%' then SUBSTRING(cid, 4, len(cid))
		else cid
	end as cid,
	case when bdate> GETDATE() then null
		else bdate
	end as bdate,
	case when trim(gen) in ('F', 'Female') then 'Female'
		 when trim(gen) in ('M', 'Male') then 'Male'
		 else 'N/A'
	end as gen
	from bronze.erp_cust_az12;
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	-------------------------------
		SET @start_time = GETDATE();
	print '>> Truncating Table silver.erp_loc_a101';
	Truncate table silver.erp_loc_a101
	print'=========================';
	print '>> Inserting Data into: silver.erp_loc_a101';
	print'=========================';

	insert into silver.erp_loc_a101(cid, cntry)

	select 
	replace(cid, '-', '') as cid,
	case when trim(cntry) ='DE' then 'Germany'
		 when trim(cntry) IN ('US', 'USA') then 'United States'
		 when trim(cntry) is null or cntry = '' then 'n/a'
		 else trim(cntry)
	end as cntry
	from bronze.erp_loc_a101;
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	---------------------

			SET @start_time = GETDATE();
	print '>> Truncating Table silver.erp_px_cat_g1v2';
	Truncate table silver.erp_px_cat_g1v2;
	print'=========================';
	print '>> Inserting Data into: silver.erp_px_cat_g1v2';
	print'=========================';
	insert into silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
	select id, cat, subcat, maintenance
	from bronze.erp_px_cat_g1v2;
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	print 'Loading Silver layer is completed';
	set @batch_end_time = GETDATE();
	print '-----------------------';
	print 'Total duration of Load: ' + cast(datediff(second, @batch_start_time, @batch_end_time)as nvarchar) + 'seconds';	
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
