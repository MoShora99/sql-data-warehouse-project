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
create or alter procedure silver.load_silver as
BEGIN
 DECLARE @start_time datetime,@end_time datetime,@Batch_start_time datetime,@Batch_end_time datetime;
BEGIN TRY
	set @Batch_start_time=getdate();
	PRINT '================================================';
	PRINT 'Loading Silver Layer';
	PRINT '================================================';

	PRINT '------------------------------------------------';
	PRINT 'Loading CRM Tables';
	PRINT '------------------------------------------------';
	set @start_time=getdate();
	print'>>> truncating table : silver.crm_cust_info';
	truncate table silver.crm_cust_info;
	print'>>> inserting table : silver.crm_cust_info';
	insert into silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_gndr,cst_marital_status,cst_create_date)

	select cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,

	CASE WHEN upper(trim(cst_gndr))='F' THEN 'Female'
	when upper(trim(cst_gndr))='M' THEN 'Male'
	else 'n/a' end as  cst_gndr,

	CASE WHEN upper(trim(cst_marital_status))='S' THEN 'Single'
	when upper(trim(cst_marital_status))='M' THEN 'Married'
	else 'n/a' end as  cst_marital_status,
	cst_create_date
	from 
	(select *,
	ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag 
	from bronze.crm_cust_info where cst_id is not null)t WHERE flag=1  ;
	set @end_time=getdate();
	print'>> Load Duration: '+cast(datediff(second,@start_time,@end_time) as NVARCHAR)+' seconds';
	PRINT '>> -------------';


	set @start_time=getdate();
	print'>>> truncating table : silver.crm_prd_info';
	truncate table silver.crm_prd_info;
	print'>>> inserting table : silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
	select 
	prd_id,

	replace(SUBSTRING(prd_key,1,5),'-','_') as  cat_id,
	SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
	prd_nm,
	ISNULL(prd_cost,0) AS prd_cost,

	CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
		 WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
		 ELSE 'n/a' END  prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(CASE WHEN prd_start_dt>prd_end_dt THEN
	lead (prd_start_dt-1) over(partition by prd_key order by  prd_start_dt ) 
	ELSE prd_end_dt END AS DATE) prd_end_dt
	from bronze.crm_prd_info ;
	set @end_time=getdate();
	print'>> Load Duration: '+cast(datediff(second,@start_time,@end_time) as NVARCHAR)+' seconds';
	PRINT '>> -------------';


	set @start_time=getdate();
	print'>>> truncating table : silver.crm_sales_details';
	truncate table silver.crm_sales_details;
	print'>>> inserting table : silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details(
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

	CASE WHEN sls_order_dt =0 OR LEN(sls_order_dt) !=8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS nvarchar) AS DATE) END AS sls_order_dt,

	CASE WHEN sls_ship_dt =0 OR LEN(sls_ship_dt) !=8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS nvarchar) AS DATE) END AS sls_ship_dt,

	CASE WHEN sls_due_dt =0 OR LEN(sls_due_dt) !=8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS nvarchar) AS DATE) END AS sls_due_dt,

	CASE WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity* ABS(sls_price) THEN 
	sls_quantity* ABS(sls_price) 
	ELSE sls_sales END AS sls_sales,
	sls_quantity,
	CASE WHEN sls_price <=0 OR sls_price IS NULL THEN
	sls_sales/NULLIF(sls_quantity,0) 
	ELSE sls_price END AS sls_price

	from bronze.crm_sales_details;
	set @end_time=getdate();
	print'>> Load Duration: '+cast(datediff(second,@start_time,@end_time) as NVARCHAR)+' seconds';
	PRINT '>> -------------';


	set @start_time=getdate();
	print'>>> truncating table : silver.erp_cust_az12';
	truncate table silver.erp_cust_az12;
	print'>>> inserting table : silver.erp_cust_az12';
	insert into silver.erp_cust_az12 (CID,bdate,GEN)
	SELECT 
	CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
	ELSE CID END AS CID,
	CASE WHEN bdate > GETDATE() THEN NULL
	ELSE bdate END AS bdate,
	CASE WHEN UPPER(TRIM(GEN)) in ('F','FEMALE') THEN 'Female'
		 when UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
		 else 'n/a' end  as GEN

	FROM bronze.erp_cust_az12 ;
	set @end_time=getdate();
	print'>> Load Duration: '+cast(datediff(second,@start_time,@end_time) as NVARCHAR)+' seconds';
	PRINT '>> -------------';

	set @start_time=getdate();
	print'>>> truncating table : silver.erp_loc_a101';
	truncate table silver.erp_loc_a101;
	print'>>> inserting table : silver.erp_loc_a101';
	insert into silver.erp_loc_a101(cid,cntry)

	select 
	REPLACE(cid,'-','') AS cid,
	CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
		 WHEN  TRIM(cntry)IN ('US','USA') THEN 'United States'
		 WHEN TRIM(cntry) = TRIM('') OR TRIM(cntry) IS NULL THEN 'n/a'
		 else TRIM(cntry) end as cntry
	from bronze.erp_loc_a101;
	set @end_time=getdate();
	print'>> Load Duration: '+cast(datediff(second,@start_time,@end_time) as NVARCHAR)+' seconds';
	PRINT '>> -------------';

	set @start_time=getdate();
	print'>>> truncating table : silver.erp_px_cat_g1v2';
	truncate table silver.erp_px_cat_g1v2;
	print'>>> inserting table : silver.erp_px_cat_g1v2';
	insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
	select * from bronze.erp_px_cat_g1v2;
	set @end_time=getdate();
	print'>> Load Duration: '+cast(datediff(second,@start_time,@end_time) as NVARCHAR)+' seconds';
	PRINT '>> -------------';

	SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
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
