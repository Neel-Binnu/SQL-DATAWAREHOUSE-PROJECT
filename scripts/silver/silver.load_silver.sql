/*
This script creates a stored procedure for silver layer schema 
where it first truncates the tables and then insert the cleaned/processed/organized data from the bronze layer.
For execution : EXEC silver.load_silver
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	TRUNCATE TABLE silver.crm_cust_info;
	PRINT'--------------------------------crm_cust_info table is Truncating-------------------------------------------'
	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gender,
		cst_create_date 
	)


	select 
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		CASE 
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END AS cst_marital_status,
		CASE 
			WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
			ELSE 'n/a'
		END AS cst_gender,
		CONVERT(VARCHAR(10), cst_create_date, 120) AS cst_create_date
		FROM (
			SELECT *,
				row_number() over(partition by cst_id order by cst_create_date DESC) flag

			FROM bronze.crm_cust_info
			where cst_id is not null
		)t
	where flag=1 ; 
	PRINT'--------------------------------crm_cust_info table is Loaded-------------------------------------------'
	PRINT'================================================================================================='


	TRUNCATE TABLE silver.crm_prd_info;
	PRINT'--------------------------------crm_prd_info table is Truncating-------------------------------------------'
	INSERT INTO silver.crm_prd_info(
	
		prd_id,
		cat_key,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt

	)
	select 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_key,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line)) 
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 AS DATE) as prd_end_dt
	FROM bronze.crm_prd_info
	PRINT'--------------------------------crm_prd_info table is Loaded-------------------------------------------'
	PRINT'================================================================================================='


	TRUNCATE TABLE silver.crm_sales_details
	PRINT'--------------------------------crm_sales_details table is Truncating-------------------------------------------'
	INSERT INTO silver.crm_sales_details(
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		sls_order_dt,
		sls_ship_dt ,
		sls_due_dt ,
		sls_sales ,
		sls_quantity ,
		sls_price 
	)

	select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE 
			WHEN sls_order_dt=0 or LEN(sls_order_dt) !=8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
		END sls_order_dt,
		CASE 
			WHEN sls_ship_dt=0 or LEN(sls_ship_dt) !=8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
		END sls_ship_dt,
		CASE 
			WHEN sls_due_dt=0 or LEN(sls_due_dt) !=8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
		END sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price

	from bronze.crm_sales_details
	PRINT'--------------------------------crm_sales_details table is Loaded-------------------------------------------'
	PRINT'================================================================================================='


	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT'--------------------------------erp_cust_az12 table is Truncating-------------------------------------------'
	INSERT INTO silver.erp_cust_az12(
		CID,
		BDATE,
		GENDER
	)

	select 

	CASE 
		WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
		ELSE CID
	END AS CID,
	CASE 
		WHEN BDATE > GETDATE() THEN NULL
		ELSE BDATE
	END AS BDATE,

	CASE 
		WHEN UPPER(TRIM(GENDER)) in ('M','MALE') THEN 'Male'
		WHEN UPPER(TRIM(GENDER)) in ('F','FEMALE') THEN 'Female'
		ELSE 'n/a'
	END AS GENDER
	from bronze.erp_cust_az12 
	PRINT'--------------------------------erp_cust_az12 table  is Loaded-------------------------------------------'
	PRINT'================================================================================================='


	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT'--------------------------------erp_loc_a101 table is Truncating-------------------------------------------'
	INSERT INTO silver.erp_loc_a101(
	CID,
	COUNTRY
	)
	select 
	REPLACE(CID,'-','') as CID,
	CASE
		WHEN trim(country)='DE' THEN 'Germany'
		WHEN trim(country) IN ('US','USA') THEN 'United States'
		WHEN trim(country) = '' OR country IS NULL  then 'n/a'
		ELSE trim(country)
	END Country
	from bronze.erp_loc_a101
	PRINT'--------------------------------erp_loc_a101 table is Loaded-------------------------------------------'
	PRINT'================================================================================================='

	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT'--------------------------------erp_px_cat_g1v2 table is Truncating-------------------------------------------'
	INSERT INTO silver.erp_px_cat_g1v2(
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
	)

	select 
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
	from bronze.erp_px_cat_g1v2
	PRINT'--------------------------------erp_px_cat_g1v2 table is Loaded-------------------------------------------'
	PRINT'================================================================================================='


END




