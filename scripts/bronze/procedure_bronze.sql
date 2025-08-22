/*
  This Script executes the stored procedure which performs insertion of data from CSV files
  However it doesn't create new tables.

  To Execute: 
  EXEC bronze.load_bronze

*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN	
		DECLARE @layer_starttime DATETIME,@layer_endtime DATETIME;

		BEGIN TRY
      SET @layer_starttime= GETDATE();
			PRINT '=======================================================================================';
			PRINT 'Loading Bronze Layer';
			PRINT '=======================================================================================';

			print '=======================================================================================';
			print 'Loading CRM Tables';
			print '=======================================================================================';

			PRINT 'TRUNCATING TABLE: bronze.crm_cust_info'
			TRUNCATE TABLE bronze.crm_cust_info;
			PRINT 'INSERTING INTO :bronze.crm_cust_info'

			BULK INSERT bronze.crm_cust_info
			FROM 'C:\Backup\NOTES\SQL_PROJECTS\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
				FIELDTERMINATOR=',',
				ROWTERMINATOR='\n',
				FIRSTROW=2
			);

			PRINT 'TRUNCATING TABLE: bronze.crm_prd_info'
			TRUNCATE TABLE bronze.crm_prd_info;
			PRINT 'INSERTING TABLE: bronze.crm_prd_info'

			BULK INSERT bronze.crm_prd_info
			FROM 'C:\Backup\NOTES\SQL_PROJECTS\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
				FIELDTERMINATOR=',',
				ROWTERMINATOR='\n',
				FIRSTROW=2
			);

			PRINT 'TRUNCATING TABLE: bronze.crm_sales_details'
			TRUNCATE TABLE bronze.crm_sales_details;
			PRINT 'INSERTING TABLE: bronze.crm_sales_details'

			BULK INSERT bronze.crm_sales_details
			FROM 'C:\Backup\NOTES\SQL_PROJECTS\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
				FIELDTERMINATOR=',',
				ROWTERMINATOR='\n',
				FIRSTROW=2
			);

			print '=======================================================================================';
			print 'Loading ERP Tables';
			print '=======================================================================================';


			PRINT 'TRUNCATING TABLE: bronze.erp_cust_az12'
			TRUNCATE TABLE bronze.erp_cust_az12;
			PRINT 'INSERTING TABLE: bronze.erp_cust_az12'

		
			BULK INSERT bronze.erp_cust_az12
			FROM 'C:\Backup\NOTES\SQL_PROJECTS\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH (
				FIELDTERMINATOR=',',
				ROWTERMINATOR='\n',
				FIRSTROW=2
			);

	
			PRINT 'TRUNCATING TABLE: bronze.erp_loc_a101'
			TRUNCATE TABLE bronze.erp_loc_a101;
			PRINT 'INSERTING TABLE: bronze.erp_loc_a101'
		

			BULK INSERT bronze.erp_loc_a101
			FROM 'C:\Backup\NOTES\SQL_PROJECTS\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH (
				FIELDTERMINATOR=',',
				ROWTERMINATOR='\n',
				FIRSTROW=2
			);

			PRINT 'TRUNCATING TABLE: bronze.erp_px_cat_g1v2'
			TRUNCATE TABLE bronze.erp_px_cat_g1v2;
			PRINT 'INSERTING TABLE: bronze.erp_px_cat_g1v2'
		

			BULK INSERT bronze.erp_px_cat_g1v2
			FROM 'C:\Backup\NOTES\SQL_PROJECTS\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH (
				FIELDTERMINATOR=',',
				ROWTERMINATOR='\n',
				FIRSTROW=2
			);

			SET @layer_endtime=GETDATE();

			PRINT 'BRONZE LAYER IS LOADED'
			PRINT '>> Entire Duration For Loading Bronze Layer is '+ CAST(DATEDIFF(second,@layer_starttime,@layer_endtime) AS NVARCHAR) +' Seconds';
		END TRY
		BEGIN CATCH
			PRINT '========================================================================================='
			PRINT 'ERROR OCCURED' + ERROR_MESSAGE()
			PRINT 'ERROR OCCURED' + CAST(ERROR_NUMBER() AS NVARCHAR)
			PRINT 'ERROR OCCURED' + CAST(ERROR_STATE() AS NVARCHAR)
			PRINT '========================================================================================='
		END CATCH
		

END
