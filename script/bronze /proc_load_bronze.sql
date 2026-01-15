
/*
=================================================================
Stored Procedure Script: Load Bronze Layer (Source -> Bronze)
=================================================================
Script Purpose:
    This Stored procedure load data from an external CSV file. 
    It performs the following action: 
        * Turnicate the bronze table before loading.
        * Uses Bulk insert to load csv file to bronze table. 
Paramenters:
    This Stored procedure does not accept any parameters or returnes any valure. 

=================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    SET @batch_start_time = GETDATE();
    BEGIN TRY
        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================================';
        PRINT '------------------------------------------------';
        PRINT 'Loading CRM TABLES';
        PRINT '------------------------------------------------';
        SET @start_time =  GETDATE();

        PRINT '>> TRUNCATING TABLE: bronze.crm_cust_info'
        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/datasets/source_crm/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time =  GETDATE();
        PRINT ' >> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
        PRINT '-----------------------';

        SET @start_time =  GETDATE();

        PRINT '>> TRUNCATING TABLE: bronze.crm_sales_details'
        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/datasets/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time =  GETDATE();
        PRINT ' >> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
        PRINT '-----------------------';

         SET @start_time =  GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.crm_prd_info'
        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/datasets/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 3,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time =  GETDATE();
        PRINT ' >> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
        PRINT '-----------------------';


        PRINT '------------------------------------------------';
        PRINT 'Loading ERP TABLES';
        PRINT '------------------------------------------------';

        SET @start_time =  GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.epr_cust_az12'

        TRUNCATE TABLE bronze.epr_cust_az12;

        BULK INSERT bronze.epr_cust_az12
        FROM'/var/opt/mssql/datasets/source_erp/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time =  GETDATE();
        PRINT ' >> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
        PRINT '-----------------------';
        
        PRINT '>> TRUNCATING TABLE: bronze.epr_loc_a101'

        SET @start_time =  GETDATE();
        TRUNCATE TABLE bronze.epr_loc_a101

        BULK INSERT bronze.epr_loc_a101
        FROM'/var/opt/mssql/datasets/source_erp/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        SET @end_time =  GETDATE();
        PRINT ' >> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
        PRINT '-----------------------';
        
        SET @start_time =  GETDATE();
        PRINT '>> TRUNCATING TABLE:  bronze.epr_px_cat_g1v2'
        TRUNCATE TABLE  bronze.epr_px_cat_g1v2

        BULK INSERT bronze.epr_px_cat_g1v2
        FROM'/var/opt/mssql/datasets/source_erp/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time =  GETDATE();
        PRINT ' >> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
        PRINT '-----------------------';
        
    END TRY
    BEGIN CATCH
    PRINT '==============================================';
    PRINT 'ERROR OCCURED DURING LOADING BRONZE LAUER'
    PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
    PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
    PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR); 
    PRINT '==============================================';
    END CATCH
    SET @batch_end_time = GETDATE();
    PRINT ' >> Batch load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'Seconds';
END


EXEC bronze.load_bronze
