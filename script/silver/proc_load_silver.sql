
/*
=================================================================
Stored Procedure Script: Load Silver Layer (Bronze -> Silver)
=================================================================
Script Purpose:
    This Stored procedure load data from an The Bronze Layer and . 
    It performs the following action: 
        * Turnicate the bronze table before loading.
        * Clean and  load Tables to the Silver Layer. 

=================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
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

        PRINT '>> Truncateing Table : silver.crm_cust_info'
        TRUNCATE TABLE silver.crm_cust_info
        PRINT 'Inserting table in to :silver.crm_cust_info '
        INSERT INTO  silver.crm_cust_info( 
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
        -- Trimming the First and lastnames 
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,

        -- Normalize  marital status to readable format and filling missing data with n/a 
        CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END cst_marital_status,
        -- Normalize gender information to readable format and filling missing data with n/a
        CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END cst_gndr,

        cst_create_date

        -- Removing duplicates and filtering from the bronze.crm_cust_info table. 
        FROM (
        SELECT 
        *,
        ROW_NUMBER() OVER( PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
        FROM bronze.crm_cust_info

        )t WHERE flag_last = 1;
        SET @end_time =  GETDATE();
        PRINT ' >> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
        PRINT '-----------------------';
        
        SET @start_time =  GETDATE();

        PRINT '>> Truncateing Table : silver.crm_prd_info'
        TRUNCATE TABLE silver.crm_prd_info
        PRINT 'Inserting table in to :silver.crm_prd_info '

        INSERT INTO silver.crm_prd_info(
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
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_' ) AS cat_id, -- Extracting catagory ID
            SUBSTRING(prd_key, 7, LEN(prd_key)) As prd_key, -- Extracting product key
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost, -- Handling missing information 
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,  -- Map product line codes to descriptive values
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            -- Calculate end date as one day before the next start date/ data enrichment 
            CAST( DATEADD(day, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) ) AS DATE) AS prd_end_dt_
            FROM bronze.crm_prd_info ;
        SET @end_time =  GETDATE();
        PRINT ' >> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
        PRINT '-----------------------';
        -- Silver : Layer Clean & Load 
        -- crm_sales_detail table
        SET @start_time =  GETDATE();

        PRINT '>> Truncateing Table : silver.crm_sales_details'
        TRUNCATE TABLE silver.crm_sales_details
        PRINT 'Inserting table in to :silver.crm_sales_details'
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
        SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
            ELSE CAST( CAST(sls_order_dt AS VARCHAR) AS DATE) 
        END
            AS sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        CASE WHEN sls_sales IS NULL OR sls_sales < =0 OR sls_sales != sls_quantity * ABS( sls_price )
            Then sls_quantity * ABS( sls_price )
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,

        CASE WHEN sls_price <= 0 OR sls_price IS NULL 
            Then sls_sales / NULLIF(sls_quantity,0)
            ELSE sls_price
        END AS sls_price


        FROM bronze.crm_sales_details;
        SET @end_time =  GETDATE();
            PRINT ' >> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
            PRINT '-----------------------';

            SET @start_time =  GETDATE();

            PRINT '>> Truncateing Table : silver.epr_loc_a101'
            TRUNCATE TABLE silver.epr_loc_a101
            PRINT 'Inserting table in to :silver.epr_loc_a101'
            INSERT INTO silver.epr_loc_a101(cid,cntry)
            SELECT
            REPLACE(cid, '-','') as cid,
            CASE WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN UPPER(TRIM(cntry))= '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
                END AS cntry
            FROM (
                SELECT cid,
                TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')) AS cntry
                FROM bronze.epr_loc_a101
            ) t;
            SET @end_time =  GETDATE();
        PRINT ' >> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
        PRINT '-----------------------';
        
        SET @start_time =  GETDATE();

            PRINT '>> TRUNCATEING TABLE : silver.epr_px_cat_g1v2 '
            TRUNCATE TABLE silver.epr_px_cat_g1v2;
            PRINT '>> Inserting to : silver.epr_px_cat_g1v2 '
            INSERT INTO silver.epr_px_cat_g1v2 (id,
            cat,
            subcat,
            maintenance
            )
            SELECT
            id,
            cat,
            subcat,
            maintenance
            FROM bronze.epr_px_cat_g1v2;
            SET @end_time =  GETDATE();
        PRINT ' >> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
        PRINT '-----------------------';
        
        SET @start_time =  GETDATE();
        PRINT '>> Truncateing Table : silver.epr_cust_az12'
        TRUNCATE TABLE silver.epr_cust_az12
        PRINT 'Inserting table in to :silver.epr_cust_az12'
        INSERT INTO silver.epr_cust_az12 ( cid, bdate, gen)
        SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid, 4, LEN(cid))
            ELSE cid 
        END AS cid,
        CASE WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
        END AS bdate,
        CASE 
            WHEN gen IS NULL OR LTRIM(RTRIM(gen)) = '' THEN 'n/a'
            WHEN UPPER(LTRIM(RTRIM(gen))) LIKE 'F%' THEN 'Female'
            WHEN UPPER(LTRIM(RTRIM(gen))) LIKE 'M%' THEN 'Male'
            ELSE 'n/a'
        END AS gen
        
        FROM bronze.epr_cust_az12
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

EXEC silver.load_silver
