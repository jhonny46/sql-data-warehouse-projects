
-- Checking to see if there are any duplicates cst_ids or Null IDs
SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1  OR cst_id IS NULL


-- Check for unwanted spaces
--  Expectation:  No results
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Data Standardization & Consistency
SELECT 
DISTINCT  cst_gndr
FROM silver.crm_cust_info

Select * From silver.crm_cust_info


 -- ____________________________
-- Checking for unwanted spaces 
-- Expecration no results

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for nulls or negetive numbers
-- Expectation : No result
SELECT  prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL;

-- Data Standardization & Consistency

SELECT 
distinct prd_line
FROM silver.crm_prd_info

-- Check for Invalid Date Orders
SELECT * 
FROM silver.crm_prd_info
WHERE prd_end_dt  <  prd_start_dt

-- ---------------------------------
-- Silver : Check for crm_sales_detail table


SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price

FROM silver.crm_sales_details
-- Check for spaces in ordur num
-- WHERE sls_ord_num != TRIM(sls_ord_num) 

-- Check for prdkey in crm prd info table  / Checks out 
-- WHERE sls_prd_key NOT IN (Select prd_key FROM silver.crm_prd_info )
WHERE sls_cust_id NOT IN (Select cst_id FROM silver.crm_cust_info ) --  Checkes out 


SELECT 
NULLIF(sls_order_dt ,0)

FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 OR sls_order_dt < 19000101 OR sls_order_dt > 20500101; -- Change 0 in to nulls AND CHECK FOR INVALID DATES

-- Check shiping date 

SELECT 
    NULLIF(sls_ship_dt, '1900-01-01')
FROM silver.crm_sales_details
WHERE sls_ship_dt =' 0' OR LEN(sls_ship_dt) != 8;  -- Change 0 in to nulls AND CHECK FOR INVALID DATES


SELECT DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'crm_sales_details'
  AND COLUMN_NAME = 'sls_ship_dt';

-- Every thing looks good for sls_ship_dt /  Use the same query for sls_due_dt
SELECT *
FROM (
    SELECT
        TRY_CONVERT(date, sls_ship_dt, 112) AS sls_ship_dt_clean,
        CASE
            WHEN sls_ship_dt IS NULL THEN 'NULL'
            WHEN LEN(sls_ship_dt) <> 10 THEN 'INVALID_LENGTH' -- 10 for counting the strings '-' b/n the yy-mm-dd
            WHEN TRY_CONVERT(date, sls_ship_dt, 112) IS NULL THEN 'INVALID_DATE'
            ELSE 'VALID'
        END AS date_status
    FROM silver.crm_sales_details
) t
WHERE t.date_status IN ('INVALID_LENGTH', 'INVALID_DATE');

-- CHECKING SALES QUANITY AND PRICE 
-- SALES MUST NE EQUAL TO QUANTITY * PRICE 
-- SALES MUST NOT BE NEGATIVE, 0 OR NULLS
SELECT
sls_sales AS ols_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales < =0 OR sls_sales != sls_quantity * ABS( sls_price )
    Then sls_quantity * ABS( sls_price )
    ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price <= 0 OR sls_price IS NULL 
    Then sls_sales / NULLIF(sls_quantity,0)
    ELSE sls_price
END AS sls_price

FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL or sls_quantity is NULL OR sls_price is NULL
OR sls_sales <=0 OR sls_quantity <= 0 OR sls_price <= 0

ORDER BY sls_sales, sls_quantity, sls_price

SELECT * FROM silver.crm_sales_details

-- ________________________________________

