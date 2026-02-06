-- SILVER LAYER (cleaning & transforming)

USE DataWareHouse;


-- creating the silver version tables
-- CRM

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
    cst_id int,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date Date,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);


CREATE TABLE silver.crm_prd_info(
    prd_id int,
    prd_key NVARCHAR(50),
    cat_id NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost int,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);


CREATE TABLE silver.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id int,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales int,
    sls_quantity int,
    sls_price int,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);


-- erp
CREATE TABLE silver.erp_cust_az12(
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);


CREATE TABLE silver.erp_loc_a101(
    cid NVARCHAR(50),
    cntry NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);


CREATE TABLE silver.erp_px_cat_g1v2(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- QUALITY CHECK

-- CLEAN & LOAD
-- WORKING ON crm_cust_info

-- just checking columns to create primary key 
select * from bronze.crm_cust_info;

SELECT cst_id, COUNT(*) FROM bronze.crm_cust_info GROUP BY cst_id HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- finally figured out that the latest information to be considered based on the creation date cst_create_date.
select * from bronze.crm_cust_info where cst_id = 29466;

-- writing code to rank

SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info WHERE cst_id = 29466


-- code to  get the data where the cst_id is not null and just non repetatives (just getting the values which created recently.)
SELECT * FROM (
SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info 
)t WHERE flag_last = 1 ;

-- 2. removing the unwanted spaces (cst_firstname, cst_lastname)
-- 3. Data standardization & consistency if (F -> Female, M -> Male)
-- if (S -> Single, M -> Married).
-- 4. Insert into silver.crm_cust_info

INSERT INTO silver.crm_cust_info(
  cst_id,
  cst_key,
  cst_firstname,
  cst_lastname,
  cst_marital_status,
  cst_gndr,
  cst_create_date)

SELECT 
cst_id,
cst_key,
Trim (cst_firstname) AS cst_firstname,
Trim (cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
     ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
     ELSE 'n/a'
END cst_gndr,
cst_create_date FROM (
SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info 
)t WHERE flag_last = 1 ;


-- WORKING ON crm_prd_info

-- 1. in px_cat_g1v2 the id is AC_BR so cat_id will be changed to that format and cat_id is extracted
-- 1.1. extract prd_key the last 10 chars because here the prd_key is in crm_sales_details
-- 2. there are null values in prd_cost so that will be replaced with 0
-- 3. now the full forms of the R,M,S,T is given in prd_line.
-- 4. in the start and end date there are null values and some end dates looks like start date vice versa for some records.
-- thought of many solutions but i think that best one would be start date will be unchanged and end date 
-- will be changed to next record start date - 1 day AS end date.
-- 5. insert the values.

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
REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
     WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
     WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
     WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
     ELSE 'n/a'
END AS prd_line,
CAST(prd_Start_dt AS DATE) AS prd_start_dt,
CAST(DATEADD(day,-1,LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;


-- WORKING ON crm_sales_details

-- 1. fixing the date as it is int
-- 2. sls * quantity = price so correct the sls becsuse it has 0s, nulls
-- 3. change the sls_ship_dt, sls_due_dt, sls_order_dt int to date format
-- 4. check the sales price and quantity because sales * quantity = price 

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
     ELSE CAST(CAST(sls_order_dt AS varchar) AS DATE)
END sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
     ELSE CAST(CAST(sls_ship_dt AS varchar) AS DATE)
END sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
     ELSE CAST(CAST(sls_due_dt AS varchar) AS DATE)
END sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
     THEN sls_quantity * ABS(sls_price)
     ELSE sls_sales
END sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
     ELSE sls_price
END sls_price
FROM bronze.crm_sales_details;

-- END OF CRM CLEANING

-- BEGINING OF ERP CLEANING
-- erp_cust_az12
-- 1. cid -> cst_key (crm_cust_info) so AW00 is left and NAS will be stripped from NASAW000...
-- 2. bdate should be less than todays date.

INSERT INTO silver.erp_cust_az12(cid, bdate, gen)

SELECT

CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
     ELSE cid
END AS cid,
CASE WHEN  bdate > GETDATE() THEN NULL
     ELSE bdate
END bdate,
CASE WHEN UPPER(TRIM(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''))) IN ('M', 'MALE') THEN 'Male'
     WHEN UPPER(TRIM(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''))) IN ('F', 'FEMALE') THEN 'Female'
     ELSE gen
END gen
FROM bronze.erp_cust_az12;


-- erp_loc_a101

-- 1. cst_key is AW000 & cid is AW-000 that has to be changed.
-- 2. cntry to be changed 
-- 3. here i am using mac so M + char(13) or char(10) is used so i have to remove that


INSERT INTO silver.erp_loc_a101(cid, cntry)

SELECT
REPLACE(cid, '-','')cid,
CASE WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')) = 'DE' THEN 'Germany'
     WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')) IN ('US', 'USA') THEN 'United States'
     WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')) = '' OR cntry IS NULL THEN 'n/a'
     ELSE TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''))
END cntry
FROM bronze.erp_loc_a101;

-- erp_px_cat_g1v2

-- the data is very good and has good quality

INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)

SELECT id,cat,subcat,
TRIM(REPLACE(REPLACE(maintenance, CHAR(13), ''), CHAR(10), '')) FROM bronze.erp_px_cat_g1v2;



