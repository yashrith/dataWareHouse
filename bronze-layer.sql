-- BRONZE LAYER (creating & inserting)

-- Create Database 'DataWareHouse'

Use master;

CREATE DATABASE DataWareHouse;

USE DataWareHouse;

-- CREATE SCHEMAS

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO


-- CREATE tables
-- crm

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
    cst_id int,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date Date
);


CREATE TABLE bronze.crm_prd_info(
    prd_id int,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost int,
    prd_line NVARCHAR(10),
    prd_start_dt DATE,
    prd_end_dt DATE
);

CREATE TABLE bronze.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id int,
    sls_order_dt int,
    sls_ship_dt int,
    sls_due_dt int,
    sls_sales int,
    sls_quantity int,
    sls_price int
);

-- erp
CREATE TABLE bronze.erp_cust_az12(
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
);


CREATE TABLE bronze.erp_loc_a101(
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);


CREATE TABLE bronze.erp_px_cat_g1v2(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);

-- inserting the values

-- CREATE OR ALTER PROCEDURE bronze.load_bronze AS BEGIN .... END

-- crm

BULK INSERT bronze.crm_cust_info FROM '/datasets/source_crm/cust_info.csv'
WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
select * from bronze.crm_cust_info;

BULK INSERT bronze.crm_prd_info FROM '/datasets/source_crm/prd_info.csv'
WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK 
)
SELECT * FROM bronze.crm_prd_info;

BULK INSERT bronze.crm_sales_details FROM '/datasets/source_crm/sales_details.csv'
WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
)
SELECT * FROM bronze.crm_sales_details;

-- erp

BULK INSERT bronze.erp_cust_az12 FROM '/datasets/source_erp/CUST_AZ12.csv'
WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
)
SELECT * FROM bronze.erp_cust_az12;

BULK INSERT bronze.erp_loc_a101 FROM '/datasets/source_erp/loc_a101.csv'
WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
)
SELECT * FROM bronze.erp_loc_a101;

BULK INSERT bronze.erp_px_cat_g1v2 FROM '/datasets/source_erp/px_cat_g1v2.csv'
WITH(
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
)
DECLARE @start DATETIME , @end DATETIME;
set @start = GETDATE();
SELECT * FROM bronze.erp_px_cat_g1v2;
set @end = GETDATE();
print'>> load duration: ' + CAST(DATEDIFF(second, @start, @end) AS NVARCHAR) + ' seconds';