/*
-----------------------------
DDL Script: Create Silver tables
-------------------------------------------
Script Purpose:
	This script creates table for the bronze schema, dropping existing tables if they already exist.
---------------------------------------------

*/
select * from silver.crm_cust_info;

if object_id ('silver.crm_cust_info', 'U') is not null
	drop table silver.crm_cust_info 
create table silver.crm_cust_info (
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(100),
	cst_lastname nvarchar(100),
	cst_marital_status  NVARCHAR(50),
	cst_gndr nvarchar(50),
	cst_create_date date,
	dwh_create_date datetime2 default getdate()
);
if object_id ('silver.crm_prd_info', 'U') is not null
	drop table silver.crm_prd_info 
create table silver.crm_prd_info (
	prd_id int,
	prd_key nvarchar(50),
	cat_id nvarchar(50),
	prd_nm nvarchar(100),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_date date,
	prd_end_date date,
	dwh_create_date datetime2 default getdate()

);

if object_id ('silver.crm_sales_details', 'U') is not null
	drop table silver.crm_sales_details
create table silver.crm_sales_details (
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_date int,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date datetime2 default getdate()


);

if object_id ('silver.erp_cust_az12', 'U') is not null
	drop table silver.erp_cust_az12
create table silver.erp_cust_az12(
	cid nvarchar(50),
	bdate date,
	gen nvarchar(10),
);
if object_id ('silver.erp_loc_a101', 'U') is not null
	drop table silver.erp_loc_a101
create table silver.erp_loc_a101(
	cid nvarchar(50),
	cntry nvarchar(50),
	dwh_create_date datetime2 default getdate()

);


if object_id ('silver.erp_px_cat_g1v2', 'U') is not null
	drop table silver.erp_px_cat_g1v2
create table silver.erp_px_cat_g1v2(
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintenance nvarchar(50),
	dwh_create_date datetime2 default getdate()

