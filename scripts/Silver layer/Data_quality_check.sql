select * from bronze.crm_cust_info;

--Removing Duplicates

select prd_id,
count(*)
from silver.crm_prd_info
group by prd_id
having count (*)>1 or prd_id is null;

--Check for unwanted spaces--
select prd_nm
from silver.crm_prd_info
where prd_cost <0 or prd_cost is null

select prd_line
from silver.crm_prd_info
where  prd_line != trim(prd_line)

--Data Standardization & Consistentcy

select distinct prd_line from silver.crm_prd_info

--Check for Invalid Date Orders--

select  * from silver.crm_prd_info
where prd_end_date< prd_start_date

------------------------

--Check for Invalid Dates--

select  
NULLIF(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 or len(sls_order_dt) != 8
or sls_order_dt > 20120603	or sls_order_dt < 19000101

	select  
	NULLIF(sls_ship_dt,0) sls_ship_dt
	from bronze.crm_sales_details
	where sls_ship_dt <= 0 or len(sls_ship_dt) != 8
or sls_ship_dt > 20120603 or sls_ship_dt < 19000101

select * 
from bronze.crm_sales_details
	where sls_order_dt >sls_ship_dt

--Data Consistency--

select distinct sls_sales, sls_quantity, sls_price
from silver.crm_sales_details

where sls_sales is null or sls_price is null or sls_quantity is null
or sls_sales <= 0  or sls_price <=0  or sls_quantity <= 0
order by sls_sales, sls_quantity, sls_price

select distinct sls_sales,
sls_quantity, 
sls_price
from silver.crm_sales_details
select * from silver.crm_sales_details

-----------------------

select * from bronze.crm_cust_info;

--Removing Duplicates

select prd_id,
count(*)
from silver.crm_prd_info
group by prd_id
having count (*)>1 or prd_id is null;

--Check for unwanted spaces--
select prd_nm
from silver.crm_prd_info
where prd_cost <0 or prd_cost is null

select prd_line
from silver.crm_prd_info
where  prd_line != trim(prd_line)

--Data Standardization & Consistentcy

select distinct prd_line from silver.crm_prd_info

--Check for Invalid Date Orders--

select  * from silver.crm_prd_info
where prd_end_date< prd_start_date
