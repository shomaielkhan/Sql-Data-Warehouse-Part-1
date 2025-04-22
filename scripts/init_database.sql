/*
---------------------------------------------
Create Database and Schemas
---------------------------------------------

Script Purpose:
	This script creates a new database 'DataWarehouse' after checking if its exists already.
	if the database exists, it is dropped or altered. Also, script set up three schemas within the
	database; 'bronze','silver' and 'gold'

Warning:
	Running this script will drop the entire database 'DataWarehouse' if it's exist

*/
use master;

if exists (select 1 from sys.databases where name='DataWarehouse')
begin
	alter database DataWarehouse set SINGLE_USER WITH rollback immediate;
	drop database DataWarehouse;
end;
go

create database DataWarehouse;
use DataWarehouse;

create schema bronze;
go
create schema silver;
go
create schema gold;