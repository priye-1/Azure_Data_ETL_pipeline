-- Databricks notebook source
create table if not exists 
datavoweldb.Managed_employee(
  Employee_id INT comment "Store unique employee id", 
  First_name STRING comment "Store First name of employee",
  Last_name STRING,
  Gender STRING,
  salary INT,
  Date_of_Birth STRING,
  Age INT,
  Country STRING,
  Department_id INT,
  Date_of_Joining STRING,
  Manager_id INT,
  Currency STRING,
  End_Date STRING
)


-- COMMAND ----------

-- move data from unmanaged table into managed table

INSERT INTO datavoweldb.Managed_employee
SELECT Employee_id, 
  First_name,
  Last_name,
  Gender,
  salary,
  Date_of_Birth,
  Age,
  Country,
  Department_id,
  Date_of_Joining,
  Manager_id,
  Currency,
  End_Date
FROM datavoweldb.employee

-- COMMAND ----------

select * from datavoweldb.Managed_employee;

-- COMMAND ----------

describe formatted datavoweldb.Managed_employee

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/datavoweldb.db/managed_employee

-- COMMAND ----------

drop table datavoweldb.Managed_employee

-- COMMAND ----------


