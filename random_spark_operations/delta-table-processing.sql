-- Databricks notebook source
-- MAGIC %python
-- MAGIC  
-- MAGIC df = spark.read.format("csv")\
-- MAGIC .option("header", "true")\
-- MAGIC .option("inferSchema", "false")\
-- MAGIC .option("sep", ",")\
-- MAGIC .load("/mnt/files/Employee.csv")
-- MAGIC
-- MAGIC display(df)
-- MAGIC temp_table_name = "emp_csv"
-- MAGIC df.createOrReplaceTempView(temp_table_name)

-- COMMAND ----------

select count(*) from emp_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC  
-- MAGIC df = spark.read.format("csv")\
-- MAGIC .option("header", "true")\
-- MAGIC .option("inferSchema", "false")\
-- MAGIC .option("sep", ",")\
-- MAGIC .load("/mnt/files/upsert_data.csv")
-- MAGIC
-- MAGIC display(df)
-- MAGIC temp_table_name = "upd_csv"
-- MAGIC df.createOrReplaceTempView(temp_table_name)

-- COMMAND ----------

-- update emp with new ids in upd table by creating database 
-- CREATE DATABASE IF NOT EXISTS DVDB;
-- USE DVDB



-- COMMAND ----------

-- create delta table
DROP TABLE IF EXISTS EMPLOYEE_DELTA_TABLE;

CREATE TABLE EMPLOYEE_DELTA_TABLE
USING delta
PARTITIONED BY(DEPARTMENT_ID)
LOCATION "/UDEMY/DELTA/EMP_DELTA_DATA"
AS(
  SELECT * FROM emp_csv WHERE DEPARTMENT_ID IS NOT NULL
)

-- COMMAND ----------

DESCRIBE DETAIL EMPLOYEE_DELTA_TABLE

-- COMMAND ----------

SELECT COUNT(*) FROM EMPLOYEE_DELTA_TABLE

-- COMMAND ----------

SELECT * FROM EMPLOYEE_DELTA_TABLE where employee_id = 12

-- COMMAND ----------

-- To note; in employee_delta_table 7 rows will be updated and 10 new rows will be created making a sum of 998 rows
MERGE INTO EMPLOYEE_DELTA_TABLE
USING upd_csv

ON EMPLOYEE_DELTA_TABLE.employee_id = upd_csv.employee_id

WHEN MATCHED THEN
  UPDATE SET
  EMPLOYEE_DELTA_TABLE.Last_Name = upd_csv.Last_Name
WHEN NOT MATCHED THEN 
  INSERT(Employee_id, First_Name, Last_Name, Gender, Salary, Date_of_Birth, Age, Country, Department_id, Date_of_joining, Manager_id, Currency, End_Date)
  VALUES(Employee_id, First_Name, Last_Name, Gender, Salary, Date_of_Birth, Age, Country, Department_id, Date_of_joining, Manager_id, Currency, End_Date)

-- COMMAND ----------

select count(*) from employee_delta_table

-- COMMAND ----------

SELECT * FROM EMPLOYEE_DELTA_TABLE VERSION AS OF 0

-- COMMAND ----------

DESCRIBE HISTORY EMPLOYEE_DELTA_TABLE;

-- COMMAND ----------

SELECT * FROM EMPLOYEE_DELTA_TABLE WHERE employee_id in (21, 35, 45, 47, 49)

-- COMMAND ----------

UPDATE dvdb.EMPLOYEE_DELTA_TABLE SET First_Name = Null WHERE employee_id in (21, 35, 45, 47, 49)

-- COMMAND ----------

select * from EMPLOYEE_DELTA_TABLE

-- COMMAND ----------

VACUUM dvdb.EMPLOYEE_DELTA_TABLE RETAIN 0 HOURS
