-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType,StructField, StringType, IntegerType
-- MAGIC
-- MAGIC custom_schema = StructType(
-- MAGIC     [
-- MAGIC         StructField("Employee_id", IntegerType(),True),
-- MAGIC         StructField("First_name", StringType(),True),
-- MAGIC         StructField("Last_name", StringType(),True),
-- MAGIC         StructField("Gender", StringType(), True),
-- MAGIC         StructField("salary", IntegerType(), True),
-- MAGIC         StructField("Date_of_Birth", StringType(),True),
-- MAGIC         StructField("Age", IntegerType(),True),
-- MAGIC         StructField("Country", StringType(),True),
-- MAGIC         StructField("Department_id", IntegerType(), True),
-- MAGIC         StructField("Date_of_Joining", StringType(), True),
-- MAGIC         StructField("Manager_id", IntegerType(), True),
-- MAGIC         StructField("Currency", StringType(), True),
-- MAGIC         StructField("End_Date", StringType(), True),
-- MAGIC     ]
-- MAGIC )  
-- MAGIC df = spark.read.format("csv")\
-- MAGIC .option("header", "true")\
-- MAGIC .schema(custom_schema)\
-- MAGIC .load("/mnt/files/Employee.csv")
-- MAGIC
-- MAGIC df.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #create temporary view
-- MAGIC df.createOrReplaceTempView("ViewEMployee")

-- COMMAND ----------

select * from viewEMployee where Department_id == 1

-- COMMAND ----------

select * from viewEMployee where Department_id > 1 and Department_id < 10

-- COMMAND ----------

select * from viewEMployee where Department_id between 1 and 10

-- COMMAND ----------

select * from viewEMployee where First_name = 'Ugo'

-- COMMAND ----------

select * from viewEMployee where Department_id is Null

-- COMMAND ----------

select * from viewEMployee where Department_id is not Null

-- COMMAND ----------

select Employee_id, First_name, Last_name, Gender, First_name as NewName from viewEMployee 

-- COMMAND ----------

select * from viewEMployee where First_name like '%a'

-- COMMAND ----------

select cast(Employee_id as string), First_name, Last_name, Gender, First_name as NewName from viewEMployee 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Aggregation in SQL 

-- COMMAND ----------

select min(Department_id), avg(Department_id), max(Department_id) from viewEMployee

-- COMMAND ----------

select count(Department_id) from viewEMployee

-- COMMAND ----------

select count(Distinct Department_id) from viewEMployee

-- COMMAND ----------

select sum(DISTINCT Department_id) from viewEMployee

-- COMMAND ----------

select Department_id, 
count(distinct Employee_id) as Number_of_employee
from viewEMployee
where department_id is not null
group by department_id
order by department_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # WINDOW FUNC IN SQL

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.window import Window
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import StructType,StructField, StringType, IntegerType
-- MAGIC
-- MAGIC custom_schema = StructType(
-- MAGIC     [
-- MAGIC         StructField("Country", StringType(),True),
-- MAGIC         StructField("Gender", StringType(), True),
-- MAGIC         StructField("Employee_id", IntegerType(),True),
-- MAGIC         StructField("First_name", StringType(),True),      
-- MAGIC         StructField("Salary", IntegerType(), True),        
-- MAGIC         StructField("Department_id", IntegerType(), True)
-- MAGIC     ]
-- MAGIC )  
-- MAGIC df = spark.read.format("csv")\
-- MAGIC .option("header", "true")\
-- MAGIC .schema(custom_schema)\
-- MAGIC .load("/mnt/files/SampleEmployee.csv")
-- MAGIC
-- MAGIC df.createOrReplaceTempView("ViewEmployee")

-- COMMAND ----------

select country, max(salary) 
from viewEmployee
group by country

-- COMMAND ----------

select Country,
Gender,
Employee_id,
First_name,
Salary,
Department_id,
max(Salary) over (partition by Country) as max_sal,
min(Salary) over (partition by Country) as min_sal
from viewEmployee
group by
Country,
Gender,
Employee_id,
First_name,
Salary,
Department_id

-- COMMAND ----------

select Country,
Gender,
Employee_id,
First_name,
Salary,
Department_id,
max(Salary) over (partition by Country) as max_sal,
min(Salary) over (partition by Country) as min_sal,
row_number() over (partition by Country order by salary desc) as row_num
from viewEmployee
group by
Country,
Gender,
Employee_id,
First_name,
Salary,
Department_id

-- COMMAND ----------

select * from(
  select Country,
  Gender,
  Employee_id,
  First_name,
  Salary,
  Department_id,
  max(Salary) over (partition by Country) as max_sal,
  min(Salary) over (partition by Country) as min_sal,
  row_number() over (partition by Country order by salary desc) as row_num
  from viewEmployee
  )
  where row_num == 3

-- COMMAND ----------

select Country,
Gender,
Employee_id,
First_name,
Salary,
Department_id,
max(Salary) over (partition by Country) as max_sal,
min(Salary) over (partition by Country) as min_sal,
rank() over (partition by Country order by Gender) as rank,
dense_rank() over (partition by Country order by Gender) as dense_rank
from viewEmployee
group by
Country,
Gender,
Employee_id,
First_name,
Salary,
Department_id

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### joins in sql

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_emp = spark.read.format("csv")\
-- MAGIC     .options(header='true', delimeter=',')\
-- MAGIC     .load("/mnt/files/Sample_data4jOIN.csv")
-- MAGIC
-- MAGIC df_dep = spark.read.format("csv")\
-- MAGIC     .options(header='true', delimeter=',')\
-- MAGIC     .load("/mnt/files/Department.csv")
-- MAGIC     
-- MAGIC df_emp.createOrReplaceTempView("emp")
-- MAGIC df_dep.createOrReplaceTempView("dept")

-- COMMAND ----------

select emp.Employee_id, emp.Department_id, dept.Department_id from emp LEFT JOIN dept on emp.Department_id = dept.Department_id

-- COMMAND ----------

select emp.Employee_id, emp.First_name, emp.Department_id, dept.Department_id from emp INNER JOIN dept on emp.Department_id = dept.Department_id

-- COMMAND ----------

select emp.Employee_id, emp.First_name, emp.Department_id, dept.Department_id from emp LEFT JOIN dept on emp.Department_id = dept.Department_id

-- COMMAND ----------

select emp.Employee_id, emp.First_name, emp.Department_id, dept.Department_id from emp FULL JOIN dept on emp.Department_id = dept.Department_id

-- COMMAND ----------



-- COMMAND ----------


