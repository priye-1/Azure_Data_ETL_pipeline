-- Databricks notebook source
SELECT * FROM 
(SELECT
E.department_id
,Name
,employee_id
,ROW_NUMBER() OVER(PARTITION BY E.department_id ORDER BY salary DESC )rw
,salary
FROM dvdb.employee E 
LEFT JOIN
dvdb.department d
ON E.department_id = d.Department_id
WHERE E.department_id IS NOT NULL AND d.name IS NOT NULL) WHERE rw < 6

-- COMMAND ----------

SELECT
CASE 

    WHEN country = "Australia" THEN "AUS"  ----- ISO 3166-1 alpha-3 code
    WHEN country = "Brazil" THEN "BRA"     ----- ISO 3166-1 alpha-3 code
    WHEN country = "Canada" THEN "CAN"     ----- ISO 3166-1 alpha-3 code
    WHEN country = "China" THEN "CHN"      ----- ISO 3166-1 alpha-3 code
    WHEN country = "Denmark" THEN "DNK"    ----- ISO 3166-1 alpha-3 code
    WHEN country = "Germany" THEN "DEU"    ----- ISO 3166-1 alpha-3 code
    WHEN country = "India" THEN "IND"      ----- ISO 3166-1 alpha-3 code
    WHEN country = "Japan" THEN "JPN"      ----- ISO 3166-1 alpha-3 code
    WHEN country = "Sweden" THEN "SWE"     ----- ISO 3166-1 alpha-3 code
    WHEN country = "UAE" THEN "ARE"        ----- ISO 3166-1 alpha-3 code
    WHEN country = "USA" THEN "USA"        ----- ISO 3166-1 alpha-3 code
  END AS code
 , SUM(salary)/1000000 AS cost_of_department_in_million
FROM dvdb.employee E 
LEFT JOIN
dvdb.department d
ON E.department_id = d.Department_id
WHERE Name IS NOT NULL
GROUP BY 
code

-- COMMAND ----------

SELECT
Name,Country
,SUM(salary)/1000000 AS cost_of_department_in_million
,CASE 
    WHEN country = "Australia" THEN "AUD" 
    WHEN country = "Brazil" THEN "BRA" 
    WHEN country = "Canada" THEN "CAN" 
    WHEN country = "China" THEN "CHN" 
    WHEN country = "Denmark" THEN "DNK" 
    WHEN country = "Germany" THEN "DEU" 
    WHEN country = "India" THEN "IND" 
    WHEN country = "Japan" THEN "JPN" 
    WHEN country = "Sweden" THEN "SWE" 
    WHEN country = "UAE" THEN "ARE" 
    WHEN country = "USA" THEN "USA" 
  END AS code
FROM dvdb.employee E 
LEFT JOIN
dvdb.department d
ON E.department_id = d.Department_id
WHERE Name IS NOT NULL
GROUP BY 
Name,Country

-- COMMAND ----------

SELECT
Name,Country
,SUM(salary)/1000000 AS cost_of_department_in_million
FROM dvdb.employee E 
LEFT JOIN
dvdb.department d
ON E.department_id = d.Department_id
WHERE Name IS NOT NULL
GROUP BY 
Name,Country

-- COMMAND ----------

SELECT D.Name, 
         AVG(salary) AS avg_salary,
         Min(salary) AS min_salary,
         MAX(salary) AS max_salary 
  FROM dvdb.employee E  
  LEFT JOIN dvdb.department D on E.Department_id = D.Department_id WHERE D.Name IS NOT NULL
  GROUP BY D.Name

-- COMMAND ----------


