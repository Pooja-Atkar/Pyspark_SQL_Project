import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from common.readutil import ReadDataUtil


if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]") \
        .appName("HR Data").getOrCreate()

    rdu = ReadDataUtil()

    # Read hrlocation csv file
    locationDF = rdu.readCsv(spark=spark,path=r"D:\PythonSparkProject\Pyspark_SQL_Project\Input_Files\hr.locations.csv")
    # locationDF.show()
    locationRDD = rdu.createRDD(spark=spark,path=r"D:\PythonSparkProject\Pyspark_SQL_Project\Input_Files\hr.locations.csv")

    countriesDF = rdu.readCsv(spark=spark,path=r"D:\PythonSparkProject\Pyspark_SQL_Project\Input_Files\hrcountries.csv")
    # countriesDF.show()
    countriesRDD = rdu.createRDD(spark=spark,path=r"D:\PythonSparkProject\Pyspark_SQL_Project\Input_Files\hrcountries.csv")

    departmentsDF = rdu.readCsv(spark=spark,path=r"D:\PythonSparkProject\Pyspark_SQL_Project\Input_Files\hrdepartments.csv")
    # departmentsDF.show()
    departmentsRDD = rdu.createRDD(spark=spark,path=r"D:\PythonSparkProject\Pyspark_SQL_Project\Input_Files\hrdepartments.csv")

    employeesDF = rdu.readCsv(spark=spark,path=r"D:\PythonSparkProject\Pyspark_SQL_Project\Input_Files\hremployees.csv")
    # employeesDF.show()
    employeesRDD = rdu.createRDD(spark=spark,path=r"D:\PythonSparkProject\Pyspark_SQL_Project\Input_Files\hremployees.csv")

    jobsDF = rdu.readCsv(spark=spark,path=r"D:\PythonSparkProject\Pyspark_SQL_Project\Input_Files\hrjobs.csv")
    # jobsDF.show()
    jobsRDD = rdu.createRDD(spark=spark,path=r"D:\PythonSparkProject\Pyspark_SQL_Project\Input_Files\hrjobs.csv")

    storyDF = rdu.readCsv(spark=spark,path=r"D:\PythonSparkProject\Pyspark_SQL_Project\Input_Files\jobhistory.csv")
    # storyDF.show()
    storyRDD = rdu.createRDD(spark=spark,path=r"D:\PythonSparkProject\Pyspark_SQL_Project\Input_Files\jobhistory.csv")

# Create Temp view
jobsDF.createTempView("jobs")
employeesDF.createTempView("employees")
# Queries

# 1. Display details of jobs where the minimum salary is greater than 10000.
# Using Sql
# df1 = spark.sql("select * from jobs where MIN_SALARY > 10000").show()
# Using DF
# df11 = jobsDF.select("*").filter("MIN_SALARY > 10000").show()

# 2. Display the first name and join date of the employees who joined between 2002 and 2005.

# 3. Display first name and join date of the employees who is either IT Programmer or Sales Man.
# Using Sql
# df3 = spark.sql("select FIRST_NAME,HIRE_DATE from employees where JOB_ID IN ('IT_PROG', 'SA_MAN')").show()
# Using DF
# df33 = employeesDF.select("FIRST_NAME","HIRE_DATE").filter("JOB_ID IN ('IT_PROG', 'SA_MAN')").show()

# 4. Display employees who joined after 1st January 2008.
# Using Sql
# df4 = spark.sql("select * from employees where HIRE_DATE > '01-01-2008'").show()
# Using DF
# df44 = employeesDF.select("*").filter("HIRE_DATE > '01-01-2008'").show()

# 5. Display details of employee with ID 150 or 160.
# Using Sql
# df5 = spark.sql("select * from employees where EMPLOYEE_ID in(150,160)").show()
# Using DF
# df55 = employeesDF.select("*").filter("EMPLOYEE_ID in(150,160)").show()

# 6. Display first name, salary, commission pct, and hire date for employees with salary less than 10000.
# Using Sql
# df6 = spark.sql("SELECT FIRST_NAME, SALARY, COMMISSION_PCT, HIRE_DATE FROM employees where SALARY < 10000").show()
# Using DF
# df66 = employeesDF.select("FIRST_NAME","SALARY", "COMMISSION_PCT", "HIRE_DATE").filter("SALARY > 10000").show()

# 7. Display job Title, the difference between minimum and maximum salaries for jobs with max salary in the range 10000 to 20000.
# Using Sql
# df7 = spark.sql("SELECT JOB_TITLE, MAX_SALARY-MIN_SALARY DIFFERENCE FROM jobs where MAX_SALARY between 10000 and 20000").show()

# 8. Display first name, salary, and round the salary to thousands.
# Using Sql
# df8 = spark.sql("select FIRST_NAME, SALARY, ROUND(SALARY, -3) from employees").show()
# Using DF
# df88 = employeesDF.select("FIRST_NAME", "SALARY",round("SALARY",-3)).show()

# 9. Display details of jobs in the descending order of the title.
# Using Sql
# df9 = spark.sql("select * from jobs order by JOB_TITLE").show()

# 10. Display employees where the first name or last name starts with S.
# Using Sql
# df10 = spark.sql("SELECT FIRST_NAME, LAST_NAME FROM employees WHERE FIRST_NAME LIKE 'S%' OR LAST_NAME LIKE 'S%'").show()
# Using DF
# df100 =employeesDF.select("FIRST_NAME", "LAST_NAME").filter("FIRST_NAME LIKE 'S%' OR LAST_NAME LIKE 'S%'").show()

# 11. Display employees who joined in the month of May.

# 12. Display details of the employees where commission percentage is null
# and salary in the range 5000 to 10000 and department is 30.
# Using Sql
# df12 = spark.sql("SELECT * FROM EMPLOYEES WHERE COMMISSION_PCT IS NULL AND SALARY BETWEEN 5000 AND 10000 AND DEPARTMENT_ID=30").show()
# Using DF
# df112 = employeesDF.select("*").filter("COMMISSION_PCT IS NULL AND SALARY BETWEEN 5000 AND 10000 AND DEPARTMENT_ID=30").show()

# 13. Display first name and date of first salary of the employees.
# Using Sql
# df13 = spark.sql("SELECT FIRST_NAME, HIRE_DATE, LAST_DAY(HIRE_DATE)+1 FROM EMPLOYEES").show()

# 14. Display first name and experience of the employees.

# 15. Display first name of employees who joined in 2001.

# 16. Display first name and last name after converting the first letter of each name to upper case and the rest to lower case.
# Using Sql
# df16 = spark.sql("SELECT INITCAP(FIRST_NAME), INITCAP(LAST_NAME) FROM EMPLOYEES").show()

# 17. Display the first word in job title.
# Using Sql
# df17 = spark.sql("SELECT JOB_TITLE, SUBSTR(JOB_TITLE,1, INSTR(JOB_TITLE, ' ')-1) FROM jobs").show()

# 18. Display the length of first name for employees where last name contain character ‘b’ after 3rd position.
# Using Sql
# df18 = spark.sql("SELECT FIRST_NAME, LAST_NAME FROM employees WHERE INSTR(LAST_NAME,'b') > 3").show()
# Using DF
# df118 = employeesDF.select("FIRST_NAME", "LAST_NAME").filter("INSTR(LAST_NAME,'b') > 3").show()+

# 19. Display first name in upper case and email address in lower case for employees where the first name and
# email address are same irrespective of the case.
# Using Sql
# df19 = spark.sql("SELECT UPPER(FIRST_NAME), LOWER(EMAIL) FROM employees WHERE UPPER(FIRST_NAME)= UPPER(EMAIL)").show()
