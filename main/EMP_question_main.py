from pyspark.sql import SparkSession
from common.readutil import ReadDataUtil
from pyspark.sql.types import *

if __name__ == '__main__':

    spark = SparkSession.builder.appName("Employee Data").master("local[*]").getOrCreate()

    rdu = ReadDataUtil()

    empDF = rdu.readCsv(spark=spark,path=r"D:\PythonSparkProject\Pyspark_SQL_Project\emp_data_files\emp_data.csv")
    # empDF.show()

    deptDF = rdu.readCsv(spark=spark,path=r"D:\PythonSparkProject\Pyspark_SQL_Project\emp_data_files\dept_data.csv")
    # deptDF.show()

    # Create Temp view

    empDF.createTempView("emp")
    deptDF.createTempView("dept")

    # df = spark.sql("select * from dept").show()

    # 1. Display all the details from Emp table.
    # Using Sql
    # df1 = spark.sql("select * from emp").show()
    # Using DF
    # df11 = empDF.select("*").show()

    # 2. Select the employees in department 30.
    # Using Sql
    # df2 = spark.sql("select * from emp where deptNo=30").show()
    # Using DF
    # df22 = empDF.select("*").filter("deptno=30").show()

    # 3. List the names, numbers and departments of all clerks.
    # Using Sql
    # df3 = spark.sql("select Empno,Ename,deptNo from emp where Job='CLERK'").show()
    # Using DF
    # df33 = empDF.select("empno","ename","deptno").filter("job = 'CLERK'").show()

    # 4. Find the department numbers and names of employees of all departments with deptno greater than 20.
    # Using Sql
    # df4 = spark.sql("select d.deptno,Ename,dname from emp e inner join dept d on(e.deptno=d.deptno) where d.deptno > 20").show()
    # df44 =spark.sql("select deptno,Ename from emp where deptno > 20").show()
    # Using DF
    # df444 = empDF.select("deptno","ename").filter("deptno > 20").show()


    # 5. Find employees whose commission is greater than their salaries.
    # Using Sql
    # df5 = spark.sql("select * from emp where comm > salary").show()
    # Using DF
    # df55 = empDF.select("*").filter("comm > salary").show()

