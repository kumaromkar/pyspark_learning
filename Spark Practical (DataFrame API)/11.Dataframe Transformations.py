# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

emp_df=spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/tables/emp_file.csv")

emp_df.display()

# COMMAND ----------

emp_df.printSchema()

# COMMAND ----------

#string method
emp_df.select("name","salary").display()

# COMMAND ----------

#colunm method
emp_df.select(col("name"),col("salary")).display()

# COMMAND ----------

emp_df.select("id + 5").display()

# COMMAND ----------

emp_df.select(col("id")+5).display()

# COMMAND ----------

## SELECT MULTIPLE COLUNMS

emp_df.select("id","name","age").display()

emp_df.select(col("id"),col("name"),col("age")).display()

# COMMAND ----------

##Multi Ways to select

emp_df.select("id",col("name"),emp_df["age"],emp_df.salary).display()

# COMMAND ----------

# MAGIC %md
# MAGIC EXPRESSION

# COMMAND ----------

emp_df.select(expr("id + 5")).display()

# COMMAND ----------

emp_df.select(expr("id + 5 as id"),expr("name as emp_name"),expr("concat(name,address)")).display()

# COMMAND ----------

emp_df.select("*").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Spark SQL

# COMMAND ----------

emp_df.createOrReplaceTempView("emp_tbl")

# COMMAND ----------

spark.sql('''

select * from emp_tbl


''').display()

# COMMAND ----------

spark.sql('''

select id,name from emp_tbl


''').display()
