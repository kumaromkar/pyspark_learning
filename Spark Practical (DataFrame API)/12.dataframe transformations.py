# Databricks notebook source
####### Import ########
from pyspark.sql.types import *
from pyspark.sql.functions import *

###### Reading Data ######
emp_df=spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/tables/emp_file.csv")

emp_df.display()

###### printing schema ######
emp_df.printSchema()

###### creating temp view #####
emp_df.createOrReplaceTempView("emp_tbl")

# COMMAND ----------

emp_df.select(col("id").alias("emp_id"),"name","age").display()

# COMMAND ----------

emp_df.filter(col("salary") >= 150000).display()

# COMMAND ----------

emp_df.where(col("salary") >= 150000).display()

# COMMAND ----------

emp_df.filter((col("salary") >= 150000) & (col("age") < 18 )).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Literal

# COMMAND ----------

emp_df.select("*",lit("kumar").alias("last_name")).display()

# COMMAND ----------

emp_df.withColumn("sur_name",lit("singh")).display()

# COMMAND ----------

emp_df.withColumnRenamed("id","emp_id").display()

# COMMAND ----------

new_emp_df=emp_df.withColumnRenamed("id","emp_id")

# COMMAND ----------

new_emp_df.display()

# COMMAND ----------

emp_df.printSchema()

# COMMAND ----------

emp_df.withColumn("id",col("id").cast("string"))\
    .withColumn("salary",col("salary").cast("long"))\
    .printSchema()


# COMMAND ----------

emp_df.drop("id",col("name")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC SPARK SQL

# COMMAND ----------

spark.sql('''
select * from emp_tbl where salary>150000 and age<18
''').display()

# COMMAND ----------

spark.sql('''
select * ,'kumar' AS last_name from emp_tbl where salary>150000 and age<18
''').display()

# COMMAND ----------

spark.sql('''
select * ,'kumar' AS last_name from emp_tbl where salary>150000 and age<18
''').display()

# COMMAND ----------

spark.sql('''
select * ,'kumar' AS last_name,concat(name,last_name) as full_name from emp_tbl where salary>150000 and age<18
''').display()

# COMMAND ----------

spark.sql('''
select * ,'kumar' AS last_name,concat(name,last_name) as full_name,id as emp_id from emp_tbl where salary>150000 and age<18
''').display()

# COMMAND ----------

spark.sql('''
select * ,'kumar' AS last_name,concat(name,last_name) as full_name,cast(id as string) from emp_tbl
where salary>150000 and age<18
''').display()
