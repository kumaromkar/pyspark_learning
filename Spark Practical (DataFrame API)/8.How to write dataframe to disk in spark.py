# Databricks notebook source
df=spark.read.csv("/FileStore/tables/emp_dtl.csv",header=True,inferSchema=True)
df.display()

# COMMAND ----------

df.write.format("csv")\
    .option("header","true")\
    .option("mode","overwrite")\
    .option("path","/FileStore/tables/csv_write")\
    .save()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/csv_write")

# COMMAND ----------

df.repartition(3).write.format("csv")\
    .option("header","true")\
    .option("mode","overwrite")\
    .option("path","/FileStore/tables/csv_write_repart")\
    .save()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/csv_write_repart")

# COMMAND ----------

df = df.repartition(3).write.format("csv")\
    .option("header", "True")\
    .mode("overwrite")\
    .option("path", "/FileStore/tables/csv_write_repart/")\
    .save()
