# Databricks notebook source
my_data=[(1,1),   
(2,1),   
(3,1),   
(4,2),   
(5,1),   
(6,2),   
(7,2)]

# COMMAND ----------

my_schema=['id','num']

# COMMAND ----------

df=spark.createDataFrame(data=my_data,schema=my_schema)

# COMMAND ----------

df.display()
