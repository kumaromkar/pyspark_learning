# Databricks notebook source
df_json=spark.read.format("json")\
    .option("inferSchema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/tables/line_delimited_json.json")

df_json.display()

# COMMAND ----------

df_json=spark.read.format("json")\
    .option("inferSchema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/tables/single_file_json.json")

df_json.display()

# COMMAND ----------

df_json=spark.read.format("json")\
    .option("inferSchema","true")\
    .option("mode","PERMISSIVE")\
    .option("multiline","true")\
    .load("/FileStore/tables/Multi_line_correct.json")

df_json.display()

# COMMAND ----------

df_json=spark.read.format("json")\
    .option("inferSchema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/tables/corrupted_json.json")

df_json.display()

# COMMAND ----------

df_json=spark.read.format("json")\
    .option("inferSchema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/tables/resturant_json_data.json")

df_json.display()



# COMMAND ----------

df_json.printSchema()
