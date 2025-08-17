# Databricks notebook source
spark

# COMMAND ----------

df_flight_data=spark.read.format("csv")\
    .option("header","false")\
    .option("inferschema","false")\
    .option("mode","FAILFAST")\
    .load("/Volumes/workspace/default/sample_file_for_testing/2010-summary.csv")

# COMMAND ----------

df_flight_data.show()

df_flight_data.display()

# COMMAND ----------

df_flight_data_header=spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","false")\
    .load("/Volumes/workspace/default/sample_file_for_testing/2010-summary.csv")

df_flight_data_header.display()


df_flight_data_header.printSchema()

# COMMAND ----------

df_flight_data_header=spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .load("/Volumes/workspace/default/sample_file_for_testing/2010-summary.csv")

df_flight_data_header.display()


df_flight_data_header.printSchema()


# COMMAND ----------

df1=spark.read.csv("/Volumes/workspace/default/sample_file_for_testing/2010-summary.csv")

# COMMAND ----------

df1.display()

# COMMAND ----------

df_csv = spark.read.csv("/Volumes/workspace/default/sample_file_for_testing/2010-summary.csv", header=True, inferSchema=True)

# COMMAND ----------

df_csv.display()
