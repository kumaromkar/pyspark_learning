# Databricks notebook source
spark

# COMMAND ----------

print(type(spark))

# COMMAND ----------

flight_df=spark.read.format("csv")\
    .option("header","false")\
    .option("inferschema","false")\
    .load("/FileStore/tables/2010_summary.csv")

flight_df.display()


# COMMAND ----------

flight_df_header=spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","false")\
    .load("/FileStore/tables/2010_summary.csv")

flight_df_header.display()


# COMMAND ----------

flight_df_header.printSchema()

# COMMAND ----------

flight_df_header_schema=spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .load("/FileStore/tables/2010_summary.csv")

flight_df_header_schema.display()


# COMMAND ----------

flight_df_header_schema.printSchema()
