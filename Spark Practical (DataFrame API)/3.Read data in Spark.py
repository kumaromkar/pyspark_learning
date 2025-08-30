# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

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

# COMMAND ----------

#Project-K learning
data_df=spark.read.option('delimiter',',').csv('/Volumes/workspace/default/sample_file_for_testing/2010-summary.csv')
data_df.display()

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.default.projectk")
spark.sql("CREATE TABLE workspace.default.projectk (DEST_COUNTRY_NAME string,ORIGIN_COUNTRY_NAME string,count integer) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")

spark.sql("LOAD DATA LOCAL INPATH '/Volumes/workspace/default/sample_file_for_testing/2010-summary.csv' INTO TABLE workspace.default.projectk")
