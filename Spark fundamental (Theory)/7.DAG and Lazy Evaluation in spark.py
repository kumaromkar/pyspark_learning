# Databricks notebook source
from pyspark.sql.functions import col

df_flight_data=spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .load("/FileStore/tables/2010_summary.csv")


df_flight_data_repartition=df_flight_data.repartition(3)

us_df_flight_data=df_flight_data.filter(col('DEST_COUNTRY_NAME')=='United States')

us_ind_data=us_df_flight_data.filter((col("ORIGIN_COUNTRY_NAME")=='India') | (col("ORIGIN_COUNTRY_NAME")=='Singapore'))

total_flight_ind_sing=us_ind_data.groupBy("DEST_COUNTRY_NAME").sum("count")

total_flight_ind_sing.show()

# COMMAND ----------

df_flight_data=spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .load("/FileStore/tables/2010_summary.csv")

df_flight_data_repartition=df_flight_data.repartition(3)

us_df_flight_data=df_flight_data.filter(col('DEST_COUNTRY_NAME')=='United States')

us_ind_data=us_df_flight_data.filter((col("ORIGIN_COUNTRY_NAME")=='India') | (col("ORIGIN_COUNTRY_NAME")=='Singapore'))

total_flight_ind_sing=us_ind_data.groupBy("DEST_COUNTRY_NAME").sum("count")

total_flight_ind_sing.show()

# COMMAND ----------

df_flight_data.show(5)
