# Databricks notebook source
df_csv = spark.read.csv("/FileStore/tables/2010_summary.csv", header=True, inferSchema=True)

#will get AnalysisException error
df_csv.select("name").show()

# COMMAND ----------

spark
