# Databricks notebook source
df=spark.read.parquet("/FileStore/tables/part_r_00000_1a9822ba_b8fb_4d8e_844a_ea30d0801b9e_gz.parquet")

# COMMAND ----------

df.display()
