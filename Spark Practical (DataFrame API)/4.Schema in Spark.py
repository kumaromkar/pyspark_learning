# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

my_schema=StructType(
    [
        StructField("DEST_COUNTRY_NAME",StringType(),True),
        StructField("ORG_COUNTRY_NAME",StringType(),True),
        StructField("COUNT",IntegerType(),True),
    ]
)


df_flight_data=spark.read.format("csv")\
    .option("header","false")\
    .option("skipRows",1)\
    .option("inferschema","false")\
    .option("mode","FAILFAST")\
    .schema(my_schema)\
    .load("/FileStore/tables/2010_summary.csv")

# COMMAND ----------

df_flight_data.display()

# COMMAND ----------

df_flight_data.printSchema()
