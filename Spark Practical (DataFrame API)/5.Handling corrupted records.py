# Databricks notebook source
df_emp_data=spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/tables/emp_file.csv")

df_emp_data.display()

# COMMAND ----------

df_emp_data=spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","DROPMALFORMED")\
    .load("/FileStore/tables/emp_file.csv")

df_emp_data.display()

# COMMAND ----------

df_emp_data=spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","FAILFAST")\
    .load("/FileStore/tables/emp_file.csv")

df_emp_data.display()

# COMMAND ----------

from pyspark.sql.types import StringType,IntegerType,StructField,StructType

# COMMAND ----------

emp_schema=StructType(
    [
        StructField("id",IntegerType(),True),
        StructField("name",StringType(),True),
        StructField("age",IntegerType(),True),
        StructField("salary",IntegerType(),True),
        StructField("address",StringType(),True),
        StructField("nominee",StringType(),True),
        StructField("_corrupt_record",StringType(),True),
    ]
)

df_emp_data=spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .schema(emp_schema)\
    .load("/FileStore/tables/emp_file.csv")

df_emp_data.display()

# COMMAND ----------

df_emp_data=spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .schema(emp_schema)\
    .option("badRecordsPath","/FileStore/tables/bad_records/")\
    .load("/FileStore/tables/emp_file.csv")

df_emp_data.display()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/bad_records/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/

# COMMAND ----------

bad_data_df=spark.read.format("json").load("/FileStore/tables/bad_records/20250815T091404/bad_records/part-00000-fb4cd9ad-6ed0-4960-9cc9-01dec298bdd1")

bad_data_df.show(truncate=False)
