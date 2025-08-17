# Databricks notebook source
df=spark.read.csv("/FileStore/tables/emp_dtl.csv",header=True,inferSchema=True)
df.display()

# COMMAND ----------

df_write=df.write.format("csv")\
        .option("header","true")\
        .option("mode","overwrite")\
        .option("path","/FileStore/tables/patition_by_address/")\
        .partitionBy("address")\
        .save()
            

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/patition_by_address/

# COMMAND ----------

df_write=df.write.format("csv")\
        .option("header","true")\
        .option("mode","overwrite")\
        .option("path","/FileStore/tables/partition_by_id/")\
        .partitionBy("id")\
        .save()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/partition_by_id/

# COMMAND ----------

df_write=df.write.format("csv")\
        .option("header","true")\
        .option("mode","overwrite")\
        .option("path","/FileStore/tables/partition_by_address_gender/")\
        .partitionBy("address","gender")\
        .save()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/partition_by_address_gender/address=INDIA/gender=m/

# COMMAND ----------

df_write=df.write.format("csv")\
        .option("header","true")\
        .option("mode","overwrite")\
        .option("path","/FileStore/tables/bucket_by_id/")\
        .bucketBy(3,"id")\
        .saveAsTable("bucket_by_id_table")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/bucket_by_id/
# MAGIC
