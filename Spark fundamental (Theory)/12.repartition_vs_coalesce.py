# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df_flight_data=spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .load("/FileStore/tables/2010_summary.csv")

df_flight_data.show()

# COMMAND ----------

df_flight_data.count()

# COMMAND ----------

df_flight_data.rdd.getNumPartitions()

# COMMAND ----------

partitioned_df_flight_data=df_flight_data.repartition(4)

# COMMAND ----------

partitioned_df_flight_data.display()

# COMMAND ----------

df_partitionedID=partitioned_df_flight_data.withColumn("partitionId",spark_partition_id())

# COMMAND ----------

df_groupby_part=df_partitionedID.groupBy("partitionId").count()

# COMMAND ----------

df_groupby_part.show()

# COMMAND ----------

partitioned_on_col=df_flight_data.repartition(300,"ORIGIN_COUNTRY_NAME")

# COMMAND ----------

partitioned_on_col.rdd.getNumPartitions()

# COMMAND ----------

partitioned_on_col.withColumn("partitionId",spark_partition_id()).groupBy("partitionId").count().display()

# COMMAND ----------

coalesce_df_flight_data=df_flight_data.repartition(8)

# COMMAND ----------

coalesce_df_flight_data.withColumn("partitionId",spark_partition_id()).groupBy("partitionId").count().display()

# COMMAND ----------

three_coalesce_df=coalesce_df_flight_data.coalesce(3)

# COMMAND ----------

three_coalesce_df.withColumn("partitionId",spark_partition_id()).groupBy("partitionId").count().display()

# COMMAND ----------

three_repartition_df=coalesce_df_flight_data.repartition(3)
three_repartition_df.withColumn("partitionId",spark_partition_id()).groupBy("partitionId").count().display()

# COMMAND ----------

coalesce_df_flight_data.coalesce(10).rdd.getNumPartitions()
