# Databricks notebook source
spark

# COMMAND ----------

print(spark.version)

# COMMAND ----------

data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17),
(18 ,'Sam',55000,   17),
(18 ,'Sam',65000,   17)]

schema=['id','name','sal','mngr_id']

manager_df=spark.createDataFrame(data,schema)
manager_df.display()

# COMMAND ----------

manager_df.count()

# COMMAND ----------

data1=[(19 ,'Sohan',50000, 18),
(20 ,'Sima',75000,  17),
(18 ,'Sam',65000,   17)]

schema1=['id','name','sal','mngr_id']

manager_df1=spark.createDataFrame(data1,schema1)

manager_df1.display()

# COMMAND ----------

manager_df.union(manager_df1).display()

manager_df.union(manager_df1).count()

# COMMAND ----------

manager_df.unionAll(manager_df1).display()

manager_df.unionAll(manager_df1).count()

# COMMAND ----------

manager_df.unionAll(manager_df1).display()

# COMMAND ----------

manager_df.createOrReplaceTempView("manager_tbl")
manager_df1.createOrReplaceTempView("manager_tbl1")


# COMMAND ----------

spark.sql('''

select * from manager_tbl

union

select * from manager_tbl1
          
''').display()

# COMMAND ----------

spark.sql('''

select * from manager_tbl

union all

select * from manager_tbl1
          
''').display()

# COMMAND ----------

spark.sql('''

select * from manager_tbl

union

select * from manager_tbl
          
''').display()

# COMMAND ----------

wrong_column_data=[(19 ,50000, 18,'Sohan'),
(20 ,75000,  17,'Sima')]


wrong_column_schema=['id','sal','mngr_id','name']

wrong_column_df=spark.createDataFrame(wrong_column_data,wrong_column_schema)

# COMMAND ----------

manager_df1.printSchema()
wrong_column_df.printSchema()

# COMMAND ----------

manager_df1.union(wrong_column_df).show()

# COMMAND ----------

manager_df1.unionByName(wrong_column_df).display()

# COMMAND ----------

wrong_column_data=[(19 ,50000, 18,'Sohan',10),
(20 ,75000,  17,'Sima',20)]

wrong_column_schema=['id','sal','mngr_id','name','bonus']

wrong_column_df=spark.createDataFrame(wrong_column_data,wrong_column_schema)

# COMMAND ----------

wrong_column_df.printSchema()
manager_df1.printSchema()

# COMMAND ----------

wrong_column_df.select('id','name','sal','mngr_id').union(manager_df1).display()

# COMMAND ----------

wrong_column_data2=[(19 ,50000, 18,'Sohan'),
(20 ,75000,  17,'Sima')]


wrong_column_schema2=['id','sal','mngr_id','nam']

wrong_column_df2=spark.createDataFrame(wrong_column_data2,wrong_column_schema2)

# COMMAND ----------

wrong_column_df.unionByName(wrong_column_df2).display()
