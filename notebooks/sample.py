# Databricks notebook source
# MAGIC %md
# MAGIC ### A sample Databricks notebook

# COMMAND ----------

print("A sample Databricks Notebook")

# COMMAND ----------

# MAGIC %run ./setup

# COMMAND ----------

print(rows)

# COMMAND ----------

# MAGIC %sh
# MAGIC pip list

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd
# MAGIC python --version

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(trip_distance)
# MAGIC FROM samples.nyctaxi.trips

# COMMAND ----------

df = spark.read.table("samples.nyctaxi.trips")
df.show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(trip_distance)
# MAGIC FROM samples.nyctaxi.trips

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

df = spark.createDataFrame(rows)

# COMMAND ----------

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS hive_metastore.default.bubu;

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("hive_metastore.default.bubu")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.default.bubu

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS hive_metastore.default.bubu

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.default.bubu