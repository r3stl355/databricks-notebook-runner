# Databricks notebook source
# MAGIC %md
# MAGIC ### A sample Databricks Notebook

# COMMAND ----------

print("A sample Databricks Notebook")

# COMMAND ----------

import pyjokes
pyjokes.get_joke()

# COMMAND ----------

# MAGIC %pip install pyjokes

# COMMAND ----------

import pyjokes
pyjokes.get_joke()

# COMMAND ----------

# MAGIC %sh pyjoke

# COMMAND ----------

# MAGIC %run ./setup

# COMMAND ----------

print(rows)

# COMMAND ----------

# MAGIC %sh
# MAGIC pip uninstall -y pyjokes

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

df = spark.read.table("samples.nyctaxi.trips")
df.show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(trip_distance)
# MAGIC FROM samples.nyctaxi.trips

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

rows

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

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in samples.nyctaxi