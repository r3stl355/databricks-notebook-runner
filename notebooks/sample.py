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
# MAGIC
# MAGIC pwd
# MAGIC ls -la
# MAGIC python --version

# COMMAND ----------

print("hello")

# COMMAND ----------

df = spark.read.table("samples.nyctaxi.trips")
df.show(5)

# COMMAND ----------

# MAGIC %sql
# MAGICSELECT sum(trip_distance)
# MAGICFROM samples.nyctaxi.trips

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

df = spark.createDataFrame(rows)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("hive_metastore.default.bubu")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGICSELECT * FROM hive_metastore.default.bubu
