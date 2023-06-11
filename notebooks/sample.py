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

df = spark.read.table("samples.nyctaxi.trips")
df.show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM samples.nyctaxi.trips

# COMMAND ----------

# MAGIC %sql
# MAGIC 