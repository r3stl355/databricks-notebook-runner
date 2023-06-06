# Databricks notebook source
# MAGIC %md
# MAGIC ### A sample Databricks notebook

# COMMAND ----------

print("A sample Databricks Notebook")

# COMMAND ----------

# MAGIC %run
# MAGIC ./setup.py

# COMMAND ----------

print(rows)

# COMMAND ----------

df = spark.read.table("samples.nyctaxi.trips")
df.show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM samples.nyctaxi.trips 