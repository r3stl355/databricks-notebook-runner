# Databricks notebook source
# MAGIC %md
# MAGIC### A sample Databricks notebook

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

df = spark.read.table("samples.nyctaxi.trips")
df.show(5)

# COMMAND ----------

# MAGIC %sql
# MAGICSELECT *
# MAGICFROM samples.nyctaxi.trips

# COMMAND ----------

# MAGIC %sql
# MAGIC