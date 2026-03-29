# Databricks notebook source
# MAGIC %%sql
# MAGIC CREATE CATALOG IF NOT EXISTS prod;

# COMMAND ----------

# MAGIC %%sql
# MAGIC show catalogs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS accenture.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS accenture.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS accenture.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop schema if exists accenture.bronze;

# COMMAND ----------

# Check if you can create schemas in demo catalog
spark.sql("CREATE SCHEMA IF NOT EXISTS accenture.manish_bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS accenture.manish_silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS accenture.manish_gold")

# COMMAND ----------

# # Step 1: Check available catalogs
# spark.sql("SHOW CATALOGS").show()

# # Step 2: Check available schemas
# spark.sql("SHOW DATABASES").show()

# Step 3: Check existing DBFS mounts
spark.sql("SELECT current_user()").show()

#spark.sql("SHOW TABLES").show()#

# COMMAND ----------

# Check catalogs available to manish.b.gautam
spark.sql("SHOW CATALOGS").show(truncate=False)

# Check schemas
spark.sql("SHOW SCHEMAS").show(truncate=False)

# COMMAND ----------

https://enb-accenture.cloud.databricks.com/files/ManishGautam/medallionArch/orders/2026-03-10/batch_1774782264_2.json

# COMMAND ----------

dbutils.fs.mkdirs ('dbfs:/FileStore/ManishGautam/medallionArch/orders/2026-03-10/')
