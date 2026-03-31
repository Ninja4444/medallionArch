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

# spark.sql("SHOW CATALOGS").show()

# spark.sql("SHOW DATABASES").show()

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

# COMMAND ----------

spark.sql('select count(*) from accenture.manish_bronze.orders').display()

# COMMAND ----------

spark.sql('truncate table accenture.manish_bronze.orders').display()

# COMMAND ----------

display(spark.sql('select * from accenture.manish_silver.orders'))


# COMMAND ----------

spark.sql("drop table if exists accenture.manish_gold.customer_lifetime_value").show()
