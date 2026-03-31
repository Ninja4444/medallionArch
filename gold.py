# Databricks notebook source
from pyspark.sql.functions import (
    sum as spark_sum, count, avg, max as spark_max,
    min as spark_min, datediff, current_date, countDistinct
)

from pyspark.sql import DataFrame, SparkSession


def build_customer_lifetime_value(spark: SparkSession) -> DataFrame:
    """Gold table: Customer Lifetime Value metrics."""

    orders = spark.table("accenture.manish_silver.orders")

    clv = orders.groupBy("category").agg(
        spark_sum("amount").alias("total_revenue"),
        count("transaction_id").alias("total_transactions"),
        avg("amount").alias("avg_value_by_category"),
        spark_min("timestamp").alias("first_order_date"),
        spark_max("timestamp").alias("last_order_date"),
        countDistinct("user_id").alias("unique_customers")
    )

    # Write as Gold table
    clv.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("accenture.manish_gold.goldAggTable_orders ")

    # Optimize for query performance
    spark.sql("""
        OPTIMIZE accenture.manish_gold.goldAggTable_orders
        ZORDER BY (category)
    """)

    return clv


# Execute the function to build the gold table
# result = build_customer_lifetime_value(spark)
# display(result)

# COMMAND ----------



# COMMAND ----------

def build_daily_revenue(spark: SparkSession) -> DataFrame:
    """Gold table: Daily revenue aggregations."""

    orders = spark.table("prod.silver.orders")

    daily = orders \
        .join(products, on="product_id", how="left") \
        .groupBy("order_date", "product_category", "region") \
        .agg(
            spark_sum("total_amount").alias("revenue"),
            count("order_id").alias("order_count"),
            avg("total_amount").alias("avg_order_value"),
            spark_sum("quantity").alias("units_sold"),
        )

    daily.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("order_date") \
        .saveAsTable("prod.gold.daily_revenue")

    return daily

# COMMAND ----------

display(spark.sql("select * from accenture.manish_gold.goldAggTable_orders").limit(2))
