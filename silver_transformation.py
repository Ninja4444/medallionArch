# Databricks notebook source
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, trim, lower, to_timestamp, row_number, sha2, concat_ws
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable


class SilverTransformer:
    """Production Silver layer transformation pipeline."""

    def __init__(self, spark: SparkSession, entity_name: str):
        self.spark = spark
        self.entity_name = entity_name
        self.source_table = f"accenture.manish_bronze.{entity_name}"
        self.target_table = f"accenture.manish_silver.{entity_name}"

    def read_bronze_incremental(
        self, last_processed_ts: str | None = None
    ) -> DataFrame:
        """Read new records from Bronze since last processing."""
        df = self.spark.table(self.source_table)

        if last_processed_ts:
            df = df.filter(
                col("_bronze_ingested_at") > last_processed_ts
            )

        return df.filter(col("_corrupt_record").isNull()) \
                 .drop("_corrupt_record")

    def deduplicate(
        self, df: DataFrame, key_columns: list[str],
        order_column: str = "_bronze_ingested_at"
    ) -> DataFrame:
        """Deduplicate using key columns, keeping latest record."""
        window = Window.partitionBy(
            *key_columns
        ).orderBy(col(order_column).desc())

        return df.withColumn("_row_num", row_number().over(window)) \
                 .filter(col("_row_num") == 1) \
                 .drop("_row_num")

    def apply_schema(
        self, df: DataFrame, column_types: dict[str, str]
    ) -> DataFrame:
        """Cast columns to target types with error handling."""
        for col_name, target_type in column_types.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast(target_type))
        return df

    def add_silver_metadata(self, df: DataFrame) -> DataFrame:
        """Add Silver-layer tracking columns."""
        from pyspark.sql.functions import current_timestamp, lit

        key_cols = [c for c in df.columns if not c.startswith("_")]
        return df \
            .withColumn("_silver_processed_at", current_timestamp()) \
            .withColumn(
                "_silver_hash",
                sha2(concat_ws("||", *[col(c) for c in key_cols]), 256)
            )

    def merge_into_silver(
        self, source_df: DataFrame, key_columns: list[str]
    ):
        """SCD Type 1 merge into Silver table."""
        if not self.spark.catalog.tableExists(self.target_table):
            source_df.write.format("delta").saveAsTable(self.target_table)
            return

        target = DeltaTable.forName(self.spark, self.target_table)
        merge_condition = " AND ".join(
            [f"target.{k} = source.{k}" for k in key_columns]
        )

        target.alias("target") \
            .merge(source_df.alias("source"), merge_condition) \
            .whenMatchedUpdateAll(
                condition="source._silver_hash != target._silver_hash"
            ) \
            .whenNotMatchedInsertAll() \
            .execute()
