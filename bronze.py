# Databricks notebook source
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    current_timestamp, input_file_name, lit, col)
from pyspark.sql.types import StructType, StringType
from delta.tables import DeltaTable


class BronzeIngester:
    """Production Bronze layer ingestion with metadata tracking."""

    def __init__(self, spark: SparkSession, source_name: str):
        self.spark = spark
        self.source_name = source_name
        self.target_table = f"accenture.manish_bronze.{source_name}"

    def ingest_from_landing(
        self,
        landing_path: str,
        file_format: str = "json",
        schema: StructType | None = None,
    ) -> DataFrame:
        """Ingest raw files from landing zone into Bronze."""

        reader = self.spark.read.format(file_format)

        if schema:
            reader = reader.schema(schema)
        else:
            # For Bronze, infer schema — we'll enforce in Silver
            reader = reader.option("inferSchema", "true")

        if file_format == "json":
            reader = reader.option("multiLine", "true") \
                           .option("mode", "PERMISSIVE") \
                           .option("columnNameOfCorruptRecord", "_corrupt_record")

        raw_df = reader.load(landing_path)

        # Add Bronze metadata columns
        enriched_df = raw_df \
            .withColumn("_bronze_ingested_at", current_timestamp()) \
            .withColumn("_bronze_source_file", input_file_name()) \
            .withColumn("_bronze_source_system", lit(self.source_name)) \
            .withColumn("_bronze_batch_id", lit(self._generate_batch_id()))

        return enriched_df

    def write_bronze(self, df: DataFrame, mode: str = "append") -> int:
        """Write DataFrame to Bronze Delta table."""

        df.write \
            .format("delta") \
            .mode(mode) \
            .option("mergeSchema", "true") \
            .saveAsTable(self.target_table)

        row_count = df.count()
        self._log_ingestion(row_count)
        return row_count

    def ingest_incremental(
        self, landing_path: str, file_format: str = "json"
    ) -> int:
        """Auto-Loader based incremental ingestion."""

        stream_df = self.spark.readStream \
            .format("cloudFiles") \
            .option("cloudFiles.format", file_format) \
            .option("cloudFiles.inferColumnTypes", "true") \
            .option("cloudFiles.schemaLocation",
                    f"dbfs:/FileStore/ManishGautam/medallionArch/checkpoints/{self.source_name}/schema") \
            .load(landing_path)

        enriched_df = stream_df \
            .withColumn("_bronze_ingested_at", current_timestamp()) \
            .withColumn("_bronze_source_file", input_file_name()) \
            .withColumn("_bronze_source_system", lit(self.source_name))

        query = enriched_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation",
                    f"dbfs:/FileStore/ManishGautam/medallionArch/checkpoints/{self.source_name}/checkpoint") \
            .option("mergeSchema", "true") \
            .trigger(availableNow=True) \
            .toTable(self.target_table)

        query.awaitTermination()
        return query.lastProgress["numInputRows"]

    def _generate_batch_id(self) -> str:
        from datetime import datetime
        return f"{self.source_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    def _log_ingestion(self, row_count: int):
        print(f"[Bronze] {self.source_name}: ingested {row_count} rows")

# COMMAND ----------

ingester = BronzeIngester(spark, "orders")

# Batch ingestion
df = ingester.ingest_from_landing("/FileStore/ManishGautam/medallionArch/orders/2026-03-10/")
count = ingester.write_bronze(df)
display(df)
print(f"Ingested {count} order records to Bronze")

# Or use Auto Loader for incremental
count = ingester.ingest_incremental("/FileStore/ManishGautam/medallionArch/orders/")
