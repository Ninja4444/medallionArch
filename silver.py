# Databricks notebook source
from dataclasses import dataclass
from typing import Callable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, sum as spark_sum


@dataclass
class QualityRule:
    name: str
    check: Callable[[DataFrame], DataFrame]
    threshold: float  # Minimum pass rate (0.0 to 1.0)
    is_critical: bool = False  # Critical = halt pipeline on failure


class DataQualityGate:
    """Validates data between medallion layers."""

    def __init__(self, table_name: str, rules: list[QualityRule]):
        self.table_name = table_name
        self.rules = rules
        self.results: list[dict] = []

    def validate(self, df: DataFrame) -> tuple[bool, DataFrame]:
        """Run all quality checks. Returns (passed, quarantined_df)."""

        total_rows = df.count()
        quarantine_condition = None
        all_passed = True

        for rule in self.rules:
            # Apply the check — returns DF with boolean column
            checked_df = rule.check(df)
            pass_count = checked_df.filter(
                col("_quality_passed")
            ).count()
            pass_rate = pass_count / total_rows if total_rows > 0 else 1.0

            passed = pass_rate >= rule.threshold

            result = {
                "rule": rule.name,
                "pass_rate": round(pass_rate, 4),
                "threshold": rule.threshold,
                "passed": passed,
                "critical": rule.is_critical,
                "failed_rows": total_rows - pass_count,
            }
            self.results.append(result)

            if not passed:
                all_passed = False
                if rule.is_critical:
                    raise DataQualityError(
                        f"Critical quality check failed: {rule.name} "
                        f"(pass_rate={pass_rate:.2%}, "
                        f"threshold={rule.threshold:.2%})"
                    )

                # Build quarantine filter
                fail_condition = ~col("_quality_passed")
                if quarantine_condition is None:
                    quarantine_condition = fail_condition
                else:
                    quarantine_condition = quarantine_condition | fail_condition

            df = checked_df.drop("_quality_passed")

        # Separate clean and quarantined records
        if quarantine_condition is not None:
            quarantined_df = df.filter(quarantine_condition)
        else:
            quarantined_df = df.limit(0)

        return all_passed, quarantined_df

    def print_report(self):
        print(f"\n{'='*60}")
        print(f"Data Quality Report: {self.table_name}")
        print(f"{'='*60}")
        for r in self.results:
            status = "PASS" if r["passed"] else "FAIL"
            print(
                f"  [{status}] {r['rule']}: "
                f"{r['pass_rate']:.2%} "
                f"(threshold: {r['threshold']:.2%}, "
                f"failed: {r['failed_rows']})"
            )
        print(f"{'='*60}\n")


class DataQualityError(Exception):
    pass

# COMMAND ----------

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

        return df.filter(col("_rescued_data").isNull()) \
                 .drop("_rescued_data")

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

# COMMAND ----------

# Define quality rules for orders
quality_rules = [
    QualityRule(
        name="order_id_not_null",
        check=lambda df: df.withColumn(
            "_quality_passed", col("transaction_id").isNotNull()
        ),
        threshold=1.0,
        is_critical=True,
    ),
    QualityRule(
        name="order_amount_positive",
        check=lambda df: df.withColumn(
            "_quality_passed", col("amount") > 0
        ),
        threshold=0.99,
    ),
    QualityRule(
        name="valid_order_date",
        check=lambda df: df.withColumn(
            "_quality_passed",
            col("amount").between(0, 99999)
        ),
        threshold=0.995,
    ),
]

# Run the Silver pipeline
transformer = SilverTransformer(spark, "orders")
bronze_df = transformer.read_bronze_incremental()
deduped_df = transformer.deduplicate(bronze_df, ["transaction_id"])

# Quality gate
gate = DataQualityGate("silver.orders", quality_rules)
passed, quarantined = gate.validate(deduped_df)
gate.print_report()

# Apply schema and write
typed_df = transformer.apply_schema(deduped_df, {
    "transaction_id": "string",
    "amount": "double",
   # "order_date": "date",
    "user_id": "long",
})
enriched_df = transformer.add_silver_metadata(typed_df)
transformer.merge_into_silver(enriched_df, ["transaction_id"])
