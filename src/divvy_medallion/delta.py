"""Delta Lake write helpers."""

from __future__ import annotations

from pyspark.sql import DataFrame


def overwrite_partition(df: DataFrame, path: str, partition_column: str, value: str) -> None:
    """Idempotently (re)write one partition: re-running a period replaces it instead of appending."""
    if df.filter(df[partition_column] != value).limit(1).count():
        raise ValueError(f"DataFrame contains rows outside partition {partition_column}={value}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{partition_column} = '{value}'")
        .partitionBy(partition_column)
        .save(path)
    )


def overwrite_table(df: DataFrame, path: str) -> None:
    """Full rebuild of a small derived table (dimensions); the schema follows the code."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
