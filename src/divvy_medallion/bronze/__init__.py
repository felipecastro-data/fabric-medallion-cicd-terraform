"""Bronze layer: raw source data landed as Delta with ingestion metadata, partitioned by period."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PERIOD_COLUMN = "_period"


def add_ingestion_metadata(df: DataFrame, *, period: str, batch_id: str, source_file: str) -> DataFrame:
    """Append lineage columns shared by every Bronze table."""
    return (
        df.withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn(PERIOD_COLUMN, F.lit(period))
    )
