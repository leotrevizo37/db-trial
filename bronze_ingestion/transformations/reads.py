from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)


TARGET = "bronze.reads_raw"
READS_SCHEMA = StructType([
    StructField("ReadId", StringType(), False),
    StructField("SensorId", StringType(), False),
    StructField("TimeSpan", TimestampType(), False),
    StructField("LocalTimeSpan", TimestampType(), True),
    StructField("Value", DoubleType(), False)
])

dp.create_streaming_table(
    name=TARGET,
    comment="Bronze reads (deduped at ingest using insert-only MERGE on ReadId)"
)

@dp.view()
def reads_source():
    return (
        spark.readStream
            .format("cloudFiles")
            .schema(READS_SCHEMA)
            .option("cloudFiles.format", "parquet")
            .option("recursiveFileLookup", "true")
            .option("cloudFiles.schemaEvolutionMode", "none")
            .option("cloudFiles.allowOverwrites", "true")
            .load("abfss://bronze@dbdemodatalake.dfs.core.windows.net/reads/")
            .withColumn("_ingest_at", current_timestamp())
    )

dp.create_auto_cdc_flow(
    target=TARGET,
    source="reads_source",
    keys=["ReadId"],
    sequence_by="_ingest_at",
    stored_as_scd_type=1
)
