from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table()
def sensor_hourly_availability():
    reads = (
        spark.readStream.table("trial.silver.reads")
        .withColumn("event_ts", F.col("LocalTimeSpan").cast("timestamp"))
        .withWatermark("event_ts", "2 hours")
    )

    hourly = (
        reads.groupBy(
            "SensorId",
            "DeviceId",
            "TenantId",
            "LocationId",
            "SublocationId",
            "value_kind",
            F.window("event_ts", "1 hour").alias("w")
        )
        .agg(
            F.count("*").alias("observed_points")
        )
        .withColumn("expected_points", F.lit(12))
        .withColumn(
            "availability_pct",
            (F.col("observed_points") / F.col("expected_points") * F.lit(100)).cast("decimal(5,2)")
        )
        .withColumn(
            "gap_minutes",
            (F.greatest(F.lit(0), F.col("expected_points") - F.col("observed_points")) * F.lit(5)).cast("int")
        )
        .select(
            "SensorId",
            "DeviceId",
            "TenantId",
            "LocationId",
            "SublocationId",
            "value_kind",
            F.col("w.start").alias("reading_hour"),
            "observed_points",
            "expected_points",
            "availability_pct",
            "gap_minutes"
        )
    )

    return hourly
