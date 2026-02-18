from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table()
def hourly_reads():
    reads = (
        spark.readStream.table("trial.silver.reads")
        .withColumn("event_ts", F.col("LocalTimeSpan").cast("timestamp"))
        .withWatermark("event_ts", "2 hours")
    )

    hourly_readings = (
        reads
        .withColumn("reading_hour", F.date_trunc("HOUR", F.col("event_ts")))
        .groupBy("reading_hour", "SensorId")
        .agg(
            F.min("Value").alias("min_value"),
            F.max("Value").alias("max_value"),
            F.avg("Value").alias("avg_value"),
            F.stddev_pop("Value").alias("stddev_value")
        )
    )

    return hourly_readings
