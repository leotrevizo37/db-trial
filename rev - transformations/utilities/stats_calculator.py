# utilities/stats_calculator.py
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    date_trunc,
    avg,
    sum as spark_sum,
    count,
    when as spark_when,
    unix_timestamp,
    from_unixtime,
    floor,
    lit,
)


def stats_calculator(
    base: DataFrame,
    sensor_type: str = "Corriente",
    min_expected_value: int = 0,
    max_expected_value: Optional[int] = None,
) -> DataFrame:
    base = (
        base.filter(col("_sensor_type") == sensor_type)
        .withColumn("HourStart", date_trunc("hour", col("LocalTimeSpan")))
        .withColumn(
            "5MinuteStart",
            from_unixtime(floor(unix_timestamp(col("LocalTimeSpan")) / 300) * 300).cast("timestamp"),
        )
    )

    if max_expected_value is None:
        base = base.withColumn(
            "is_anomaly",
            spark_when(col("Value") < lit(min_expected_value), lit(1)).otherwise(lit(0)),
        )

        stats_df = (
            base.groupBy("DeviceId", "HourStart", "LocationId", "SensorTyId", "_sensor_type")
            .agg(
                avg("Value").alias("Val_dummy"),
                spark_sum(spark_when(col("Value") >= lit(min_expected_value), lit(1)).otherwise(lit(0))).alias("is_okay"),
                count(lit(1)).alias("total_readings"),
                spark_sum("is_anomaly").alias("Anomalies_raw"),
            )
        )
    else:
        base = base.withColumn(
            "is_anomaly",
            spark_when(
                (col("Value") < lit(min_expected_value)) | (col("Value") > lit(max_expected_value)),
                lit(1),
            ).otherwise(lit(0)),
        )

        stats_df = (
            base.groupBy("DeviceId", "HourStart", "LocationId", "SensorTyId", "_sensor_type")
            .agg(
                avg("Value").alias("Val_dummy"),
                spark_sum(
                    spark_when(
                        (col("Value") >= lit(min_expected_value)) & (col("Value") <= lit(max_expected_value)),
                        lit(1),
                    ).otherwise(lit(0))
                ).alias("is_okay"),
                count(lit(1)).alias("total_readings"),
                spark_sum("is_anomaly").alias("Anomalies_raw"),
            )
        )

    return (
        stats_df.withColumn(
            "StatusCode",
            spark_when((col("is_okay") / col("total_readings")) >= lit(0.50), lit(1)).otherwise(lit(3)),
        )
        .withColumn("Promedio", col("is_okay") / col("total_readings"))
        .withColumnRenamed("Anomalies_raw", "Anomalies")
        .fillna({"Anomalies": 0})
        .withColumnRenamed("Val_dummy", "MeasurementValue")
        .withColumnRenamed("HourStart", "LocalTimeSpan")
    )
