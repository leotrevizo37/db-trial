from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
import re

@udf(returnType=BooleanType())
def is_valid_email(email):
    """
    This function checks if the given email address has a valid format using regex.
    Returns True if valid, False otherwise.
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if email is None:
        return False
    return re.match(pattern, email) is not None

@udf(returnType=DataFrame)
def stats_calculator(
    base: DataFrame,
    sensor_type: str = "Corriente",
    min_expected_value: int = 0,
    max_expected_value: Optional[int] = None,
) -> DataFrame:
    base_filtered = (
        base
        .filter(F.col("_sensor_type") == F.lit(sensor_type))
        .withColumn("HourStart", F.date_trunc("hour", F.col("LocalTimeSpan")))
        .withColumn(
            "5MinuteStart",
            F.from_unixtime(F.floor(F.unix_timestamp(F.col("LocalTimeSpan")) / F.lit(300)) * F.lit(300)).cast("timestamp"),
        )
    )

    if max_expected_value is None:
        base_flagged = base_filtered.withColumn(
            "is_anomaly",
            F.when(F.col("Value") < F.lit(min_expected_value), F.lit(1)).otherwise(F.lit(0)),
        )

        stats_df = (
            base_flagged
            .groupBy("DeviceId", "HourStart", "LocationId", "SensorTyId", "_sensor_type")
            .agg(
                F.avg("Value").alias("MeasurementValue"),
                F.sum(F.when(F.col("Value") >= F.lit(min_expected_value), F.lit(1)).otherwise(F.lit(0))).alias("is_okay"),
                F.count(F.lit(1)).alias("total_readings"),
                F.sum("is_anomaly").alias("Anomalies"),
            )
        )
    else:
        base_flagged = base_filtered.withColumn(
            "is_anomaly",
            F.when(
                (F.col("Value") < F.lit(min_expected_value)) | (F.col("Value") > F.lit(max_expected_value)),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )

        stats_df = (
            base_flagged
            .groupBy("DeviceId", "HourStart", "LocationId", "SensorTyId", "_sensor_type")
            .agg(
                F.avg("Value").alias("MeasurementValue"),
                F.sum(
                    F.when(
                        (F.col("Value") >= F.lit(min_expected_value)) & (F.col("Value") <= F.lit(max_expected_value)),
                        F.lit(1),
                    ).otherwise(F.lit(0))
                ).alias("is_okay"),
                F.count(F.lit(1)).alias("total_readings"),
                F.sum("is_anomaly").alias("Anomalies"),
            )
        )

    return (
        stats_df
        .fillna({"Anomalies": 0})
        .withColumn(
            "StatusCode",
            F.when((F.col("is_okay") / F.col("total_readings")) >= F.lit(0.50), F.lit(1)).otherwise(F.lit(3)),
        )
        .withColumn("Promedio", F.col("is_okay") / F.col("total_readings"))
        .withColumn("LocalTimeSpan", F.col("HourStart"))
        .drop("HourStart", "is_okay", "total_readings")
    )