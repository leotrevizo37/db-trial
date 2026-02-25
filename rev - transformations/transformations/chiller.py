from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit, expr

from utilities.stats_calculator import stats_calculator
from utilities.stats_calculator_time_window import stats_calculator_time_window

@dp.materialized_view(name="mv_cuartos_refrigeracion", comment="Cooler rooms: temperature window + current stats + OperationId")
def mv_cuartos_refrigeracion():
    base = spark.read.table("trial.silver.chiller_reads")

    temperature_stats_df = (
        stats_calculator_time_window(base, "Temperatura", 34, 40, 240)
    )

    current_stats_df = (
        stats_calculator(base, "Corriente", 1)
        .withColumn("Anomalies", lit(None))
    )

    combined_df = temperature_stats_df.unionByName(current_stats_df)

    group_ids_df = combined_df.select("DeviceId", "LocalTimeSpan").distinct().withColumn("OperationId", expr("uuid()"))

    return combined_df.join(group_ids_df, on=["DeviceId", "LocalTimeSpan"], how="left")
