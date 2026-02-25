from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

from utilities.stats_calculator_time_window import stats_calculator_time_window

@dp.materialized_view(name="mv_maquinas_nieve", comment="Ice machines: current sensor window stats + OperationId")
def mv_maquinas_nieve():
    maquinas_nieve_df = stats_calculator_time_window(
        spark.read.table("trial.silver.vend_machine_reads"),
        "Corriente",
        1,
        None,
        20
    )


    group_ids_df = (
        maquinas_nieve_df.select("DeviceId", "LocalTimeSpan").distinct().withColumn("OperationId", expr("uuid()"))
    )

    return maquinas_nieve_df.join(group_ids_df, on=["DeviceId", "LocalTimeSpan"], how="left")
