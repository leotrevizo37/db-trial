from pyspark import pipelines as dp


@dp.table
def sensor_tys():
    return (spark
        .read
        .parquet("abfss://bronze@dbdemodatalake.dfs.core.windows.net/reference/dims_sensorTys.parquet")
    )