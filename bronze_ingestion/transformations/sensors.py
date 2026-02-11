from pyspark import pipelines as dp


@dp.table()
def sensors():
    return (spark
        .read
        .parquet("abfss://bronze@dbdemodatalake.dfs.core.windows.net/reference/dims_sensors.parquet")
        .dropDuplicates(["SensorId"])
    )