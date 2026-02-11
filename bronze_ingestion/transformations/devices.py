from pyspark import pipelines as dp


@dp.table()
def devices():
    return (spark
        .read
        .parquet("abfss://bronze@dbdemodatalake.dfs.core.windows.net/reference/dims_devices.parquet")
        .dropDuplicates(["DeviceId"])
    )