from pyspark import pipelines as dp


@dp.table
def device_tys():
    return (spark
        .read
        .parquet("abfss://bronze@dbdemodatalake.dfs.core.windows.net/reference/dims_deviceTys.parquet")
    )