from pyspark import pipelines as dp


@dp.table()
def locations():
    return (spark
        .read
        .parquet("abfss://bronze@dbdemodatalake.dfs.core.windows.net/reference/dims_locations.parquet")
        .dropDuplicates(["LocationId"])
    )