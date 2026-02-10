from pyspark import pipelines as dp


@dp.table
def sublocations():
    return (spark
        .read
        .parquet("abfss://bronze@dbdemodatalake.dfs.core.windows.net/reference/dims_sublocations.parquet")
    )