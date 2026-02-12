from pyspark import pipelines as dp


@dp.table()
def tenants():
    return (spark
        .read
        .parquet("abfss://bronze@dbdemodatalake.dfs.core.windows.net/reference/dims_tenants.parquet")
        .dropDuplicates(["TenantId"])
    )