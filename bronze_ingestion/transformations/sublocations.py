from pyspark import pipelines as dp


@dp.table()
def sublocations():
    return (spark
        .table("dimensions.dbo.Sublocations")
        .dropDuplicates(["SublocationId"])
    )