from pyspark import pipelines as dp


@dp.table()
def device_tys():
    return (spark
        .table("dimensions.dbo.DeviceTys")
        .dropDuplicates(["DeviceTyId"])
    )