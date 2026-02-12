from pyspark import pipelines as dp


@dp.table()
def devices():
    return (spark
        .table("dimensions.dbo.Devices")
        .dropDuplicates(["DeviceId"])
    )