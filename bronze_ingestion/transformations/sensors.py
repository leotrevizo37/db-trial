from pyspark import pipelines as dp


@dp.table()
def sensors():
    return (spark
        .table("dimensions.dbo.sensors")
        .dropDuplicates(["SensorId"])
    )