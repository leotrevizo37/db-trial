from pyspark import pipelines as dp


@dp.table()
def sensor_tys():
    return (spark
        .table("dimensions.dbo.SensorTys")
        .dropDuplicates(["SensorTyId"])
    )