from pyspark import pipelines as dp


@dp.table()
def locations():
    return (spark
        .table("dimensions.dbo.Locations")
        .dropDuplicates(["LocationId"])
    )