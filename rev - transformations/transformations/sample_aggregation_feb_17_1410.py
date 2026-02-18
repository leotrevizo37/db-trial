from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.stats import stats_calculator


@dp.table(name="stats_current")
def stats_current():
    base = dp.read("factlecturas_extended")
    return stats_calculator(base, sensor_type="Corriente", min_expected_value=1, max_expected_value=None)
