from superset.db_engine_specs.hive import HiveEngineSpec
from typing import List, Set, Tuple


class SparkSqlEngineSpec(HiveEngineSpec):
    engine = "sparksql"

    _time_grain_functions = {
        None: "{col}",
        "PT1S": "date_trunc('second', {col})",
        "PT1M": "date_trunc('minute', {col})",
        "PT1H": "date_trunc('hour', {col})",
        "P1D": "date_trunc('day', {col})",
        "P1W": "date_trunc('week', {col})",
        "P1M": "date_trunc('month', {col})",
        "P0.25Y": "date_trunc('quarter', {col})",
        "P1Y": "date_trunc('year', {col})",
    }

    # Do not expand data.. for some reason nested type from PyHive is not compatible with Presto engine spec.
    @classmethod
    def expand_data(
        cls, columns: List[dict], data: List[dict]
    ) -> Tuple[List[dict], List[dict], List[dict]]:
        return columns, data, []
