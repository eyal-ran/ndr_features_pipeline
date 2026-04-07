"""Shared lightweight dependency stubs for offline unit-test execution."""

import sys
import types


if "pyspark" not in sys.modules:
    pyspark = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql.SparkSession = object
    pyspark_sql.DataFrame = object
    pyspark_sql.functions = types.ModuleType("pyspark.sql.functions")
    pyspark_sql.types = types.ModuleType("pyspark.sql.types")
    for _name in ["StringType", "IntegerType", "LongType", "DoubleType", "TimestampType", "DateType"]:
        setattr(pyspark_sql.types, _name, type(_name, (), {}))

    pyspark.sql = pyspark_sql
    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.sql"] = pyspark_sql
    sys.modules["pyspark.sql.functions"] = pyspark_sql.functions
    sys.modules["pyspark.sql.types"] = pyspark_sql.types
