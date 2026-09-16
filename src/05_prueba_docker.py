from __future__ import annotations

import dask
from pyspark.sql import SparkSession


def main() -> None:
    dask_value = dask.compute(dask.delayed(lambda: 2 + 2)())[0]
    spark = (
        SparkSession.builder
        .appName("PruebaDockerDaskSpark")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark_value = spark.range(5).count()
    print(f"Dask disponible: 2 + 2 = {dask_value}")
    print(f"PySpark disponible: filas de prueba = {spark_value}")
    spark.stop()


if __name__ == "__main__":
    main()