"""Spark session helpers.

WHY two builders: the transformation logic (cleaning.py, calendar_gen.py) is pure
PySpark so it runs identically in three places — local pytest, CI, and AWS Glue.
- get_local_spark(): a Delta-enabled local session for tests/dev (adds the Java 17+
  module-opens Spark needs on modern JDKs).
- In production the Glue job uses GlueContext.spark_session; Delta is already on the
  Glue 4.0 classpath, so no extra packages are required there.
"""
from __future__ import annotations

_JAVA_OPENS = (
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
)


def get_local_spark(app_name: str = "de-global-partners-local", with_delta: bool = False):
    """Local SparkSession for tests and local development.

    with_delta=False (default): plain session — sufficient for transformation logic
    that reads CSV and returns DataFrames (no Delta I/O), and avoids fetching Delta
    jars over the network. Set with_delta=True only when exercising a Delta read/write
    locally (requires network access to Maven the first time).
    """
    import os
    from pyspark.sql import SparkSession

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    builder = (
        SparkSession.builder.master("local[2]").appName(app_name)
        .config("spark.driver.extraJavaOptions", _JAVA_OPENS)
        .config("spark.executor.extraJavaOptions", _JAVA_OPENS)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if with_delta:
        builder = (builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        try:
            from delta import configure_spark_with_delta_pip
            builder = configure_spark_with_delta_pip(builder)
        except Exception:
            pass
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
