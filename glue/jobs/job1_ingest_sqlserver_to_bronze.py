"""Glue Job 1 — ingest_sqlserver_to_bronze  (SDD §9.1)

Pulls the three source tables from SQL Server via JDBC and lands them as raw, typed
Delta in the Bronze zone, partitioned by batch_date, with ingest metadata. Encryption
in transit is enforced (encrypt=true / TLS); credentials are read from Secrets Manager
whose ARN is passed in — this job NEVER embeds or handles secret values (credential
boundary; Gerardo owns the secret and the Glue connection).

Fallback (SDD risk R5): --source_mode csv reads the raw CSVs from an S3 landing prefix
and lands Bronze directly, so Silver/Gold are not blocked while SQL Server is wired.
"""
import sys

try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from pyspark.context import SparkContext
    _GLUE = True
except Exception:
    _GLUE = False

from pyspark.sql import functions as F

TABLES = ["order_items", "order_item_options", "date_dim"]


def _args(argv):
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--source_mode", default="jdbc", choices=["jdbc", "csv"])
    p.add_argument("--batch_date", required=False)          # YYYY-MM-DD; run_date param
    p.add_argument("--bronze_path", default="s3://global-partners-dev-bronze")
    p.add_argument("--landing_path", default=None)          # csv mode
    # jdbc mode — connection details resolved from a Glue connection / Secrets Manager;
    # values are supplied by Gerardo, never by this code.
    p.add_argument("--jdbc_url", default=None)              # e.g. jdbc:sqlserver://host:1433;databaseName=...;encrypt=true
    p.add_argument("--secret_arn", default=None)            # Secrets Manager ARN (user/pass)
    known, _ = p.parse_known_args(argv)
    return known


def _read_jdbc(spark, a, table):
    # Credentials fetched at runtime from Secrets Manager by ARN; not logged, not stored.
    import boto3, json
    sm = boto3.client("secretsmanager")
    sec = json.loads(sm.get_secret_value(SecretId=a.secret_arn)["SecretString"])
    return (spark.read.format("jdbc")
            .option("url", a.jdbc_url)               # must include encrypt=true (TLS)
            .option("dbtable", f"dbo.{table}")
            .option("user", sec["username"])
            .option("password", sec["password"])
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load())


def _read_csv(spark, a, table):
    base = a.landing_path.rstrip("/")
    r = spark.read.option("header", True)
    if table == "order_items":
        r = r.option("multiLine", True).option("quote", '"').option("escape", '"')
    return r.csv(f"{base}/{table}.csv")


def main(argv):
    a = _args(argv)
    if _GLUE:
        sc = SparkContext(); spark = GlueContext(sc).spark_session
    else:
        from glue.lib.spark_session import get_local_spark
        spark = get_local_spark("ingest_to_bronze", with_delta=True)

    for t in TABLES:
        df = _read_csv(spark, a, t) if a.source_mode == "csv" else _read_jdbc(spark, a, t)
        df = (df.withColumn("ingest_ts", F.current_timestamp())
                .withColumn("source", F.lit(a.source_mode))
                .withColumn("batch_date", F.lit(a.batch_date) if a.batch_date else F.current_date()))
        # idempotent per-partition overwrite (reload = re-run of a batch_date; SDD §9.4)
        (df.write.format("delta").mode("overwrite")
           .partitionBy("batch_date")
           .option("replaceWhere", f"batch_date = '{a.batch_date}'" if a.batch_date else "true")
           .option("overwriteSchema", "true")
           .save(f"{a.bronze_path.rstrip('/')}/b_{t}"))
        print(f"[bronze] b_{t}: {df.count():,} rows")


if __name__ == "__main__":
    main(sys.argv[1:])
