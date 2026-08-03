"""
Creates the demo.{default,bronze,silver,gold} namespaces via a plain
spark-submit -- not through the Thrift server -- so it never hits the
Hive-protocol "implicit USE on connect" problem that requires a namespace
to already exist before Beeline/dbt can even open a session.

Run once, before the Thrift server starts (see entrypoint.sh). Safe to run
on every container start: CREATE NAMESPACE IF NOT EXISTS is idempotent.
"""
from pyspark.sql import SparkSession

NAMESPACES = ["default", "bronze", "silver", "gold"]


def main() -> None:
    spark = (
        SparkSession.builder.appName("bootstrap-namespaces")
        # Same reasoning as bronze_stream.py: this only touches the `demo`
        # Iceberg REST catalog, so there's no reason to boot Hive's Derby
        # metastore for it, and doing so would contend with the Thrift
        # server if it happened to already be running.
        .config("spark.sql.catalogImplementation", "in-memory")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    for ns in NAMESPACES:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS demo.{ns}")

    print(f"Namespaces ready: {', '.join(f'demo.{ns}' for ns in NAMESPACES)}")
    spark.stop()


if __name__ == "__main__":
    main()