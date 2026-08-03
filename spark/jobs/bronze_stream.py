"""
Kafka -> Iceberg bronze layer, for the real Coinbase feed.

Runs two Structured Streaming queries concurrently in one Spark
application:

  orderbook_updates -> demo.bronze.orderbook_events
      Landed close to raw: envelope fields (type, product_id, time,
      sequence) are parsed out for partitioning/filtering, but the
      snapshot's bids/asks arrays and the l2update's changes array are
      kept as the original JSON string in `payload`, because their
      shape differs by message type. Flattening into individual price
      levels happens in the silver layer (dbt), not here.

  trades -> demo.bronze.trades
      Landed fully typed: this message shape ("match") is stable, so
      there's no reason to defer parsing.

Run inside the spark-iceberg container, e.g.:
    docker compose exec spark-iceberg spark-submit /home/jobs/bronze_stream.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, to_timestamp
from pyspark.sql.types import LongType, StringType, StructField, StructType

CATALOG = "demo"
BRONZE_DB = "bronze"

ORDERBOOK_TABLE = f"{CATALOG}.{BRONZE_DB}.orderbook_events"
ORDERBOOK_TOPIC = "orderbook_updates"
ORDERBOOK_CHECKPOINT = "/home/iceberg/checkpoints/bronze_orderbook_events"

TRADES_TABLE = f"{CATALOG}.{BRONZE_DB}.trades"
TRADES_TOPIC = "trades"
TRADES_CHECKPOINT = "/home/iceberg/checkpoints/bronze_trades"

# Common envelope across snapshot/l2update messages. `sequence` is often
# null here (Coinbase's level2 channel doesn't reliably include it on every
# message type) -- the heartbeat channel's sequence is the one the producer
# actually uses for gap detection; this column is best-effort/informational.
ORDERBOOK_ENVELOPE_SCHEMA = StructType(
    [
        StructField("type", StringType()),
        StructField("product_id", StringType()),
        StructField("time", StringType()),
        StructField("sequence", LongType()),
    ]
)

TRADE_SCHEMA = StructType(
    [
        StructField("trade_id", LongType()),
        StructField("sequence", LongType()),
        StructField("product_id", StringType()),
        StructField("price", StringType()),  # Coinbase sends numerics as strings
        StructField("size", StringType()),
        StructField("side", StringType()),
        StructField("time", StringType()),
    ]
)


def ensure_bronze_tables(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{BRONZE_DB}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ORDERBOOK_TABLE} (
            type         STRING,
            product_id   STRING,
            time         STRING,
            sequence     BIGINT,
            payload      STRING,
            ingest_time  TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(ingest_time))
        TBLPROPERTIES ('format-version' = '2')
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TRADES_TABLE} (
            trade_id     BIGINT,
            sequence     BIGINT,
            product_id   STRING,
            price        DOUBLE,
            size         DOUBLE,
            side         STRING,
            time         TIMESTAMP,
            raw_payload  STRING,
            ingest_time  TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(time))
        TBLPROPERTIES ('format-version' = '2')
        """
    )


def start_orderbook_query(spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", ORDERBOOK_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    payload = raw.select(col("value").cast("string").alias("payload"))
    parsed = (
        payload.withColumn("envelope", from_json(col("payload"), ORDERBOOK_ENVELOPE_SCHEMA))
        .select(
            col("envelope.type").alias("type"),
            col("envelope.product_id").alias("product_id"),
            col("envelope.time").alias("time"),
            col("envelope.sequence").alias("sequence"),
            col("payload"),
        )
        .withColumn("ingest_time", current_timestamp())
    )

    return (
        parsed.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", ORDERBOOK_CHECKPOINT)
        .toTable(ORDERBOOK_TABLE)
    )


def start_trades_query(spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", TRADES_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw.select(
            col("value").cast("string").alias("raw_payload"),
            from_json(col("value").cast("string"), TRADE_SCHEMA).alias("d"),
        )
        .select(
            col("d.trade_id").alias("trade_id"),
            col("d.sequence").alias("sequence"),
            col("d.product_id").alias("product_id"),
            col("d.price").cast("double").alias("price"),
            col("d.size").cast("double").alias("size"),
            col("d.side").alias("side"),
            to_timestamp(col("d.time")).alias("time"),
            col("raw_payload"),
        )
        .withColumn("ingest_time", current_timestamp())
    )

    return (
        parsed.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", TRADES_CHECKPOINT)
        .toTable(TRADES_TABLE)
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("coinbase-to-iceberg-bronze")
        # This job only touches the `demo` Iceberg REST catalog, never
        # Spark's built-in session catalog -- forcing in-memory here means
        # it can run at the same time as the Thrift server in the same
        # container without both contending for the embedded Derby lock at
        # /home/metastore_db (see the earlier XSDB6 troubleshooting notes).
        .config("spark.sql.catalogImplementation", "in-memory")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    ensure_bronze_tables(spark)

    orderbook_query = start_orderbook_query(spark)
    trades_query = start_trades_query(spark)

    print(f"Streaming into {ORDERBOOK_TABLE} and {TRADES_TABLE} ... (Ctrl+C to stop)")
    # awaitAnyTermination (not awaitTermination on a single query) so that if
    # either stream dies, the whole job exits loudly instead of one query
    # silently stopping while the other keeps running unnoticed.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()