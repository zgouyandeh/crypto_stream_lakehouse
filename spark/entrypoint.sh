#!/bin/bash
set -e

# The Thrift server boots an embedded Derby metastore at /home/metastore_db
# for Spark's built-in session catalog (separate from the `demo` Iceberg
# REST catalog). Derby only allows one JVM to hold this open at a time, so
# clear any stale lock left behind by a killed/crashed previous run.
#
# IMPORTANT: once this container is up, do NOT run a second `spark-sql` or
# plain `spark-submit` (Hive-enabled) process inside it -- that's a second
# Spark driver JVM contending for the same lock and will crash one or both
# with "ERROR XSDB6: Another instance of Derby may have already booted the
# database". For any other ad-hoc SQL against the running Thrift server, use
# Beeline instead (it's a JDBC client, not a new driver) -- the
# demo.default/silver/gold namespaces themselves are now created
# automatically below, before Beeline/dbt ever need to connect:
#   docker compose exec spark-iceberg beeline -u "jdbc:hive2://localhost:10000/bronze" \
#     -n dbt -e "SHOW TABLES IN demo.silver;"
echo "Cleaning up any stale Derby metastore lock..."
rm -rf /home/metastore_db

# Create demo.{default,bronze,silver,gold} via a plain spark-submit -- not
# through Hive/Thrift -- so this never depends on the Thrift server being up
# yet, and never hits the "implicit USE <namespace> on connect" problem that
# otherwise requires a namespace to already exist before Beeline/dbt can even
# open a session. Runs every container start; idempotent (CREATE NAMESPACE
# IF NOT EXISTS), so it's harmless to repeat.
echo "Bootstrapping Iceberg namespaces..."
spark-submit /home/jobs/bootstrap_namespaces.py

echo "Starting Spark Thrift server on port 10000 (this is what dbt-spark talks to)..."
/opt/spark/sbin/start-thriftserver.sh \
  --master local[*] \
  --hiveconf hive.server2.thrift.port=10000 \
  --hiveconf hive.server2.thrift.bind.host=0.0.0.0

# start-thriftserver.sh forks a background JVM; tail its log so the
# container stays alive and you can `docker compose logs -f spark-iceberg`
LOG_DIR=$(ls -d /opt/spark/logs 2>/dev/null || echo /opt/spark/logs)
mkdir -p "$LOG_DIR"
tail -F "$LOG_DIR"/*.out 2>/dev/null || sleep infinity