#!/bin/bash
# Run once after the spark-iceberg container is up (and after bronze_stream.py
# has created demo.bronze at least once), before the first `dbt run`.
#
#   docker compose exec spark-iceberg bash /home/jobs/init_namespaces.sh
#
# Why "bronze" and not the empty/default database: opening a Hive-protocol
# session (Beeline, or dbt-spark's thrift connection) issues an implicit
# `USE <database>` as its very first command. Iceberg REST catalogs don't
# ship a pre-existing "default" namespace, so pointing at an empty database
# fails with NoSuchNamespaceException before any of your own SQL runs.
# "demo.bronze" already exists (created by bronze_stream.py), so we bootstrap
# through that one instead.
set -e

beeline -u "jdbc:hive2://localhost:10000/bronze" -n dbt -e "
CREATE NAMESPACE IF NOT EXISTS demo.default;
CREATE NAMESPACE IF NOT EXISTS demo.silver;
CREATE NAMESPACE IF NOT EXISTS demo.gold;
"

echo "Namespaces ready: demo.default, demo.silver, demo.gold"
