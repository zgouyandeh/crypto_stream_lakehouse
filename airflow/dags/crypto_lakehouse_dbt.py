"""
Schedules dbt (silver + gold) on top of the continuously-streaming bronze
layer. The streaming jobs (Kafka producer, bronze_stream.py) are NOT
managed here -- they're long-running services, not periodic batch tasks.

Runs `dbt run` then `dbt test` inside the already-running `dbt` container
(the one with `entrypoint: ["sleep", "infinity"]` in docker-compose.yml) via
`docker exec`, using the /var/run/docker.sock mount already present on the
Airflow containers. This sidesteps the path-translation problems that come
with DockerOperator launching fresh sibling containers on Windows/WSL.

Prerequisite: the `docker` CLI must be installed inside the Airflow image
(see Dockerfile.airflow snippet in the accompanying notes) -- BashOperator
runs this command inside the Airflow container itself, not on the host.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_CONTAINER = "dbt"  # must match the `container_name:` in docker-compose.yml
DBT_CMD = "dbt {subcommand} --project-dir /usr/app --profiles-dir /usr/app"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="crypto_lakehouse_dbt",
    description="Silver/gold transformation on top of the streaming bronze layer",
    schedule="*/10 * * * *",   # every 10 minutes; tune to how often bronze gets meaningfully new data
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,          # never let two dbt runs overlap
    default_args=default_args,
    tags=["dbt", "silver", "gold"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"docker exec {DBT_CONTAINER} {DBT_CMD.format(subcommand='run')}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"docker exec {DBT_CONTAINER} {DBT_CMD.format(subcommand='test')}",
        # Don't fail the whole DAG run just because a test failed -- you
        # still want to know about it, but a data-quality warning shouldn't
        # look identical to a broken pipeline. Remove this if you'd rather
        # test failures block downstream consumption entirely.
        trigger_rule="all_done",
    )

    dbt_run >> dbt_test