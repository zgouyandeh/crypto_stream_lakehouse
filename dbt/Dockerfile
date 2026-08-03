FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential libsasl2-dev git \
    && rm -rf /var/lib/apt/lists/*

# PyHive[hive] pulls in the thrift/sasl bits dbt-spark needs for method: thrift
RUN pip install --no-cache-dir "dbt-spark[PyHive]==1.8.*" "dbt-core==1.8.*"

ENV DBT_PROFILES_DIR=/usr/app
WORKDIR /usr/app
