# tabulario/spark-iceberg already bundles Spark + the Iceberg Spark runtime +
# the Iceberg AWS bundle (S3FileIO), which is why the "demo" REST catalog in
# spark-defaults.conf works with no extra jars. We only add the Kafka
# connector jars (not shipped in the base image) needed for Structured
# Streaming, plus a Thrift server entrypoint so dbt-spark can connect.
#
# IMPORTANT: before building, confirm the Spark version actually inside this
# base image matches the jars you're copying in:
#   docker run --rm tabulario/spark-iceberg:latest spark-submit --version
# and confirm iceberg-spark-runtime / iceberg-aws-bundle aren't already
# present (they should be) before ever adding a second copy of either:
#   docker run --rm tabulario/spark-iceberg:latest bash -c "ls /opt/spark/jars | grep -Ei 'iceberg|aws'"
FROM tabulario/spark-iceberg:latest

# Only the Kafka Structured Streaming connector + its two required
# dependencies. Do NOT add iceberg-spark-runtime, iceberg-aws-bundle,
# aws-java-sdk-bundle, or hadoop-aws here -- see note above / the chat
# explanation for why they're either already present or unused by this
# pipeline (native S3FileIO, not s3a://).
COPY jars/spark-sql-kafka-0-10_2.12-3.5.5.jar          /opt/spark/jars/
COPY jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar /opt/spark/jars/
COPY jars/kafka-clients-3.5.0.jar                       /opt/spark/jars/
COPY jars/commons-pool2-2.11.1.jar                      /opt/spark/jars/

# Small Python client for the producer script
RUN pip install --no-cache-dir kafka-python==2.0.2 websocket-client==1.8.0

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY entrypoint.sh /home/entrypoint.sh
# Windows editors/checkouts often save this as CRLF, which breaks the
# shebang (bash looks for an interpreter literally named "/bin/bash\r" and
# reports "No such file or directory" even though the file exists) -- strip
# any \r before it ever gets a chance to matter.
RUN sed -i 's/\r$//' /home/entrypoint.sh && chmod +x /home/entrypoint.sh

WORKDIR /home
