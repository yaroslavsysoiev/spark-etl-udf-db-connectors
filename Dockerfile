FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# Compose does not expose a GPU; use CPU PyTorch without CUDA/Triton downloads.
# Keep this in a separate layer so other dependency changes reuse it.
ARG TORCH_VERSION=2.10.0+cpu
RUN pip3 install --no-cache-dir --timeout 120 --retries 5 \
    --index-url https://download.pytorch.org/whl/cpu "torch==${TORCH_VERSION}"

COPY requirements.txt /tmp/requirements.txt

# Explicitly retain the CPU build when resolving sentence-transformers dependencies.
RUN pip3 install --no-cache-dir --timeout 120 --retries 5 \
    -r /tmp/requirements.txt "torch==${TORCH_VERSION}"

# Delta Lake, PostgreSQL and Kafka dependencies
RUN python3 - <<'PY'
import urllib.request

jars = {
    # Delta Lake
    "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar":
        "/opt/spark/jars/delta-spark_2.12-3.2.0.jar",

    "https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar":
        "/opt/spark/jars/delta-storage-3.2.0.jar",

    # PostgreSQL JDBC
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar":
        "/opt/spark/jars/postgresql-42.7.8.jar",

    # Kafka Structured Streaming
    "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar":
        "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar",

    "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar":
        "/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar",

    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar":
        "/opt/spark/jars/kafka-clients-3.4.1.jar",

    "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar":
        "/opt/spark/jars/commons-pool2-2.11.1.jar",
}

for url, destination in jars.items():
    print(f"Downloading {url}")
    urllib.request.urlretrieve(url, destination)
PY

COPY spark_pipelines /app/spark_pipelines

WORKDIR /app

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
