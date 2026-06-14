FROM apache/airflow:2.10.5

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    minio \
    psycopg2-binary \
    kafka-python \
    clickhouse-driver \
    pandas \
    numpy \
    pyspark