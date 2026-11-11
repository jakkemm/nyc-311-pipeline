FROM apache/airflow:2.10.5-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* 

ENV JAVA_HOME=/usr/bin/java

COPY --chown=airflow:root requirements.txt /tmp/requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /tmp/requirements.txt
