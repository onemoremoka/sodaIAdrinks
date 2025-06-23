FROM apache/airflow:slim-3.0.2-python3.10

ENV AIRFLOW_HOME=/home/airflow/airflow
WORKDIR $AIRFLOW_HOME

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
        build-essential \
        libmariadb-dev \
        nano \
        unzip \
  && rm -rf /var/lib/apt/lists/*

RUN mkdir -p $AIRFLOW_HOME && chown -R 50000:0 $AIRFLOW_HOME

COPY . $AIRFLOW_HOME

RUN chown -R 50000:0 $AIRFLOW_HOME

USER airflow
