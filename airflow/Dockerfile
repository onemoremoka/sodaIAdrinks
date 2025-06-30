FROM apache/airflow:slim-3.0.2-python3.10

ENV AIRFLOW_HOME=/home/airflow/airflow
ENV PYTHONPATH=/home/airflow/airflow
ENV TZ=America/Santiago
ENV AIRFLOW__CORE__DEFAULT_TIMEZONE=America/Santiago

WORKDIR $AIRFLOW_HOME

USER root

# Setear zona horaria en el sistema operativo
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libmariadb-dev \
    nano \
    unzip \
    tzdata \
    && ln -snf /usr/share/zoneinfo/America/Santiago /etc/localtime \
    && echo "America/Santiago" > /etc/timezone \
    && dpkg-reconfigure -f noninteractive tzdata \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p $AIRFLOW_HOME/dags $AIRFLOW_HOME/src
COPY airflow/ $AIRFLOW_HOME/
COPY src/ $AIRFLOW_HOME/src/
COPY data/ $AIRFLOW_HOME/data/

RUN chown -R airflow: $AIRFLOW_HOME

USER airflow

COPY requirements.txt $AIRFLOW_HOME/
RUN pip install --no-cache-dir -r requirements.txt -c /home/airflow/constraints.txt

