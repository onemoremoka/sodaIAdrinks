FROM apache/airflow:slim-3.0.2-python3.10

ENV AIRFLOW_HOME=/home/airflow/airflow
WORKDIR $AIRFLOW_HOME

# Usamos root para copiar archivos y preparar el entorno
USER root

# Instala herramientas necesarias
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libmariadb-dev \
    nano \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Crear carpetas necesarias
RUN mkdir -p $AIRFLOW_HOME/dags $AIRFLOW_HOME/src

# Copiar archivos del proyecto
COPY airflow/ $AIRFLOW_HOME/
COPY src/ $AIRFLOW_HOME/src/
COPY requirements.txt $AIRFLOW_HOME/

# Cambiar permisos de los archivos copiados al usuario airflow
RUN chown -R airflow: $AIRFLOW_HOME

# Cambiar al usuario airflow
USER airflow

# Ejecutar pip install como el usuario correcto y con el constraints path correcto
RUN pip install --no-cache-dir -r requirements.txt -c /home/airflow/constraints.txt
