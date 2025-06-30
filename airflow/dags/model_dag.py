import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from src.utils import creates_folders, ingestion_data
from src.preprocessing import preprocessing_data
from src.loader import split_data
from src.train import train_model
from airflow.models import Variable

Variable.set("dev", "False")
print("✅ DAG model_dag.py cargado correctamente")

dag_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "owner": "airflow",
}

with DAG(
    dag_id="MODEL_DAG",
    description="DAG para el procesamiento de datos y entrenamiento de modelo",
    schedule=timedelta(days=1),
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=["MDS"],
    default_args=dag_args,
) as dag:
    start = EmptyOperator(task_id="start")

    task_create_folders = PythonOperator(
        task_id="create_folders",
        python_callable=creates_folders,
        op_kwargs={"ds": "{{ ds }}"},
    )

    task_ingestion_data = PythonOperator(
        task_id="ingestion_data",
        python_callable=ingestion_data,
        op_kwargs={"ds": "{{ ds }}", "dev": "{{ var.value.dev }}"},
    )

    task_preprocessing_data = PythonOperator(
        task_id="preprocessing_data",
        python_callable=preprocessing_data,
        op_kwargs={"ds": "{{ ds }}", "dev": "{{ var.value.dev }}"},
    )

    task_split_data = PythonOperator(
        task_id="split_data",
        python_callable=split_data,
        op_kwargs={"ds": "{{ ds }}", "dev": "{{ var.value.dev }}"},
    )

    task_train_model = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
        op_kwargs={"model_path": "models/trained_model.pkl"},
    )

    (
        start
        >> task_create_folders
        >> task_ingestion_data
        >> task_preprocessing_data
        >> task_split_data
        >> task_train_model
    )
