import os
from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

dag_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "owner": "airflow",
}


def creates_folders(ds, **kwargs):
    BASE_PATH = os.path.join(os.getenv("AIRFLOW_HOME"), "runs", ds)

    subfolders = [
        "raw_data",
        "preprocessed_data",
        "splits_data",
        "models",
    ]

    paths = dict()
    for folder in subfolders:
        path = os.path.join(BASE_PATH, folder)
        os.makedirs(path, exist_ok=True)
        paths[folder] = path
        print(f"Folder {folder} created at {path}")

    ti = kwargs["ti"]
    ti.xcom_push(key="paths", value=paths)


def split_data(ds, **kwargs):
    ti = kwargs["ti"]
    paths = ti.xcom_pull(key="paths", task_ids="create_folders")

    raw_data_path = os.path.join(paths["raw_data"], "data.csv")

    df = pd.read_csv(raw_data_path)

    # aqui eliminamos la columna Y. luego separar los conjuntos

    print("Splitting data into training and validation sets...")
    return


def preprocess_data():
    print("Preprocessing data...")


def train_model():
    print("Training the model...")


def gradio_interface():
    print("Launching Gradio interface for user interaction...")


# Define the DAG
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

    task_split_data = PythonOperator(
        task_id="split_data",
        python_callable=split_data,
        op_kwargs={"ds": "{{ ds }}"},
    )

    task_preprocess_data = PythonOperator(
        task_id="preprocess_data",
        python_callable=preprocess_data,
        op_kwargs={"data_path": "data/split_data.csv"},
    )

    task_train_model = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
        op_kwargs={"model_path": "models/trained_model.pkl"},
    )

    task_gradio_interface = PythonOperator(
        task_id="gradio_interface",
        python_callable=gradio_interface,
    )

    (
        start
        >> task_create_folders
        >> task_split_data
        >> task_preprocess_data
        >> task_train_model
        >> task_gradio_interface
    )
