import os
from typing import Any
import pandas as pd
import shutil


def creates_folders(ds: str, **kwargs: Any) -> None:
    """ " Crea las carpetas necesarias para almacenar los datos y modelos de la iteración actual"""
    BASE_PATH = os.path.join(os.getenv("AIRFLOW_HOME", "."), "runs", ds)

    subfolders = [
        "raw_data",
        "preprocessed_data",
        "splits_data",
        "models",
    ]

    paths = {}
    for folder in subfolders:
        path = os.path.join(BASE_PATH, folder)
        os.makedirs(path, exist_ok=True)
        paths[folder] = path
        print(f"Folder {folder} created at {path}")

    kwargs["ti"].xcom_push(key="paths", value=paths)


def ingestion_data(ds: str, **kwargs: Any) -> None:
    """Agrega los datasets a la carpeta raw_data  y los guarda en un diccionario"""
    ti = kwargs["ti"]
    paths = ti.xcom_pull(key="paths", task_ids="create_folders")
    dataset_names = os.listdir("data")

    # se hace una copia de los datasets a la carpeta raw_data para la k-esima iteración
    src_path = [
        os.path.join("data", name)
        for name in dataset_names
        if name.endswith(".parquet")
    ]

    dest_path = [
        os.path.join(paths["raw_data"], os.path.basename(name)) for name in src_path
    ]

    datasets = dict()
    for src, dest in zip(src_path, dest_path):
        if os.path.exists(dest):
            print(f"File {dest} already exists. Skipping copy.")
            continue

        # se copia el dataset a la carpeta raw_data
        print(f"Copying {src} to {dest}...")
        shutil.copy(src, dest)

        # se leen los datos y se guardan en un Dataframe
        df = pd.read_parquet(dest)
        datasets[os.path.basename(dest)] = df

    kwargs["ti"].xcom_push(key="datasets", value=datasets)

    print(f"Datasets ingested: {list(datasets.keys())}")
