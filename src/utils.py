import os
import shutil


def creates_folders(ds: str, **kwargs) -> None:
    """
    Crea las carpetas necesarias para almacenar los datos y modelos de la iteración actual
    """

    BASE_PATH = os.path.join(os.getenv("AIRFLOW_HOME", "."), "runs", ds)
    print(f"Creating folders in {BASE_PATH} for date {ds}")

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


# emula la funcion de download de los datasets y los guarda en la carpeta raw_data
def ingestion_data(ds: str, **kwargs) -> None:
    """
    Copia y mergea los .parquet de data/ en raw_data
    """

    ti = kwargs["ti"]
    paths = ti.xcom_pull(key="paths", task_ids="create_folders")

    parquet_files = [f for f in os.listdir("data") if f.endswith(".parquet")]

    [
        shutil.copy(os.path.join("data", f), os.path.join(paths["raw_data"], f))
        for f in parquet_files
        if not os.path.exists(os.path.join(paths["raw_data"], f))
    ]

    # pasarr la ruta de los datasets a xcom . no usar read_parquet
    ti.xcom_push(
        key="datasets",
        value={f: os.path.join(paths["raw_data"], f) for f in parquet_files},
    )
