import os
import pandas as pd
from typing import Any


def split_data(ds: str, **kwargs: Any) -> None:
    ti = kwargs["ti"]
    paths = ti.xcom_pull(key="paths", task_ids="create_folders")

    raw_data_path = os.path.join(paths["raw_data"], "data.csv")

    df = pd.read_csv(raw_data_path)

    # Aquí iría la lógica para separar entrenamiento/validación
    print("Splitting data into training and validation sets...")
