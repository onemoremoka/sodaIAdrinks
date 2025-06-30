from src.preprocessing import preprocessing_data
from src.utils import creates_folders, ingestion_data
from src.loader import split_data
from src.train import train_model
import pandas as pd


df = pd.read_parquet("data/transacciones.parquet")


ds = "13132024"
df_completed_path = "data/df_completed.parquet"

data_path = {
    "raw_data": "runs/13132024/raw_data",
    "preprocessed_data": "runs/13132024/preprocessed_data",
    "splits_data": "runs/13132024/splits_data",
    "models": "runs/13132024/models",
}

datasets = {
    "clientes": "runs/13132024/raw_data/clientes.parquet",
    "transacciones": "runs/13132024/raw_data/transacciones.parquet",
    "productos": "runs/13132024/raw_data/productos.parquet",
}

df_completed_path = {
    "df_completed_path": "runs/13132024/preprocessed_data/df_completed.parquet",
}

task_creates_folders = creates_folders(
    ds=ds,
)

task_ingestion_data = ingestion_data(
    ds=ds,
    data_path=data_path,
    dev=True,  # Set to True for development mode
)


preprocessing_data_ = preprocessing_data(
    ds=ds,
    data_path=datasets,
    paths=data_path,
    dev=True,  # Set to True for development mode
)


split_data_ = split_data(
    ds=ds,
    data_completed_path=df_completed_path,
    paths=data_path,
    dev=True,  # Set to True for development mode
)

train_model_ = train_model(
    ds=ds,
    paths=data_path,
    dev=True,
)
