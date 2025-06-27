from src.preprocessing import preprocessing_data
import pandas as pd


df = pd.read_parquet("data/transacciones.parquet")

data_path = {
    "clientes": "data/clientes.parquet",
    "transacciones": "data/transacciones.parquet",
    "productos": "data/productos.parquet",
}

preprocessing_data_ = preprocessing_data(
    ds="{{ ds }}",
    data_path=data_path,
    dev=True,  # Set to True for development mode
)
