import os
import pandas as pd
from typing import Dict, Any
import numpy as np


def df_transacciones_clean(df: pd.DataFrame) -> pd.DataFrame:
    # formatear purchase_date a datetime
    df["purchase_date"] = pd.to_datetime(df["purchase_date"])

    # se crean columnas: week, year para agrupar
    df["week"] = df["purchase_date"].dt.isocalendar().week
    df["year"] = df["purchase_date"].dt.year

    df = (
        df[df["items"] > 0]
        .groupby(["year", "week", "customer_id", "product_id"])
        .count()
        .reset_index()
        .copy()
    )

    # todo la suma de items es mayor a 0 por construccion del dataset
    df["target"] = df["items"].apply(lambda x: 1 if x > 0 else 0)
    print(
        f"cantidad total de transacciones: {df.shape[0]}, items=1: {df['target'].sum()}"
    )

    # se ordena el dataframe para legibilidad
    df = df.sort_values(by=["year", "week", "customer_id", "product_id"]).reset_index(
        drop=True
    )

    # se eliminan columnas innecesarias
    df = df.drop(columns=["items", "purchase_date", "order_id"])
    df.to_parquet("data/transacciones_clean.parquet", index=False)

    return df


def df_clientes_clean(df: pd.DataFrame) -> pd.DataFrame:
    # convertir columnas de tipo object a category
    df = df.apply(lambda col: col.astype("category") if col.dtype == "object" else col)
    df["region_id"] = df["region_id"].astype("category")
    df["zone_id"] = df["zone_id"].astype("category")

    df_notnulls = df[df["X"].notna()]
    df_without_duplicated = df_notnulls.drop_duplicates(
        subset=df.drop(columns=["customer_id"]).columns
    )
    print(
        f"Duplicados eliminados: {df.shape[0] - df_without_duplicated.shape[0]}, "
        f"porcentaje: {((df.shape[0] - df_without_duplicated.shape[0]) / df.shape[0]) * 100:.2f}%"
    )
    return df_without_duplicated


def df_productos_clean(df: pd.DataFrame) -> pd.DataFrame:
    # Seleccionar las columnas que definen un producto (ignorando product_id)
    cols_identificadoras = [
        "brand",
        "category",
        "sub_category",
        "segment",
        "package",
        "size",
    ]

    # Buscar duplicados basados en esas columnas
    duplicados = df[df.duplicated(subset=cols_identificadoras, keep=False)]

    # Ordenar para facilitar análisis
    duplicados = duplicados.sort_values(by=cols_identificadoras).reset_index(drop=True)
    print(f"Cantidad de productos duplicados: {duplicados.shape[0]}")
    print(f"cantidad de productos únicos: {df.shape[0] - duplicados.shape[0]}")


def preprocessing_data(ds: str, **kwargs) -> pd.DataFrame:
    if not kwargs["dev"]:
        ti = kwargs["ti"]
        dataset_paths = ti.xcom_pull(key="datasets", task_ids="ingestion_data")

    dataset_paths = kwargs.get("data_path", "data/split_data.parquet")
    df_clean = {}

    for file_name, file_path in dataset_paths.items():
        df = pd.read_parquet(file_path)

        if "clientes" in file_name:
            df_clean["clientes"] = df_clientes_clean(df)
        elif "transacciones" in file_name:
            df_clean["transacciones"] = df_transacciones_clean(df)
        elif "productos" in file_name:
            df_clean["productos"] = df_productos_clean(df)


# import pandas as pd

# # Paso 1: Cargar tus datos (si aún no están en memoria)
# # df_productos = pd.read_parquet("ruta/a/tu_archivo.parquet")

# # Paso 2: Definir columnas que identifican un producto
# atributos = df_productos.drop(columns=["product_id"]).columns

# # Paso 3: Crear tabla de productos únicos
# df_productos_unicos = (
#     df_productos
#     .drop_duplicates(subset=atributos)
#     .reset_index(drop=True)
#     .copy()
# )
# df_productos_unicos["standard_product_id"] = range(1, len(df_productos_unicos) + 1)

# # Paso 4: Crear tabla de mapeo (product_id → standard_product_id)
# df_mapeo = df_productos.merge(
#     df_productos_unicos,
#     on=list(atributos),
#     how="left"
# )[["product_id", "standard_product_id"]]

# # Paso 5: Guardar archivos
# df_productos_unicos.to_parquet("data/productos_unicos.parquet", index=False)
# df_mapeo.to_parquet("data/producto_id_mapping.parquet", index=False)
