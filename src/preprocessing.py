import os
import pandas as pd
from typing import Dict, Any
import numpy as np


def df_transacciones_clean(
    df: pd.DataFrame, path_output: str, ratio_negativos: int = 2
) -> pd.DataFrame:
    """
    ratio_negativos: cantidad máxima de negativos por cada positivo (puedes ajustarlo)
    """
    # formatear purchase_date a datetime
    df["purchase_date"] = pd.to_datetime(df["purchase_date"])

    # se crean columnas: week, year para agrupar
    df["week"] = df["purchase_date"].dt.isocalendar().week
    df["year"] = df["purchase_date"].dt.year

    # Filtrar compras válidas
    df = df[df["items"] > 0]

    # Agrupar y crear columna target
    df = (
        df.groupby(["year", "week", "customer_id", "product_id"])
        .size()
        .reset_index(name="count")
    )
    df["target"] = 1  # Solo hay compras reales en este df

    # --------- AUMENTO DE DATOS BALANCEADO (undersampling de negativos) ----------

    # Agrupar por año, semana y cliente: solo generar negativos para estos
    muestras = []
    for (year, week, customer_id), df_grp in df.groupby([
        "year",
        "week",
        "customer_id",
    ]):
        productos_comprados = set(df_grp["product_id"])
        # productos que podría haber comprado ese cliente esa semana
        productos_todos = df["product_id"].unique()
        # Generar negativos solo para productos que no compró
        productos_neg = [p for p in productos_todos if p not in productos_comprados]
        # Undersampling: elegimos solo algunos
        np.random.shuffle(productos_neg)
        productos_neg = productos_neg[
            : min(ratio_negativos * len(productos_comprados), len(productos_neg))
        ]
        # Crear registros negativos
        for prod in productos_neg:
            muestras.append({
                "year": year,
                "week": week,
                "customer_id": customer_id,
                "product_id": prod,
                "target": 0,
            })
    df_neg = pd.DataFrame(muestras)

    # Juntamos positivos y negativos
    df_final = pd.concat(
        [df[["year", "week", "customer_id", "product_id", "target"]], df_neg],
        ignore_index=True,
    )

    df_final = df_final.sort_values(
        by=["year", "week", "customer_id", "product_id"]
    ).reset_index(drop=True)

    # Eliminar columnas innecesarias si existen
    columnas_drop = [
        col
        for col in ["items", "purchase_date", "order_id", "count"]
        if col in df_final.columns
    ]
    df_final = df_final.drop(columns=columnas_drop, errors="ignore")

    # Guardar
    df_final.to_parquet(
        os.path.join(path_output, "transacciones_clean.parquet"), index=False
    )

    # Imprimir conteos
    n_1 = (df_final["target"] == 1).sum()
    n_0 = (df_final["target"] == 0).sum()
    print(f"Total de registros con target=1 (compra): {n_1}")
    print(f"Total de registros con target=0 (NO compra): {n_0}")

    return df_final


def df_clientes_clean(df: pd.DataFrame, path_output: str) -> pd.DataFrame:
    # convertir columnas de tipo object a category
    df = df.apply(lambda col: col.astype("category") if col.dtype == "object" else col)
    df["region_id"] = df["region_id"].astype("category")
    df["zone_id"] = df["zone_id"].astype("category")

    df_notnulls = df[df["X"].notna()]
    df_without_duplicated = df_notnulls.drop_duplicates(
        subset=df.drop(columns=["customer_id"]).columns
    ).drop(columns=["num_visit_per_week", "zone_id", "region_id"])
    # print(
    #     f"Duplicados eliminados: {df.shape[0] - df_without_duplicated.shape[0]}, "
    #     f"porcentaje: {((df.shape[0] - df_without_duplicated.shape[0]) / df.shape[0]) * 100:.2f}%"
    # )

    print(f"path {path_output}")
    df_without_duplicated.to_parquet(
        os.path.join(path_output, "clientes_clean.parquet"), index=False
    )

    return df_without_duplicated


def df_productos_clean(df: pd.DataFrame, path_output: str) -> pd.DataFrame:
    # Seleccionar las columnas que definen un producto (ignorando product_id)
    columns = df.drop(columns=["product_id"]).columns.to_list()
    print(f"Columnas para eliminar duplicados: {columns}")

    df_uniques = (
        df.drop_duplicates(subset=columns)
        .sort_values(by=columns)
        .reset_index(drop=True)
    ).copy()

    df_mapping = df.merge(
        df_uniques[["product_id"] + columns],
        on=columns,
        how="left",
        suffixes=("", "_mapping"),
    )

    df_mapping.to_parquet(
        os.path.join(path_output, "productos_clean.parquet"), index=False
    )
    df_uniques.to_parquet(
        os.path.join(path_output, "productos_uniques.parquet"), index=False
    )

    return df_mapping


def preprocessing_data(ds: str, **kwargs) -> None:
    if not kwargs["dev"]:
        ti = kwargs["ti"]
        dataset_paths = ti.xcom_pull(key="datasets", task_ids="ingestion_data")
        preprocessed_path = ti.xcom_pull(key="paths", task_ids="create_folders")[
            "preprocessed_data"
        ]

    dataset_paths = kwargs.get("data_path")
    preprocessed_path = kwargs.get("paths")["preprocessed_data"]
    df_clean = {}

    for file_name, file_path in dataset_paths.items():
        df = pd.read_parquet(file_path)

        if "clientes" in file_name:
            df_clean["clientes"] = df_clientes_clean(df, preprocessed_path)
        elif "transacciones" in file_name:
            df_clean["transacciones"] = df_transacciones_clean(df, preprocessed_path)
        elif "productos" in file_name:
            df_clean["productos"] = df_productos_clean(df, preprocessed_path)

    # hacer merge de los dataframes
    df_merged = (
        (
            df_clean["transacciones"]
            .merge(
                df_clean["clientes"],
                on="customer_id",
                how="left",
                suffixes=("", "_cliente"),
            )
            .merge(
                df_clean["productos"],
                on="product_id",
                how="left",
                suffixes=("", "_producto"),
            )
        )
        .drop(
            columns=[
                "customer_id",
                "product_id",
                "product_id_mapping",
            ]
        )
        .copy()
    )

    df_merged.to_parquet(
        os.path.join(preprocessed_path, "df_completed.parquet"), index=False
    )

    # xcom path de los dataframes preprocesados
    if not kwargs["dev"]:
        ti.xcom_push(
            key="df_completed_path",
            value=os.path.join("data", "df_completed.parquet"),
        )
