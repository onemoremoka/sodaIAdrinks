import os
import pandas as pd
from typing import Dict, Any


def df_transacciones_clean(df: pd.DataFrame) -> pd.DataFrame:
    df_without_duplicated = df.drop_duplicates(
        subset=["customer_id", "product_id", "purchase_date", "items"]
    )
    print(f"Duplicados eliminados: {df.shape[0] - df_without_duplicated.shape[0]}")
    df_cleaned = df_without_duplicated[df_without_duplicated["items"] >= 0]
    return df_cleaned


def df_clientes_clean(df: pd.DataFrame) -> pd.DataFrame:
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
    df = df.apply(lambda col: col.astype("category") if col.dtype == "object" else col)
    df_without_duplicated = df.drop_duplicates(
        subset=df.drop(columns=["product_id"]).columns
    )
    print(
        f"dataset productos"
        f"Duplicados eliminados: {df.shape[0] - df_without_duplicated.shape[0]}, "
        f"porcentaje: {((df.shape[0] - df_without_duplicated.shape[0]) / df.shape[0]) * 100:.2f}%"
    )

    df = df_without_duplicated
    # Generar ID único para productos duplicados
    cols_without_id = df.columns.difference(["product_id"])
    df["_dedup_key"] = df[cols_without_id.to_list()].apply(
        lambda row: tuple(row), axis=1
    )
    dedup_reference = df.drop_duplicates(subset=["_dedup_key"])[
        ["_dedup_key", "product_id"]
    ]
    dedup_reference = dedup_reference.rename(
        columns={"product_id": "unique_product_id"}
    )
    df_map_ids = df.merge(dedup_reference, on="_dedup_key", how="left")

    print(f"Total productos unicos: {df_map_ids['unique_product_id'].nunique()}")

    return df_map_ids


def preprocessing_data(ds: str, **kwargs) -> pd.DataFrame:
    ti = kwargs["ti"]
    dataset_paths = ti.xcom_pull(key="datasets", task_ids="ingestion_data")

    df_clean = {}

    for file_name, file_path in dataset_paths.items():
        df = pd.read_parquet(file_path)

        if "clientes" in file_name:
            df_clean["clientes"] = df_clientes_clean(df)
        elif "transacciones" in file_name:
            df_clean["transacciones"] = df_transacciones_clean(df)
        elif "productos" in file_name:
            df_clean["productos"] = df_productos_clean(df)

    # Debug info
    print(df_clean["clientes"].info())
    print(df_clean["transacciones"].info())
    print(df_clean["productos"].info())

    # Merge transacciones con productos
    df_merge_trans_prod = pd.merge(
        df_clean["transacciones"], df_clean["productos"], on="product_id", how="left"
    ).rename(columns={"unique_product_id": "kept_product_id"})

    # Merge con clientes
    df_merge_trans_prod_clt = pd.merge(
        df_merge_trans_prod, df_clean["clientes"], on="customer_id", how="left"
    )

    # Limpiar columnas innecesarias
    if "_dedup_key" in df_merge_trans_prod_clt.columns:
        df_merge_trans_prod_clt.drop(columns=["_dedup_key"], inplace=True)

    # Eliminar columna product_id original si hay conflicto
    if (
        "product_id" in df_merge_trans_prod_clt.columns
        and "kept_product_id" in df_merge_trans_prod_clt.columns
    ):
        df_merge_trans_prod_clt.drop(columns=["product_id"], inplace=True)

    # Renombrar columna final
    df_completed = df_merge_trans_prod_clt.rename(
        columns={"kept_product_id": "product_id"}
    )

    # Validar que no haya columnas duplicadas
    if df_completed.columns.duplicated().any():
        raise ValueError(
            f"Columnas duplicadas detectadas: {df_completed.columns[df_completed.columns.duplicated()].tolist()}"
        )

    # Guardar
    paths = ti.xcom_pull(key="paths", task_ids="create_folders")
    dataframe_path = os.path.join(
        paths["preprocessed_data"], "preprocessed_data.parquet"
    )

    print(f"Guardando DataFrame preprocesado en {dataframe_path}")
    df_completed.to_parquet(dataframe_path, index=False)
