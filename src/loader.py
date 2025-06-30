import os
import pandas as pd
from typing import Any
from sklearn.model_selection import train_test_split

SEED = 42


def split_temporal(
    df, target_col, test_size=0.1, val_size=0.2, sort_by="purchase_date", seed=SEED
):
    df_sorted = df.sort_values(by=["year", "week"], ascending=True)
    X = df_sorted.drop(columns=[target_col])
    y = df_sorted[target_col]

    # Separar test
    X_temp, X_test, y_temp, y_test = train_test_split(
        X, y, test_size=test_size, random_state=seed, shuffle=False
    )

    # Separar validation del restante
    val_prop = val_size / (1 - test_size)
    X_train, X_val, y_train, y_val = train_test_split(
        X_temp, y_temp, test_size=val_prop, random_state=seed, shuffle=False
    )
    return X_train, X_val, X_test, y_train, y_val, y_test


def split_data(ds: str, **kwargs) -> None:
    ti = kwargs["ti"]
    df_completed_path = ti.xcom_pull(
        key="df_completed_path", task_ids="preprocessing_data"
    )
    splits_path = ti.xcom_pull(key="paths", task_ids="create_folders")["splits_data"]
    df = pd.read_parquet(df_completed_path)

    X_train, X_val, X_test, y_train, y_val, y_test = split_temporal(
        df,
        target_col="target",
        test_size=0.1,
        val_size=0.2,
        sort_by="purchase_date",
        seed=SEED,
    )

    # Guardar los splits
    X_train.to_parquet(os.path.join(splits_path, "X_train.parquet"), index=False)
    X_val.to_parquet(os.path.join(splits_path, "X_val.parquet"), index=False)
    X_test.to_parquet(os.path.join(splits_path, "X_test.parquet"), index=False)

    print(f"informacion X_train: {X_train.shape} columnas: {X_train.columns} \n")
    y_train.to_frame().to_parquet(
        os.path.join(splits_path, "y_train.parquet"), index=False
    )
    y_val.to_frame().to_parquet(os.path.join(splits_path, "y_val.parquet"), index=False)
    y_test.to_frame().to_parquet(
        os.path.join(splits_path, "y_test.parquet"), index=False
    )
