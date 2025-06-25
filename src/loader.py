import os
import pandas as pd
from typing import Any
from sklearn.model_selection import train_test_split

SEED = 42


def split_temporal(
    df, target_col, test_size=0.1, val_size=0.2, sort_by="purchase_date", seed=SEED
):
    df_sorted = df.sort_values(by=sort_by)
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


def split_data(ds: str, **kwargs: Any) -> None:
    ti = kwargs["ti"]
    dfs = ti.xcom_pull(key="datasets", task_ids="ingestion_data")

    for name, df in dfs.items():
        pass
