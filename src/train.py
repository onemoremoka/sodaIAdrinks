import os
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder, StandardScaler
from sklearn.preprocessing import FunctionTransformer


def enrich_features(X):
    X = X.copy()

    # Ejemplo: Total de entregas por semana y tipo de cliente
    X["total_deliveries_customer_week"] = X.groupby(["year", "week", "customer_type"])[
        "num_deliver_per_week"
    ].transform("sum")

    X["skus_per_customer_week"] = X.groupby(["year", "week", "customer_type"])[
        "brand"
    ].transform("nunique")

    X["avg_deliveries_brand_week"] = X.groupby(["year", "week", "brand"])[
        "num_deliver_per_week"
    ].transform("mean")

    X["total_deliveries_size_week"] = X.groupby(["year", "week", "size"])[
        "num_deliver_per_week"
    ].transform("sum")
    X["total_deliveries_segment_week"] = X.groupby(["year", "week", "segment"])[
        "num_deliver_per_week"
    ].transform("sum")
    X["ratio_deliveries_sku_customer_week"] = X["num_deliver_per_week"] / X[
        "total_deliveries_customer_week"
    ].replace(0, 1)

    # (Opcional) Puedes quitar columnas intermedias si quieres reducir tamaño
    X = X.drop(
        columns=[
            "total_deliveries_customer_week",
            "total_deliveries_size_week",
            "total_deliveries_segment_week",
        ]
    )
    print("columanas totales: ", X.columns)
    return X


def train_model(model_path: str, **kwargs) -> None:
    if not kwargs["dev"]:
        ti = kwargs["ti"]
        splits_path = ti.xcom_pull(key="paths", task_ids="create_folders")[
            "splits_data"
        ]
    splits_path = kwargs.get("paths")["splits_data"]

    X_train = pd.read_parquet(os.path.join(splits_path, "X_train.parquet"))

    function_transformer = FunctionTransformer(func=enrich_features)

    num_pipeline = Pipeline([
        ("inputer", SimpleImputer(strategy="mean")),
        ("scaler", MinMaxScaler()),
    ])

    cat_pipeline = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])

    # ColumnTransformer
    preprocessor = ColumnTransformer(
        [
            ("num", num_pipeline, numerical_features),
            ("cat", cat_pipeline, categorical_features),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    ).set_output(transform="pandas")

    # Pipeline final
    pipeline_preprocessing = Pipeline([
        ("transformer", function_transformer),
        ("preprocessing", preprocessor),
    ])

    pipeline_training = Pipeline([
        ("preprocessing", pipeline_preprocessing),
        ("classifier", RandomForestClassifier(random_state=42)),
    ])

    joblib.dump(pipeline_training, "pipeline_training.pkl")
