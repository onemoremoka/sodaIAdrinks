import os
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder, StandardScaler
from sklearn.preprocessing import FunctionTransformer
from sklearn.metrics import classification_report


def enrich_features(X):
    X = X.copy()

    # Ejemplo: Total de entregas por semana y tipo de cliente
    X["total_deliveries_customer_week"] = X.groupby(
        ["year", "week", "customer_type"], observed=True
    )["num_deliver_per_week"].transform("sum")

    X["skus_per_customer_week"] = X.groupby(
        ["year", "week", "customer_type"], observed=True
    )["brand"].transform("nunique")

    X["avg_deliveries_brand_week"] = X.groupby(
        ["year", "week", "brand"], observed=True
    )["num_deliver_per_week"].transform("mean")

    X["total_deliveries_size_week"] = X.groupby(
        ["year", "week", "size"], observed=True
    )["num_deliver_per_week"].transform("sum")
    X["total_deliveries_segment_week"] = X.groupby(
        ["year", "week", "segment"], observed=True
    )["num_deliver_per_week"].transform("sum")
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


def train_model(ds: str, **kwargs) -> None:
    if not kwargs.get("dev", False):
        ti = kwargs["ti"]
        splits_path = ti.xcom_pull(key="paths", task_ids="create_folders")[
            "splits_data"
        ]
        models_path = ti.xcom_pull(key="paths", task_ids="create_folders")["models"]

    # Carga el DataFrame
    X_val = pd.read_parquet(os.path.join(splits_path, "X_val.parquet"))
    y_val = pd.read_parquet(os.path.join(splits_path, "y_val.parquet"))
    X_train = pd.read_parquet(os.path.join(splits_path, "X_train.parquet"))
    y_train = pd.read_parquet(
        os.path.join(splits_path, "y_train.parquet")
    )  # Asegúrate de tener este archivo

    # Define features numéricos y categóricos
    numerical_features = ["year", "week", "X", "num_deliver_per_week", "size"]

    categorical_features = [
        "customer_type",
        "brand",
        "category",
        "sub_category",
        "segment",
        "package",
    ]

    # Pipelines
    num_pipeline = Pipeline([
        ("imputer", SimpleImputer(strategy="mean")),
        ("scaler", MinMaxScaler()),
    ])

    cat_pipeline = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])

    preprocessor = ColumnTransformer(
        [
            ("num", num_pipeline, numerical_features),
            ("cat", cat_pipeline, categorical_features),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    ).set_output(transform="pandas")

    pipeline_preprocessing = Pipeline([
        ("transformer", FunctionTransformer(func=enrich_features)),
        ("preprocessing", preprocessor),
    ])

    pipeline_training = Pipeline([
        ("preprocessing", pipeline_preprocessing),
        ("classifier", RandomForestClassifier(random_state=42)),
    ])

    print("Pipeline de entrenamiento creado correctamente. Iniciando entrenamiento...")
    # Entrena el pipeline
    pipeline_training.fit(
        X_train, y_train.values.ravel()
    )  # .ravel() por si y_train es dataframe

    # inference y score
    y_pred = pipeline_training.predict(X_val)
    print(classification_report(y_val, y_pred))

    # Guarda el pipeline entrenado
    print("Guardando el modelo entrenado...", models_path)
    joblib.dump(pipeline_training, os.path.join(models_path, "trained_model.pkl"))
