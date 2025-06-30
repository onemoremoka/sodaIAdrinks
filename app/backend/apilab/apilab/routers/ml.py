import logging
import os

from fastapi import APIRouter, Depends, HTTPException
import pandas as pd
import joblib

from apilab.ml_models.model import MLModelService
from apilab.models.ml import MLModel, MLModelIn
from src.preprocessing import (
    df_transacciones_clean,
)  # Usa las mismas que tu entrenamiento

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/inference", response_model=MLModel, status_code=200)
async def predict_model(
    ml_model_input: MLModelIn,
):
    df_input = pd.DataFrame([ml_model_input.dict()])

    # Preprocesa la entrada
    df_input = df_transacciones_clean(df_input, path_output="/tmp")
    print("🟩 df_input preprocesado:", df_input.head())

    # printear la fecha actual: 2025-06-30
    now_ = pd.Timestamp.now().strftime("%Y-%m-%d")

    df_clientes_clean = pd.read_parquet(
        os.path.join(
            "/app/airflow/runs", now_, "preprocessed_data", "clientes_clean.parquet"
        )
    )
    df_productos_clean = pd.read_parquet(
        os.path.join(
            "/app/airflow/runs", now_, "preprocessed_data", "productos_clean.parquet"
        )
    )

    df_merged = df_input.merge(
        df_clientes_clean, on="customer_id", how="left", suffixes=("", "_cliente")
    ).merge(df_productos_clean, on="product_id", how="left", suffixes=("", "_producto"))

    # 5. Eliminar columnas innecesarias (igual que en el DAG de entrenamiento)
    for col in [
        "customer_id",
        "product_id",
        "product_id_mapping",
        "target",
    ]:
        if col in df_merged.columns:
            df_merged = df_merged.drop(columns=[col])

    model_path = os.path.join("/app/airflow/runs", now_, "models", "trained_model.pkl")

    model = joblib.load(model_path)
    predict = model.predict(df_merged)

    return MLModel(
        customer_id=ml_model_input.customer_id,
        product_id=ml_model_input.product_id,
        prediction=predict[0],
        message="Predicción realizada con éxito",
    )
