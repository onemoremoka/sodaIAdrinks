import logging
import os
from typing import List
from fastapi import Body
from fastapi import APIRouter, Depends, HTTPException
import pandas as pd
import joblib
from typing import List, Dict, Any


from apilab.ml_models.model import MLModelService
from apilab.models.ml import MLModel, MLModelIn
from src.preprocessing import (
    df_transacciones_clean,
)  # Usa las mismas que tu entrenamiento

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/inference_batch")
async def predict_model_batch(batch_input: List[Dict[str, Any]]):
    if not batch_input:
        raise HTTPException(status_code=400, detail="No input data received")

    # Convertir lista de dict a DataFrame
    df_input = pd.DataFrame(batch_input)

    # Preprocesar todo el batch de una vez
    df_input = df_transacciones_clean(df_input, path_output="/tmp")

    # Fecha actual
    now_ = pd.Timestamp.now().strftime("%Y-%m-%d")

    # Leer datos auxiliares para merge
    clientes_path = os.path.join(
        "/app/airflow/runs", now_, "preprocessed_data", "clientes_clean.parquet"
    )
    productos_path = os.path.join(
        "/app/airflow/runs", now_, "preprocessed_data", "productos_clean.parquet"
    )

    df_clientes_clean = pd.read_parquet(clientes_path)
    df_productos_clean = pd.read_parquet(productos_path)

    # Merge con clientes y productos
    df_merged = df_input.merge(
        df_clientes_clean, on="customer_id", how="left", suffixes=("", "_cliente")
    ).merge(df_productos_clean, on="product_id", how="left", suffixes=("", "_producto"))

    # Drop columnas innecesarias
    for col in ["customer_id", "product_id", "product_id_mapping", "target"]:
        if col in df_merged.columns:
            df_merged = df_merged.drop(columns=[col])

    # Cargar modelo
    model_path = os.path.join("/app/airflow/runs", now_, "models", "trained_model.pkl")
    model = joblib.load(model_path)

    # Predecir batch completo
    predictions = model.predict(df_merged)

    # Armar lista de resultados, emparejando con los datos originales
    results = []
    for input_dict, pred in zip(batch_input, predictions):
        results.append({
            "customer_id": input_dict.get("customer_id"),
            "product_id": input_dict.get("product_id"),
            "prediction": int(pred),
            "message": "Predicción realizada con éxito",
        })

    return results
