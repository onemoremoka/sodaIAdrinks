import pandas as pd
import os
import joblib
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import numpy as np
import configparser

config = configparser.ConfigParser()
config.read('config/config.ini')

def load_data(input_path):
    transacciones = pd.read_parquet(os.path.join(input_path, 'transacciones.parquet'))
    clientes = pd.read_parquet(os.path.join(input_path, 'clientes.parquet'))
    productos = pd.read_parquet(os.path.join(input_path, 'productos.parquet'))
    return transacciones, clientes, productos

def preprocess_data(transacciones, clientes, productos):
    # Merge datasets
    merged = pd.merge(transacciones, clientes, on='customer_id')
    merged = pd.merge(merged, productos, on='product_id')
    
    # Feature engineering
    merged['purchase_date'] = pd.to_datetime(merged['purchase_date'])
    merged['purchase_week'] = merged['purchase_date'].dt.isocalendar().week
    merged['purchase_year'] = merged['purchase_date'].dt.year
    
    # Target variable: purchased in next week
    merged = merged.sort_values(['customer_id', 'product_id', 'purchase_date'])
    merged['next_week_purchase'] = merged.groupby(['customer_id', 'product_id'])['purchase_date'].shift(-1)
    merged['next_week_purchase'] = (merged['next_week_purchase'] - merged['purchase_date']).dt.days <= 7
    merged['next_week_purchase'] = merged['next_week_purchase'].astype(int)
    
    # Agregar características históricas
    historical = merged.groupby(['customer_id', 'product_id']).agg(
        total_purchases=('items', 'count'),
        avg_items=('items', 'mean'),
        last_purchase=('purchase_date', 'max')
    ).reset_index()
    
    merged = pd.merge(merged, historical, on=['customer_id', 'product_id'])
    
    # Seleccionar características
    features = config['FEATURES']['columns'].split(',')
    target = 'next_week_purchase'
    
    return merged[features], merged[target]

import os
import sys
import logging
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
import joblib

def extract_and_process_data(input_path, output_path):
    try:
        # 1. Validación de rutas
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input path not found: {input_path}")
        
        os.makedirs(output_path, exist_ok=True)
        
        # 2. Carga de datos con logging
        logging.info("Loading data...")
        transacciones, clientes, productos = load_data(input_path)
        
        # 3. Validación básica de datos
        if transacciones.empty or clientes.empty or productos.empty:
            raise ValueError("One or more DataFrames are empty")
            
        # 4. Procesamiento
        logging.info("Preprocessing data...")
        X, y = preprocess_data(transacciones, clientes, productos)
        
        # 5. Guardado con verificación
        logging.info(f"Saving processed data to {output_path}")
        X_path = os.path.join(output_path, 'processed_features.parquet')
        y_path = os.path.join(output_path, 'processed_target.parquet')
        
        X.to_parquet(X_path)
        y.to_parquet(y_path)
        logging.info(f"Files created: {os.listdir(output_path)}")
        
        # 6. Preprocesador
        numerical_cols = config['FEATURES']['numerical'].split(',')
        categorical_cols = config['FEATURES']['categorical'].split(',')
        
        preprocessor = ColumnTransformer(
            transformers=[
                ('num', StandardScaler(), numerical_cols),
                ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_cols)
            ])
        
        preprocessor_path = os.path.join(output_path, 'preprocessor.joblib')
        joblib.dump(preprocessor, preprocessor_path)
        logging.info(f"Preprocessor saved: {preprocessor_path}")
        
        return True
        
    except Exception as e:
        # Captura todos los errores y los registra
        logging.error(f"Error in extract_and_process_data: {str(e)}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logging.error(f"Line {exc_tb.tb_lineno}: {str(e)}")
        raise  # Vuelve a lanzar la excepción para que Airflow la capture