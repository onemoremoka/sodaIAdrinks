import pandas as pd
import os
import joblib
import numpy as np
from scipy.stats import ks_2samp, chi2_contingency
import configparser

config = configparser.ConfigParser()
config.read('config/config.ini')

def calculate_feature_stats(df, feature, feature_type):
    if feature_type == 'numerical':
        return {
            'mean': df[feature].mean(),
            'std': df[feature].std(),
            'min': df[feature].min(),
            'max': df[feature].max()
        }
    else:  # categorical
        return df[feature].value_counts(normalize=True).to_dict()

def detect_data_drift(processed_path, reference_path):
    # Cargar datos actuales
    current_X = pd.read_parquet(os.path.join(processed_path, 'processed_features.parquet'))
    
    # Cargar estadísticas de referencia
    reference_stats = joblib.load(os.path.join(reference_path, 'reference_stats.joblib'))
    
    # Obtener configuración de características
    numerical_cols = config['FEATURES']['numerical'].split(',')
    categorical_cols = config['FEATURES']['categorical'].split(',')
    drift_threshold = float(config['DRIFT']['threshold'])
    
    drift_detected = False
    
    # Verificar drift en características numéricas (KS test)
    for feature in numerical_cols:
        if feature in current_X.columns:
            current_data = current_X[feature].dropna()
            reference_data = reference_stats[feature]
            
            # KS test
            statistic, p_value = ks_2samp(current_data, reference_data['values'])
            if p_value < drift_threshold:
                print(f"Drift detected in {feature} (p-value: {p_value})")
                drift_detected = True
    
    # Verificar drift en características categóricas (Chi-square)
    for feature in categorical_cols:
        if feature in current_X.columns:
            current_counts = current_X[feature].value_counts(normalize=True)
            reference_dist = reference_stats[feature]
            
            # Alinear categorías
            all_categories = set(current_counts.index) | set(reference_dist.keys())
            current_vector = [current_counts.get(cat, 0) for cat in all_categories]
            reference_vector = [reference_dist.get(cat, 0) for cat in all_categories]
            
            # Chi-square test
            _, p_value, _, _ = chi2_contingency([current_vector, reference_vector])
            if p_value < drift_threshold:
                print(f"Drift detected in {feature} (p-value: {p_value})")
                drift_detected = True
    
    # Actualizar referencia si hay drift
    if drift_detected:
        print("Data drift detected. Updating reference stats...")
        new_reference_stats = {}
        for feature in numerical_cols:
            new_reference_stats[feature] = {
                'values': current_X[feature].sample(min(10000, len(current_X))) if len(current_X) > 10000 else current_X[feature],
                'stats': calculate_feature_stats(current_X, feature, 'numerical')
            }
        for feature in categorical_cols:
            new_reference_stats[feature] = calculate_feature_stats(current_X, feature, 'categorical')
        
        joblib.dump(new_reference_stats, os.path.join(reference_path, 'reference_stats.joblib'))
    
    return drift_detected