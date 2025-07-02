import os
import mlflow
import mlflow.sklearn
import optuna
import joblib
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.metrics import f1_score, roc_auc_score, precision_score, recall_score
from optuna.samplers import TPESampler
import shap
import matplotlib.pyplot as plt
import configparser

config = configparser.ConfigParser()
config.read('config/config.ini')

def objective(trial, X, y):
    # Hiperparámetros a optimizar
    params = {
        'n_estimators': trial.suggest_int('n_estimators', 100, 1000),
        'max_depth': trial.suggest_int('max_depth', 3, 15),
        'min_samples_split': trial.suggest_int('min_samples_split', 2, 20),
        'min_samples_leaf': trial.suggest_int('min_samples_leaf', 1, 10),
        'bootstrap': trial.suggest_categorical('bootstrap', [True, False]),
        'class_weight': trial.suggest_categorical('class_weight', ['balanced', None])
    }
    
    # Validación cruzada
    cv = StratifiedKFold(n_splits=3, shuffle=True, random_state=42)
    scores = []
    
    for train_idx, val_idx in cv.split(X, y):
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
        
        model = RandomForestClassifier(**params, random_state=42, n_jobs=-1)
        model.fit(X_train, y_train)
        
        y_pred = model.predict(X_val)
        y_pred_proba = model.predict_proba(X_val)[:, 1]
        
        f1 = f1_score(y_val, y_pred)
        auc = roc_auc_score(y_val, y_pred_proba)
        scores.append((f1 + auc) / 2)  # Combinación de métricas
    
    return np.mean(scores)

def train_model(processed_path, model_path):
    # Cargar datos procesados
    X = pd.read_parquet(os.path.join(processed_path, 'processed_features.parquet'))
    y = pd.read_parquet(os.path.join(processed_path, 'processed_target.parquet')).squeeze()
    
    # Cargar preprocesador
    preprocessor = joblib.load(os.path.join(processed_path, 'preprocessor.joblib'))
    
    # Preprocesar datos
    X_preprocessed = preprocessor.transform(X)
    feature_names = (preprocessor.named_transformers_['cat'].get_feature_names_out().tolist() + 
                     preprocessor.transformers_[0][2])
    
    # Configurar MLflow
    mlflow.set_tracking_uri(config['MLFLOW']['tracking_uri'])
    mlflow.set_experiment("sodai_product_prediction")
    
    with mlflow.start_run():
        # Optimización de hiperparámetros con Optuna
        study = optuna.create_study(
            direction='maximize',
            sampler=TPESampler(seed=42)
        )
        study.optimize(lambda trial: objective(trial, X_preprocessed, y), n_trials=30)
        
        # Entrenar modelo final con mejores parámetros
        best_params = study.best_params
        model = RandomForestClassifier(**best_params, random_state=42, n_jobs=-1)
        model.fit(X_preprocessed, y)
        
        # Evaluación
        y_pred = model.predict(X_preprocessed)
        y_pred_proba = model.predict_proba(X_preprocessed)[:, 1]
        
        # Registrar métricas
        mlflow.log_params(best_params)
        mlflow.log_metric("f1_score", f1_score(y, y_pred))
        mlflow.log_metric("roc_auc", roc_auc_score(y, y_pred_proba))
        mlflow.log_metric("precision", precision_score(y, y_pred))
        mlflow.log_metric("recall", recall_score(y, y_pred))
        
        # Guardar modelo
        mlflow.sklearn.log_model(model, "model")
        joblib.dump(model, os.path.join(model_path, 'sodai_model.joblib'))
        
        # Interpretabilidad con SHAP
        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_preprocessed)
        
        # Gráfico de importancia global
        plt.figure(figsize=(10, 8))
        shap.summary_plot(shap_values[1], X_preprocessed, feature_names=feature_names, show=False)
        plt.title('SHAP Feature Importance')
        plt.tight_layout()
        mlflow.log_figure(plt.gcf(), "shap_feature_importance.png")
        plt.close()
        
        # Gráfico de dependencia para la característica más importante
        most_important_feature = feature_names[np.argmax(np.abs(shap_values[1]).mean(axis=0))]
        plt.figure(figsize=(10, 6))
        shap.dependence_plot(
            most_important_feature,
            shap_values[1],
            X_preprocessed,
            feature_names=feature_names,
            show=False
        )
        plt.title(f'Dependence Plot for {most_important_feature}')
        mlflow.log_figure(plt.gcf(), "shap_dependence_plot.png")
        plt.close()