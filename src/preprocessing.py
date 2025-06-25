from typing import Any
import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin


# funcion auxiliar para el preprocesamiento
class IQR(BaseEstimator, TransformerMixin):
    def __init__(self, lambda_=1.5):
        self.lambda_ = lambda_

    def fit(self, X, y=None):
        q1 = X.quantile(0.25)
        q3 = X.quantile(0.75)
        iqr = q3 - q1
        self.lower_bound_ = q1 - self.lambda_ * iqr
        self.upper_bound_ = q3 + self.lambda_ * iqr
        return self

    def transform(self, X):
        X_transformed = X.copy()
        for col in X.columns:
            X_transformed.loc[
                (X[col] < self.lower_bound_[col]) | (X[col] > self.upper_bound_[col]),
                col,
            ] = np.nan
        return X_transformed

    def count_outliers(self, X):
        if isinstance(X, pd.Series):
            X = X.to_frame()
        resumen = []
        for col in X.columns:
            if col in self.lower_bound_:
                outliers = (X[col] < self.lower_bound_[col]) | (
                    X[col] > self.upper_bound_[col]
                )
                resumen.append({
                    "columna": col,
                    "outliers": outliers.sum(),
                    "porcentaje": 100 * outliers.sum() / len(X),
                })
        return pd.DataFrame(resumen).sort_values(by="porcentaje", ascending=False)

    def get_feature_names_out(self, input_features=None):
        return input_features


def preprocessing_data(ds: str, **kwargs) -> None:
    """Preprocesa los datos y entrena un modelo de Random Forest"""
    ti = kwargs["ti"]
    df_paths = ti.xcom_pull(key="datasets", task_ids="ingestion_data")
    print(df_paths)
