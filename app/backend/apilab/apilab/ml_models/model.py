import os
import cloudpickle
from typing import Any
import joblib
from apilab.models.ml import MLModel, MLModelIn


def get_latest_model_path() -> str:
    """
    Busca el modelo entrenado más reciente dentro de /app/airflow/runs.
    """
    models_dir = "/app/airflow/runs"
    # Solo considera carpetas que tengan el modelo
    dates = sorted(os.listdir(models_dir), reverse=True)
    for date in dates:
        model_path = os.path.join(models_dir, date, "models", "trained_model.pkl")
        if os.path.exists(model_path):
            print(f"🟩 Encontrado modelo: {model_path}")
            return model_path
    raise FileNotFoundError("No trained_model.pkl found in any run date.")


MODEL_PATH = get_latest_model_path()
print("✅ MLModelService cargado correctamente", MODEL_PATH)


class MLModelService:
    _instance = None

    def __init__(self, model_path: str = MODEL_PATH):
        self.model_path = model_path
        print(
            f"Instantiating MLModelService with model path: {self.model_path}. Exists: {os.path.exists(self.model_path)}"
        )
        self.model = self._load_model()

    @classmethod
    def get_instance(cls) -> "MLModelService":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _load_model(self) -> Any:
        print(f"🟩 Cargando modelo desde: {self.model_path}")
        if not os.path.exists(self.model_path):
            raise FileNotFoundError(
                f"El modelo no existe en la ruta: {self.model_path}"
            )
        try:
            with open(self.model_path, "rb") as f:
                model = joblib.load(f)
            print("🟢 Modelo cargado correctamente")
            return model
        except Exception as e:
            print(f"🔴 Error al cargar el modelo: {e}")
            raise

    async def predict(self, ml_model_in: MLModelIn) -> MLModel:
        features = [
            [
                ml_model_in.customer_id,
                ml_model_in.product_id,
                ml_model_in.order_id,
                ml_model_in.purchase_date,
                ml_model_in.items,
            ]
        ]
        print(f"Predicting with features: {features}")

        prediction = int(self.model.predict(features)[0])
        print(f"Prediction result: {prediction}")

        return MLModel(
            customer_id=ml_model_in.customer_id,
            product_id=ml_model_in.product_id,
            prediction=prediction,
            message="Predicción realizada con éxito",
        )
