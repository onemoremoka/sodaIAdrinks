import io
import joblib
import pandas as pd


def predict(ds: str, **kwargs) -> None:  # entregarle la semana (t+1)
    if not kwargs["dev"]:
        ti = kwargs["ti"]
        model_path = ti.xcom_pull(key="paths", task_ids="create_folders")["models"]

    file = kwargs.get("file")
    model_path = kwargs.get("model_path", "models/trained_model.pkl")

    pipeline = joblib.load(model_path)
    input_data = pd.read_json(io.BytesIO(file.read()))
    predictions = pipeline.predict(input_data)
    labels = ["No Comprado" if pred == 0 else "Comprado" for pred in predictions]

    return {
        "Predicción": labels[0],
    }
