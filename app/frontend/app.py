import gradio as gr
import pandas as pd
import requests
import tempfile
import os

BACKEND_URL = (
    "http://backend_apilab:8000/inference"  # Cambia si usas localhost o port forwarding
)


def predict_from_csv(file):
    df = pd.read_csv(file.name)
    data = df.to_dict(orient="records")  # Lista de dicts con todas las filas

    try:
        response = requests.post(
            BACKEND_URL.replace("/inference", "/inference_batch"), json=data
        )
        if response.status_code == 200:
            results = response.json()
            # Filtrar predicciones positivas si quieres
            results = [r for r in results if r.get("prediction", 0) != 0]
        else:
            results = [{"error": f"Status code {response.status_code}"}]
    except Exception as e:
        results = [{"error": str(e)}]

    if len(results) == 0:
        output_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        pd.DataFrame().to_csv(output_csv.name, index=False)
        return [], output_csv.name

    df_results = pd.DataFrame(results)
    output_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    df_results.to_csv(output_csv.name, index=False)

    return results, output_csv.name


with gr.Blocks() as demo:
    gr.Markdown("# App MDS - Análisis de Ventas Semanales")
    gr.Markdown(
        "Sube un archivo CSV con las ventas semanales para analizar y visualizar los resultados. Solo se mostrarán las predicciones positivas. El archivo debe tener el mismo formato que transacciones.parquet, incluyendo las columnas: customer_id, product_id, order_id, purchase_date y items. Puedes usar el archivo de ejemplo sample.csv que está en la raíz del proyecto."
    )
    with gr.Row():
        with gr.Column():
            file_input = gr.File(
                label="Subir archivo CSV con las ventas de la semana",
                file_types=[".csv"],
            )
            output_json = gr.JSON(label="Resultados de predicción")
            download_button = gr.File(label="Descargar resultados (CSV)")

    file_input.change(
        predict_from_csv, inputs=file_input, outputs=[output_json, download_button]
    )

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860, debug=True)
