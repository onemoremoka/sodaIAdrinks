import random

import gradio as gr


def agent_response(message, history):
    return random.choice(
        [
            "Sí, puedo ayudarte con el análisis de ventas.",
            "Por favor, sube el archivo de ventas para continuar.",
            "¡Perfecto! El archivo se ha recibido correctamente.",
        ]
    )


with gr.Blocks() as demo:
    gr.Markdown("# App MDS - Análisis de Ventas Semanales")
    gr.Markdown(
        "Sube el archivo con las ventas semanales para analizar y visualizar los resultados. "
        "Usa el chatbot para resolver dudas sobre el análisis."
    )
    with gr.Row():
        with gr.Column():
            gr.UploadButton(
                "Subir archivo CSV con las ventas de la semana",
                file_types=[".csv"],
                file_count="single",
                scale=7,
            )

            gr.JSON(
                label="Vista preliminar de la tabla de ventas",
                value={
                    "Fecha": ["2023-01-01", "2023-01-02", "2023-01-03"],
                    "Ventas": [100, 200, 300],
                },
                scale=7,
                show_indices=True,
            )

        with gr.Column():
            gr.ChatInterface(
                agent_response,
                type="messages",
                theme="ocean",
                chatbot=gr.Chatbot(height=300, type="messages"),
                textbox=gr.Textbox(
                    placeholder="Escribe aquí tus consultas sobre el análisis de ventas...",
                    container=False,
                    scale=7,
                ),
            )

if __name__ == "__main__":
    demo.launch(debug=True)
