FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir gradio

# Copia el resto del código
COPY . .

# Expón el puerto default de Gradio
EXPOSE 7860

# Ejecuta la app
CMD ["python", "app.py"]
