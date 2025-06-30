# sodaIAdrinks

Sistema de análisis y predicción de datos de ventas de bebidas utilizando Apache Airflow, FastAPI y Gradio.

## Estructura del Proyecto

- **`src/`**: Código fuente principal (preprocessing, training, predicción)
- **`airflow/`**: Configuración y DAGs de Apache Airflow para orquestación de ML
- **`app/`**: Aplicación web con backend (FastAPI) y frontend (Gradio)
- **`data/`**: Datos de entrada (clientes, productos, transacciones)
- **`notebooks/`**: Jupyter notebooks para análisis exploratorio

## Modo de Uso

### � Ejecución con Docker Compose

Este proyecto está diseñado para ejecutarse únicamente mediante Docker Compose, que automatiza la creación de todas las interfaces y servicios necesarios.

#### Opción 2: Solo Aplicación Web
```bash
# Solo backend FastAPI y frontend Gradio y Airflow
docker compose build up
```

**Interfaces disponibles:**
- **Airflow UI**: http://localhost:8080 (orquestación de pipelines ML)
- **FastAPI Backend**: http://localhost:8000 (API REST para predicciones)
- **Gradio Frontend**: http://localhost:7860 (interfaz web interactiva)

### 📊 Flujo de Trabajo

1. **Ingesta de datos**: Los datos se procesan desde `data/` hacia `airflow/runs/`
2. **Entrenamiento**: Airflow ejecuta el pipeline de ML automáticamente
3. **Predicción**: API backend sirve el modelo entrenado
4. **Interfaz**: Frontend Gradio permite interacciones amigables

### 🛠️ Comandos Útiles

```bash
# Ver logs de contenedores
docker compose logs -f [servicio]

# Reiniciar servicios
docker compose restart

# Detener todos los servicios
docker compose down

# Rebuild de imágenes
docker compose up --build
```
## airflow

mas detalles en [airflow/README.md](airflow/README.md)

Se implementan las funciones para realizar el drift detection "drift_detection.py" y el reentrenamiento automatico en model_training.py ambos en la carpeta airflow. No se agregar al pipeline de datos por tiempo. Se espera agregar para la ultima etapa.
