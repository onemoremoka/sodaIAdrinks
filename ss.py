import pandas as pd

# Leer el archivo CSV
df = pd.read_csv("tmpe3_7ucyn.csv")

# Seleccionar solo las columnas customer_id y product_id
df_filtered = df[["customer_id", "product_id"]]

# Guardar el resultado en un nuevo archivo CSV
df_filtered.to_csv("filtered_data_pred1.csv", index=False)

# Mostrar las primeras filas para verificar
print("Primeras 5 filas del archivo filtrado:")
print(df_filtered.head())

# Mostrar información del DataFrame filtrado
print(f"\nForma del DataFrame filtrado: {df_filtered.shape}")
print(f"Columnas: {list(df_filtered.columns)}")
