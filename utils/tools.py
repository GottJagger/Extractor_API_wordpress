import os
import pandas as pd
import json

def clean_column(df, column_name):
    """
    Limpia una columna del DataFrame, asegurando que todos los valores sean cadenas JSON válidas.
    Args:
    - df (pd.DataFrame): DataFrame que contiene la columna a limpiar.
    - column_name (str): Nombre de la columna a limpiar.
    """
    def clean_meta_data(value):
        try:
            # Intenta convertir a JSON
            return json.dumps(value)
        except (TypeError, ValueError):
            # Si falla, devuelve una cadena vacía
            return ''

    if column_name in df.columns:
        df[column_name] = df[column_name].apply(clean_meta_data)

def save_df_to_parquet(df, endpoint, folder='./Parquets'):
    """
    Guarda un DataFrame en un archivo Parquet en una carpeta especificada con un nombre basado en el endpoint.
    Args:
    - df (pd.DataFrame): DataFrame de pandas a guardar.
    - endpoint (str): Endpoint utilizado para generar el DataFrame.
    - folder (str): Carpeta donde se guardará el archivo Parquet.
    """
    if df.empty:
        return "No data found for the given endpoint.", None

    # Limpia las columnas que pueden tener datos incompatibles
    for column in df.columns:
        # Ajusta la columna 'meta_data' según sea necesario
        clean_column(df, column)

    # Asegurarse de que la carpeta exista
    if not os.path.exists(folder):
        os.makedirs(folder)

    # Generar el nombre del archivo basado en el endpoint
    filename = f'woocommerce_{endpoint.strip("/").replace("/", "_")}.parquet'
    filepath = os.path.join(folder, filename)

    # Guardar el DataFrame en un archivo Parquet
    df.to_parquet(filepath, index=False)

    return filepath, filename
