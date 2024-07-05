import os
import pandas as pd
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("WooCommerce Data Extraction") \
    .getOrCreate()

def save_df_to_csv(df, endpoint, folder='./CSVs'):
    """
    Guarda un DataFrame en un archivo CSV en una carpeta especificada con un nombre basado en el endpoint y muestra el contenido del DataFrame.

    Args:
    - df (pd.DataFrame): DataFrame de pandas a guardar.
    - endpoint (str): Endpoint utilizado para generar el DataFrame.
    - folder (str): Carpeta donde se guardará el archivo CSV.
    """
    if df.empty:
        return "No data found for the given endpoint.", None

    # Asegurarse de que la carpeta exista
    if not os.path.exists(folder):
        os.makedirs(folder)

    # Generar el nombre del archivo basado en el endpoint
    filename = f'woocommerce_{endpoint.strip("/").replace("/", "_")}.csv'
    filepath = os.path.join(folder, filename)

    # Guardar el DataFrame en un archivo CSV
    df.to_csv(filepath, index=False)

    return filepath, filename

def save_df_to_parquet(df, endpoint, folder='./Parquets'):
    """
    Guarda un DataFrame en un archivo Parquet en una carpeta especificada con un nombre basado en el endpoint y muestra el contenido del DataFrame.

    Args:
    - df (pd.DataFrame): DataFrame de pandas a guardar.
    - endpoint (str): Endpoint utilizado para generar el DataFrame.
    - folder (str): Carpeta donde se guardará el archivo Parquet.
    """
    if df.empty:
        return "No data found for the given endpoint.", None

    # Asegurarse de que la carpeta exista
    if not os.path.exists(folder):
        os.makedirs(folder)

    # Generar el nombre del archivo basado en el endpoint
    filename = f'woocommerce_{endpoint.strip("/").replace("/", "_")}.parquet'
    filepath = os.path.join(folder, filename)

    # Convertir DataFrame de pandas a Spark DataFrame y guardar como Parquet
    spark_df = spark.createDataFrame(df)
    spark_df.write.mode("overwrite").parquet(filepath)

    return filepath, filename

def upload_to_drive(filepath, filename):
    """
    Sube un archivo a Google Drive.

    Args:
    - filepath (str): Ruta del archivo a subir.
    - filename (str): Nombre del archivo en Google Drive.
    """
    SCOPES = ['https://www.googleapis.com/auth/drive.file']
    creds = None
    ruta = 'utils/credencials.json'  # Ajusta la ruta según la ubicación de tu credentials.json
    redirect_uri = 'http://localhost:64162'  # URI de redireccionamiento que deseas utilizar

    # Verifica la existencia del token.json para obtener las credenciales
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    # Si no hay credenciales válidas, inicia el flujo de autorización
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(ruta, SCOPES, redirect_uri=redirect_uri)
            creds = flow.run_local_server(port=0)
        
        # Guarda las credenciales actualizadas en token.json
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        # Crea un cliente de la API de Drive
        service = build('drive', 'v3', credentials=creds)

        # Crea el archivo en Google Drive
        file_metadata = {'name': filename}
        media = MediaFileUpload(filepath, mimetype='text/csv')
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f'File ID: {file.get("id")}')

    except HttpError as error:
        print(f'An error occurred: {error}')
        file = None

    return file.get('id') if file else None
