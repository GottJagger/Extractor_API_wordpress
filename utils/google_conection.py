import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

SCOPES = ['https://www.googleapis.com/auth/drive.file']
PORT = 8080  # Puerto específico

def authenticate_google_api():
    creds = None
    credenciales_path = 'utils/credentials.json'

    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credenciales_path, SCOPES)
            creds = flow.run_local_server(port=PORT)  # Usar puerto específico
        
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    
    return creds

def upload_to_google_sheets(filepath, sheet_name):
    creds = authenticate_google_api()

    try:
        drive_service = build('drive', 'v3', credentials=creds)

        file_metadata = {
            'name': sheet_name,
            'mimeType': 'application/vnd.google-apps.spreadsheet'
        }
        media = MediaFileUpload(filepath, mimetype='text/csv', resumable=True)
        file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        
        print(f'Sheet ID: {file.get("id")}')
        return file.get('id')

    except HttpError as error:
        print(f'An error occurred: {error}')
        return None

# Ejemplo de uso
if __name__ == "__main__":
    csv_filepath = './CSVs/woocommerce_wc_v2_customers.csv'  # Ruta a tu archivo CSV
    sheet_name = 'WooCommerce Customers'
    sheet_id = upload_to_google_sheets(csv_filepath, sheet_name)
    if sheet_id:
        print(f'Google Sheet created successfully with ID: {sheet_id}')
    else:
        print('Failed to create Google Sheet.')
