import pandas as pd
from dotenv import load_dotenv
from utils.woocommerce_api import WooCommerceAPI

def main():
    # Cargar las variables de entorno desde el archivo .env
    load_dotenv()
    
    # Llamar al método estático directamente desde la clase
    WooCommerceAPI.extract_woocommerce()

if __name__ == "__main__":
    main()
