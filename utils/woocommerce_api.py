import requests
import pandas as pd
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import os
from datetime import datetime, timedelta
from utils.tools import save_df_to_parquet
from utils.google_connection import upload_to_gcs
import logging

# Suprimir la advertencia de solicitud insegura
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class WooCommerceAPI:
    def __init__(self):
        self.consumer_key = os.getenv('WOOCOMMERCE_CONSUMER_KEY')
        self.consumer_secret = os.getenv('WOOCOMMERCE_CONSUMER_SECRET')
        self.base_url = os.getenv('BASE_URL')
        self.bucket_name = os.getenv('GCS_BUCKET_NAME')
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def _fetch_data(self, endpoint, params):
        all_data = []
        try:
            logging.info(f"Fetching data from {endpoint} with params {params}")
            while True:
                response = requests.get(f'{self.base_url}{endpoint}', auth=(self.consumer_key, self.consumer_secret), params=params, verify=False)
                response.raise_for_status()
                data = response.json()
                if isinstance(data, dict) and data.get('code'):
                    logging.error(f"Error: {data['message']}")
                    return None
                all_data.extend(data)
                if len(data) < params.get('per_page', 100):
                    break
                params['page'] += 1

            df = pd.DataFrame(all_data)
            logging.info(f"Data fetched successfully from {endpoint}, total records: {len(df)}")

            # Limpieza y transformación de datos
            for column in df.columns:
                if df[column].apply(type).eq(list).any():
                    df[column] = df[column].apply(lambda x: x if isinstance(x, list) else [x])

            # Guarda la data en Parquet
            logging.info("Saving data to Parquet...")
            parquet_filepath, parquet_filename = save_df_to_parquet(df, endpoint)
            if parquet_filepath:
                logging.info(f"Parquet file saved at {parquet_filepath}")
                # Sube el Parquet a GCS
                parquet_blob_name = f'woocommerce/{parquet_filename}'
                logging.info(f"Uploading {parquet_filepath} to GCS bucket {self.bucket_name} with blob name {parquet_blob_name}")
                upload_to_gcs(self.bucket_name, parquet_filepath, parquet_blob_name)
                logging.info("Upload to GCS completed.")

            return df
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            return None
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return None

    def get_all_customers(self):
        logging.info("Inicio de la extracción de clientes...")
        endpoint = '/wc/v2/customers'
        params = {
            'context': 'view',
            'page': 1,
            'per_page': 100,
            'order': 'asc',
            'role': 'customer'
        }
        return self._fetch_data(endpoint, params)

    def get_all_orders(self):
        logging.info("Inicio de la extracción de pedidos...")
        endpoint = '/wc/v2/orders'
        params = {
            'context': 'view',
            'page': 1,
            'per_page': 100,
            'order': 'desc',
            'orderby': 'date',
            'status': 'any'
        }
        return self._fetch_data(endpoint, params)

    def get_all_products(self):
        logging.info("Inicio de la extracción de productos...")
        endpoint = '/wc/v2/products'
        params = {
            'context': 'view',
            'page': 1,
            'per_page': 100,
            'order': 'desc',
            'orderby': 'date',
            'status': 'any'
        }
        return self._fetch_data(endpoint, params)

    def get_all_reports_sales(self):
        logging.info("Inicio de la extracción de reportes de ventas...")
        endpoint = '/wc/v2/reports/sales'
        params = {
            'context': 'view'
        }
        return self._fetch_data(endpoint, params)

    def get_shipping_zones_locations(self):
        logging.info("Inicio de la extracción de zonas de envío...")
        endpoint = '/wc/v2/shipping/zones'
        params = {}
        return self._fetch_data(endpoint, params)

    def get_wc_analytics(self):
        logging.info("Inicio de la extracción de analítica de WC...")
        endpoint = '/wc-analytics'
        params = {
            'context': 'view',
            'namespace': 'wc-analytics'
        }
        return self._fetch_data(endpoint, params)

    def get_wc_analytics_coupons(self):
        logging.info("Inicio de la extracción de analítica de cupones...")
        endpoint = '/wc-analytics/coupons'
        last_year = (datetime.now() - timedelta(days=365)).isoformat()
        params = {
            'context': 'view',
            'after': last_year,
            'order': 'asc',
            'orderby': 'date',
            'page': 1,
            'per_page': 100
        }
        return self._fetch_data(endpoint, params)

    def get_wc_analytics_orders(self):
        logging.info("Inicio de la extracción de analítica de pedidos...")
        endpoint = '/wc-analytics/orders'
        last_year = (datetime.now() - timedelta(days=365)).isoformat()
        params = {
            'context': 'view',
            'after': last_year,
            'order': 'asc',
            'orderby': 'date',
            'page': 1,
            'per_page': 100
        }
        return self._fetch_data(endpoint, params)

    def get_wc_analytics_products(self, **kwargs):
        logging.info("Inicio de la extracción de analítica de productos...")
        endpoint = '/wc-analytics/products'
        params = {
            'context': 'view',
            'page': 1,
            'per_page': 100,
            'order': 'asc'
        }
        params.update(kwargs)
        return self._fetch_data(endpoint, params)
    
    def extract_woocommerce(self):
        # Ejemplo de uso para endpoint /wc/v2/customers
        result_customers = self.get_all_customers()
        logging.info(result_customers)
        
        # Ejemplo de uso para endpoint /wc/v2/orders
        result_orders = self.get_all_orders()
        logging.info(result_orders)
        
        # Ejemplo de uso para endpoint /wc/v2/products
        result_products = self.get_all_products()
        logging.info(result_products)

        # Ejemplo de uso para endpoint /wc/v2/reports/sales
        result_reports_sales = self.get_all_reports_sales()
        logging.info(result_reports_sales)
        
        # Ejemplo de uso para endpoint /wc/v2/shipping/zones
        result_shipping_zones = self.get_shipping_zones_locations()
        logging.info(result_shipping_zones)
        
        # Ejemplo de uso para endpoint /wc-analytics
        result_analytics = self.get_wc_analytics()
        logging.info(result_analytics)
        
        # Ejemplo de uso para endpoint /wc-analytics/coupons
        result_coupons = self.get_wc_analytics_coupons()
        logging.info(result_coupons)

        # Ejemplo de uso para endpoint /wc-analytics/orders
        result_orders_analytics = self.get_wc_analytics_orders()
        logging.info(result_orders_analytics)
        
        # Ejemplo de uso para endpoint /wc-analytics/products con parámetros personalizados
        result_products_analytics = self.get_wc_analytics_products(after="2023-01-01T00:00:00", before="2024-01-01T00:00:00", category=1)
        logging.info(result_products_analytics)
    
