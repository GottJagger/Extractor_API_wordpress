import requests
import pandas as pd
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import os
from datetime import datetime, timedelta
from utils.tools import save_df_to_csv, save_df_to_parquet, upload_to_drive

# Suppress only the single InsecureRequestWarning from urllib3
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class WooCommerceAPI:
    def __init__(self):
        self.consumer_key = os.getenv('WOOCOMMERCE_CONSUMER_KEY')
        self.consumer_secret = os.getenv('WOOCOMMERCE_CONSUMER_SECRET')
        self.base_url = os.getenv('BASE_URL')

    def _fetch_data(self, endpoint, params):
        all_data = []
        try:
            while True:
                response = requests.get(f'{self.base_url}{endpoint}', auth=(self.consumer_key, self.consumer_secret), params=params, verify=False)
                response.raise_for_status()
                data = response.json()
                if isinstance(data, dict) and data.get('code'):
                    print(f"Error: {data['message']}")
                    return f"Error: {data['message']}"
                all_data.extend(data)
                if len(data) < params.get('per_page', 100):
                    break
                params['page'] += 1

            df = pd.DataFrame(all_data)
            
            #crea los CSV
            csv_filepath, csv_filename = save_df_to_csv(df, endpoint)
            
            # guarda los csv al drive 
            # TODO: toca solucionar inconveniente del guardado
            # upload_to_google_sheets(csv_filepath, csv_filename)
            
            # guarda la data en parquets
            save_df_to_parquet(df, endpoint)

            return df
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return f"Request failed: {e}"
        except Exception as e:
            print(f"An error occurred: {e}")
            return f"An error occurred: {e}"

    def get_all_customers(self):
        print("Inicio de la extracción de clientes...")
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
        print("Inicio de la extracción de pedidos...")
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
        print("Inicio de la extracción de productos...")
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
        print("Inicio de la extracción de reportes de ventas...")
        endpoint = '/wc/v2/reports/sales'
        params = {
            'context': 'view'
        }
        return self._fetch_data(endpoint, params)

    def get_shipping_zones_locations(self):
        print("Inicio de la extracción de zonas de envío...")
        endpoint = '/wc/v2/shipping/zones'
        params = {}
        return self._fetch_data(endpoint, params)

    def get_wc_analytics(self):
        print("Inicio de la extracción de analítica de WC...")
        endpoint = '/wc-analytics'
        params = {
            'context': 'view',
            'namespace': 'wc-analytics'
        }
        return self._fetch_data(endpoint, params)

    def get_wc_analytics_coupons(self):
        print("Inicio de la extracción de analítica de cupones...")
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
        print("Inicio de la extracción de analítica de pedidos...")
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
        print("Inicio de la extracción de analítica de productos...")
        endpoint = '/wc-analytics/products'
        params = {
            'context': 'view',
            'page': 1,
            'per_page': 100,
            'order': 'asc'
        }
        params.update(kwargs)  # Añadir los parámetros proporcionados
        return self._fetch_data(endpoint, params)
