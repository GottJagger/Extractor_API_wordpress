import pandas as pd
from dotenv import load_dotenv
from utils.woocommerce_api import WooCommerceAPI

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

def main():
    wc_api = WooCommerceAPI()

    # Ejemplo de uso para endpoint /wc/v2/customers
    result_customers = wc_api.get_all_customers()
    print(result_customers)
    
    # Ejemplo de uso para endpoint /wc/v2/orders
    result_orders = wc_api.get_all_orders()
    print(result_orders)
    
    # Ejemplo de uso para endpoint /wc/v2/products
    result_products = wc_api.get_all_products()
    print(result_products)

    # Ejemplo de uso para endpoint /wc/v2/reports/sales
    result_reports_sales = wc_api.get_all_reports_sales()
    print(result_reports_sales)
    
    # Ejemplo de uso para endpoint /wc/v2/shipping/zones
    result_shipping_zones = wc_api.get_shipping_zones_locations()
    print(result_shipping_zones)
    
    # Ejemplo de uso para endpoint /wc-analytics
    result_analytics = wc_api.get_wc_analytics()
    print(result_analytics)
    
    # Ejemplo de uso para endpoint /wc-analytics/coupons
    result_coupons = wc_api.get_wc_analytics_coupons()
    print(result_coupons)

    # Ejemplo de uso para endpoint /wc-analytics/orders
    result_orders_analytics = wc_api.get_wc_analytics_orders()
    print(result_orders_analytics)
    
    # Ejemplo de uso para endpoint /wc-analytics/products con parámetros personalizados
    result_products_analytics = wc_api.get_wc_analytics_products(after="2023-01-01T00:00:00", before="2024-01-01T00:00:00", category=1)
    print(result_products_analytics)

# Ejecutar la función principal
if __name__ == "__main__":
    main()
