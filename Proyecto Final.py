import requests  
import pandas as pd  
from sqlalchemy import create_engine  
import smtplib  
import schedule  
import time  

# Configuraciones  
DB_CONNECTION_STRING = 'postgresql://usuario:contraseña@localhost:5432/tu_base_de_datos'  
API_URL_WEATHER = 'https://api.open-meteo.com/v1/forecast'  
API_URL_ALPHA_VANTAGE = 'https://www.alphavantage.co/query'  
API_KEY = 'TU_API_KEY'  # Reemplaza con tu clave de API de Alpha Vantage  
DATA_WAREHOUSE_CONNECTION_STRING = 'postgresql://usuario:contraseña@tu_data_warehouse:5432/warehouse'  
ALERT_EMAIL = 'destinatario@gmail.com'  
EMAIL_USER = 'tu_email@gmail.com'  
EMAIL_PASS = 'tu_contraseña'  
LIMITE_TEMPERATURA = 30  # Límite para la alerta de temperatura  
STOCK_SYMBOL = 'AAPL'  # Símbolo de la acción (ejemplo: Apple)  

# Funciones de Extracción  
def extract_data_from_db():  
    """Extrae datos desde una base de datos."""  
    engine = create_engine(DB_CONNECTION_STRING)  
    df = pd.read_sql('SELECT * FROM tu_tabla', con=engine)  
    return df  

def extract_data_from_api_weather(city):  
    """Extrae datos desde la API de clima."""  
    params = {  
        'latitude': 35.6895,  # Ejemplo para Tokio  
        'longitude': 139.6917,   
        'current_weather': True  
    }  
    response = requests.get(API_URL_WEATHER, params=params)  
    return pd.json_normalize(response.json()['current_weather'])  

def extract_data_from_alpha_vantage():  
    """Extrae datos desde Alpha Vantage usando func TIME_SERIES_DAILY."""  
    params = {  
        'function': 'TIME_SERIES_DAILY',  
        'symbol': STOCK_SYMBOL,  
        'apikey': API_KEY,  
    }  
    response = requests.get(API_URL_ALPHA_VANTAGE, params=params)  
    data = response.json()  
    
    # Validar la respuesta de la API  
    if "Time Series (Daily)" not in data:  
        print("Error al obtener datos de Alpha Vantage:", data)  
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error  
    
    # Convertir los datos en un DataFrame de Pandas  
    df = pd.json_normalize(data['Time Series (Daily)']).T  
    df.columns = ['open', 'high', 'low', 'close', 'volume']  
    df.index.name = 'date'  
    df.reset_index(inplace=True)  
    df['date'] = pd.to_datetime(df['date'])  
    
    # Convertir los datos a tipo numérico  
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].apply(pd.to_numeric)  

    return df  

# Funciones de Transformación  
def transform_data(df_weather, df_stocks, df_db):  
    """Transforma los datos extraídos combinándolos en un solo DataFrame."""  
    df_combined = pd.concat([df_weather, df_stocks, df_db], axis=1)  
    # Añadir columnas adicionales que sean útiles, como rendimiento de acciones  
    df_combined['rendimiento_acciones'] = df_combined['close'].astype(float).pct_change()  
    return df_combined  

# Funciones de Carga  
def load_data_to_warehouse(df):  
    """Carga datos en el Data Warehouse."""  
    engine = create_engine(DATA_WAREHOUSE_CONNECTION_STRING)  
    df.to_sql('nombre_tabla', con=engine, if_exists='append', index=False)  

# Funciones de Alerta  
def send_alert(email, message):  
    """Envía un correo electrónico de alerta."""  
    server = smtplib.SMTP('smtp.gmail.com', 587)  
    server.starttls()  
    server.login(EMAIL_USER, EMAIL_PASS)  
    server.sendmail(EMAIL_USER, email, message)  
    server.quit()  

def check_alerts(df):  
    """Verifica condiciones para enviar alertas."""  
    if df['temperature'].max() > LIMITE_TEMPERATURA:  
        send_alert(ALERT_EMAIL, '¡Alerta de temperatura! Se ha superado el límite configurado.')  

# Proceso ETL  
def run_etl():  
    """Ejecuta el proceso completo de extracción, transformación y carga."""  
    df_db = extract_data_from_db()  
    df_weather = extract_data_from_api_weather(city='Tokio')  # Cambiar ciudad según sea necesario  
    df_stocks = extract_data_from_alpha_vantage()  
    
    if df_stocks.empty:  # Verificar si no se obtuvieron datos de acciones  
        print("No se pudo obtener datos de acciones.")  
        return  

    df_transformed = transform_data(df_weather, df_stocks, df_db)  
    load_data_to_warehouse(df_transformed)  
    check_alerts(df_transformed)  

# Programar el pipeline ETL para que se ejecute cada hora  
schedule.every().hour.do(run_etl)  

if __name__ == '__main__':  
    while True:  
        schedule.run_pending()  
        time.sleep(1)
        
def backfill_data(df_new, start_date, end_date):  
    """  
    Rellena datos para fechas específicas en un intervalo dado.  
    
    Parameters:  
    - df_new: DataFrame que contiene los nuevos datos a integrar.  
    - start_date: Fecha de inicio del backfill.  
    - end_date: Fecha de finalización del backfill.  
    """  
    # Filtrar el DataFrame para sólo incluir datos en el rango especificado  
    df_backfill = df_new[(df_new['fecha_generacion'] >= start_date) & (df_new['fecha_generacion'] <= end_date)]  
    
    if not df_backfill.empty:  from sqlalchemy import create_engine  

def extract_data_from_another_db():  
    """Conecta a otra base de datos y extrae datos."""  
    # Reemplaza estos parámetros con tus detalles de conexión  
    engine = create_engine('postgresql://usuario:contraseña@host:puerto/base_datos')  
    
    query = "SELECT fecha_generacion, stock_symbol, stock_close, stock_volume FROM otra_tabla"  
    
    df = pd.read_sql(query, con=engine)  
    return df
load_data_to_warehouse(df_backfill)  

# Ejemplo de uso  
backfill_data(df_stocks, '2023-01-01', '2023-10-31')  
CREATE TABLE IF NOT EXISTS datos_ingestados(  
    id SERIAL PRIMARY KEY,  
    fecha_extraccion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  
    fecha_generacion DATE NOT NULL,  
    temperatura FLOAT,  
    stock_symbol VARCHAR(10),  
    stock_close FLOAT,  
    stock_volume BIGINT,  
    clima_descripcion VARCHAR(255),  
    clima_humedad INT,  
    rendimiento_acciones FLOAT,  
    comentarios VARCHAR(255)  )  
DISTSTYLE EVEN -- (opcional) Para distribuir datos equitativamente entre nodos.  
SORTKEY (fecha_generacion, stock_symbol); -- Claves de ordenación para acelerar las consultas filtradas por fecha y símbolo.
from sqlalchemy import create_engine  

def extract_data_from_another_db():  
    """Conecta a otra base de datos y extrae datos."""  
    # Reemplaza estos parámetros con tus detalles de conexión  
    engine = create_engine('postgresql://usuario:contraseña@host:puerto/base_datos')  
    
    query = "SELECT fecha_generacion, stock_symbol, stock_close, stock_volume FROM otra_tabla"  
    
    df = pd.read_sql(query, con=engine)  
    return df
    