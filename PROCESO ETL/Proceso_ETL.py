import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import pymysql

# =====================
# CONFIGURACIÓN DE LA BASE DE DATOS
# =====================
host = '127.0.0.1'
user = 'root'
password = 'AlumnaAdalab'
database = 'Proyecto: Optimización de Talento'

# =====================
# EXTRACT: Funciones para la extracción de datos
# =====================
def extraer_datos_bd(tabla):
    """Extrae datos de una tabla de la base de datos."""
    try:
        conx = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conx.cursor()
        query = f"SELECT * FROM {tabla}"
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        print(f"Datos extraídos de la tabla '{tabla}':")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error al extraer datos de la base de datos: {e}")
        return None
    finally:
        if conx.is_connected():
            cursor.close()
            conx.close()

def extraer_datos_csv(ruta_archivo):
    """Extrae datos desde un archivo CSV."""
    try:
        df = pd.read_csv(ruta_archivo)
        print(f"Datos extraídos del archivo CSV '{ruta_archivo}':")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error al extraer datos del archivo CSV: {e}")
        return None

def extraer_datos_excel(ruta_archivo, hoja=None):
    """Extrae datos desde un archivo Excel."""
    try:
        df = pd.read_excel(ruta_archivo, sheet_name=hoja) if hoja else pd.read_excel(ruta_archivo)
        print(f"Datos extraídos del archivo Excel '{ruta_archivo}':")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error al extraer datos del archivo Excel: {e}")
        return None

# =====================
# TRANSFORM: Funciones para limpiar y transformar los datos
# =====================
def limpiar_transformar_datos(df):
    """Limpia y transforma los datos."""
    try:
        # Eliminar duplicados
        df = df.drop_duplicates()

        # Convertir columnas a minúsculas
        df.columns = df.columns.str.lower()

        # Eliminar valores nulos
        df = df.dropna()

        # Reemplazar caracteres como '$' o ',' en columnas numéricas
        for col in df.select_dtypes(include='object').columns:
            if df[col].str.contains(r'\$|,').any():
                df[col] = df[col].replace({'\$': '', ',': ''}, regex=True).astype(float)

        print("Datos transformados:")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error durante la transformación de datos: {e}")
        return None

# =====================
# LOAD: Funciones para crear y cargar datos en la base de datos
# =====================
def crear_base_datos():
    """Crea la base de datos si no existe."""
    try:
        conx = pymysql.connect(
            host=host,
            user=user,
            password=password
        )
        cursor = conx.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        print(f"Base de datos '{database}' creada o ya existente.")
        conx.close()
    except Exception as e:
        print(f"Error al crear la base de datos: {e}")

def cargar_datos(tabla, df):
    """Carga los datos en una tabla de la base de datos."""
    try:
        # Crear conexión a la base de datos usando SQLAlchemy
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
        df.to_sql(tabla, con=engine, if_exists='append', index=False)
        print(f"Datos cargados exitosamente en la tabla '{tabla}'.")
    except Exception as e:
        print(f"Error al cargar datos en la tabla {tabla}: {e}")

# =====================
# PROCESO ETL COMPLETO
# =====================
def proceso_etl():
    """Ejecuta el proceso ETL completo."""
    # Paso 1: Crear la base de datos
    crear_base_datos()

    # Definición de las tablas y sus fuentes
    fuentes = {
        'empleados': 'empleados.csv',
        'departamentos': 'departamentos.xlsx'
    }

    # Iterar sobre cada fuente de datos
    for tabla, ruta in fuentes.items():
        print(f"\nProcesando la tabla '{tabla}' desde la fuente '{ruta}'...")

        # Paso 2: Extracción
        if ruta.endswith('.csv'):
            datos = extraer_datos_csv(ruta)
        elif ruta.endswith('.xlsx'):
            datos = extraer_datos_excel(ruta)
        else:
            print(f"Formato de archivo desconocido para '{ruta}'.")
            continue

        if datos is None:
            print(f"Error al extraer datos de '{ruta}'. Saltando...")
            continue

        # Paso 3: Transformación
        datos_transformados = limpiar_transformar_datos(datos)
        if datos_transformados is None:
            print(f"Error al transformar datos de '{ruta}'. Saltando...")
            continue

        # Paso 4: Carga
        cargar_datos(tabla, datos_transformados)


