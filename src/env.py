import os
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo config.env ubicado en la raíz del proyecto
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config.env'))
print(f"Cargando variables de entorno desde: {dotenv_path}")

load_dotenv(dotenv_path)

DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
SSL_CA = os.getenv('SSL_CA')
CSV_PATH = os.getenv('CSV_PATH')

print(f"DB_PORT cargado: {DB_PORT}")
print(f"CSV_PATH cargado: {CSV_PATH}")

# Asegúrate de convertir DB_PORT a int antes de usarlo
if DB_PORT is not None:
    DB_PORT = int(DB_PORT)
else:
    print("Error: La variable de entorno DB_PORT no está definida")
