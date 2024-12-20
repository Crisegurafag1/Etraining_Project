import os
from dotenv import load_dotenv
import mysql.connector
from sqlalchemy import create_engine
from mysql.connector import errorcode
from env import DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME, SSL_CA, CSV_PATH

def get_mysql_connection():
    """
    Establecemos una conexión directa a la base de datos MySQL usando `mysql.connector`.
    
    Returns:
        connection: Objeto de conexión a MySQL
    """
    try:
        connection = mysql.connector.connect(
            user=DB_USERNAME,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            ssl_ca=SSL_CA,
            ssl_disabled=False
        )
        print("Conexión exitosa a la base de datos MySQL")
        return connection
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Algo está mal con tu nombre de usuario o contraseña")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("La base de datos no existe")
        else:
            print(f"Error al conectar a la base de datos: {err}")
        return None

def get_sqlalchemy_engine():
    """
    Establece una conexión ORM a la base de datos MySQL utilizando SQLAlchemy.
    
    Returns:
        engine: Objeto de motor de SQLAlchemy para interactuar con la base de datos.
    """
    connection_string = f'mysql+mysqlconnector://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?ssl_ca={SSL_CA}'
    engine = create_engine(connection_string)
    return engine
