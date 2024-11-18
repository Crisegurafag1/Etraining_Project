import os
import pandas as pd
from sqlalchemy import create_engine, text
from db_connection import get_sqlalchemy_engine, CSV_PATH

def load_and_transform_data(file_name, table_name, column_mappings={}, sep=';'):
    """
    Carga y transforma datos desde un archivo CSV en una tabla de la base de datos.

    Args:
        file_name (str): Nombre del archivo CSV.
        table_name (str): Nombre de la tabla de destino en la base de datos.
        column_mappings (dict): Diccionario opcional para renombrar columnas del CSV.
        sep (str): Delimitador de columnas en el CSV (default es ';').

    Returns:
        None
    """
    engine = get_sqlalchemy_engine()
    try:
        file_path = os.path.join(CSV_PATH, file_name)
        if not os.path.exists(file_path):
            print(f"Error: El archivo {file_name} no existe.")
            return

        df = pd.read_csv(file_path, sep=sep)

        if column_mappings:
            df.rename(columns=column_mappings, inplace=True)

        if table_name == 'cases':
            date_columns = ['date_symptom', 'date_death', 'date_diagnosis', 'date_recovery']
            for col in date_columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        temp_table_name = f"temp_{table_name}"
        df.to_sql(temp_table_name, con=engine, if_exists='replace', index=False)

        with engine.begin() as conn:
            columns = df.columns.tolist()
            update_clause = ", ".join([f"{col}=VALUES({col})" for col in columns])
            insert_query = text(f"""
                INSERT INTO {table_name}
                SELECT * FROM {temp_table_name}
                ON DUPLICATE KEY UPDATE {update_clause};
            """)
            conn.execute(insert_query)
            conn.execute(text(f"DROP TABLE {temp_table_name};"))

        print(f"{table_name.capitalize()} datos cargados con éxito.")

    except Exception as e:
        print(f"Error al cargar datos en la tabla {table_name}: {e}")

def run_etl():
    """
    Ejecuta el proceso ETL para cargar datos desde archivos CSV en la base de datos.
    
    Returns:
        None
    """
    print("Iniciando el proceso ETL")

    column_mappings = {
        'municipality.csv': {'name_municipality': 'name'},
        'type_contagio.csv': {'name': 'name'},
        'gender.csv': {'name': 'name'},
        'status.csv': {'name': 'name'}
    }

    files_tables = [
        ('Department.csv', 'departament'),
        ('municipality.csv', 'municipality'),
        ('type_contagio.csv', 'type_contagio'),
        ('gender.csv', 'gender'),
        ('status.csv', 'status'),
        ('cases.csv', 'cases')
    ]

    for file_name, table_name in files_tables:
        load_and_transform_data(file_name, table_name, column_mappings.get(file_name, {}))

    print("Proceso ETL completado")

if __name__ == "__main__":
    run_etl()
