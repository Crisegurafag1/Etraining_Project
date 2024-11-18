import pandas as pd
import os
from db_connection import get_sqlalchemy_engine
from db_connection import CSV_PATH

def load_and_transform_data(file_name, table_name):
    engine = get_sqlalchemy_engine()
    try:
        df = pd.read_csv(os.path.join(CSV_PATH, file_name))
        if table_name == 'cases':
            df['date_symptom'] = pd.to_datetime(df['date_symptom'], format='%d/%m/%Y %H:%M', errors='coerce')
            df['date_death'] = pd.to_datetime(df['date_death'], format='%d/%m/%Y %H:%M', errors='coerce')
            df['date_diagnosis'] = pd.to_datetime(df['date_diagnosis'], format='%d/%m/%Y %H:%M', errors='coerce')
            df['date_recovery'] = pd.to_datetime(df['date_recovery'], format='%d/%m/%Y %H:%M', errors='coerce')
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"Datos cargados en la tabla {table_name} con éxito.")
    except Exception as e:
        print(f"Error al cargar datos en la tabla {table_name}: {e}")

def run_etl():
    files_tables = {
        'type_contagio.csv': 'type_contagio',
        'cases.csv': 'cases',
        'Departament.csv': 'Departament',
        'municipality.csv': 'municipality',
        'gender.csv': 'gender',
        'status.csv': 'status'
    }
    for file_name, table_name in files_tables.items():
        load_and_transform_data(file_name, table_name)
