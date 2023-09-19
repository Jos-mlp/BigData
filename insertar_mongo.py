import pandas as pd
from conexion_mongo import ObtenerConexion  # Importa la función desde el archivo conexion.py

# Nombre del archivo CSV
archivo_csv = 'coronavirus.csv'  # Reemplaza con la ruta de tu archivo CSV

# Cargar los datos desde el archivo CSV en un DataFrame
df = pd.read_csv(archivo_csv, usecols=[
    'age', 'city', 'confirmed_date', 'country', 'date_onset_symptoms',
    'deceased_date', 'infected_by', 'recovery_test', 'region', 'sex', 'smoking', 'deceased_date_D'
])

# Llamar a la función para obtener la colección MongoDB
coleccion = ObtenerConexion()

if coleccion is not None:
    # Insertar los datos en la colección
    registros = df.to_dict(orient='records')
    coleccion.insert_many(registros)
    print(f"Se insertaron {len(registros)} registros en la colección.")
else:
    print("No se pudo conectar a MongoDB. Verifica la configuración en conexion.py.")
