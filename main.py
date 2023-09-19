import conexion_SQL
import conexion_aws
from conexion_mongo import ObtenerConexion

conexion_mongo = conexion_SQL.ConexionSQL(host="localhost", port=3307, user="root", password="agentepeke", database="casos_covid")
conexion_mongo.conectar()

# Agregar datos a la tabla covid_data
datos_a_insertar = (
30, "Ciudad Ejemplo", "2023-09-19", "País Ejemplo", "2023-09-15", "2023-09-25", 1, "Resultado Prueba", "Región Ejemplo",
"M", "No", "2023-09-25")
conexion_mongo.agregar_datos(datos_a_insertar)

# Ver todos los datos de la tabla covid_data
conexion_mongo.ver_todos_los_datos()

# Cerrar la conexión
conexion_mongo.cerrar_conexion()

#---------------------------------------------------------------------------
conexion_aws = conexion_aws.ConexionAWS(
    aws_access_key_id="AKIARWNB3T23E3RTZCZD",
    aws_secret_access_key="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",
    region_name="us-east-2",
    table_name="casos_covid"
)
conexion_aws.conectar()

# Agregar datos a DynamoDB
datos_a_insertar = (
        30, "Ciudad Ejemplo", "2023-09-19", "País Ejemplo", "2023-09-15",
        "2023-09-25", 1, "Resultado Prueba", "Región Ejemplo", "M", "No", "2023-09-25"
    )
conexion_aws.agregar_datos(datos_a_insertar)

# Listar todos los datos en DynamoDB
print("Datos en la tabla:")
conexion_aws.listar_todos_los_datos()

#Conexion a mongodb y lectura de datos
mi_coleccion = ObtenerConexion()

if mi_coleccion is not None:
    # Obtener todos los documentos de la colección
    todos_los_documentos = mi_coleccion.find()

    # Recorrer y mostrar los documentos
    for documento in todos_los_documentos:
        print(documento)

else:
    print("No se pudo conectar a MongoDB. Verifica la configuración en conexion.py.")
