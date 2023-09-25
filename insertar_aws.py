import csv
import boto3

class InsertarAWS:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, table_name, csv_file):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.table_name = table_name
        self.csv_file = csv_file
        self.last_id_covid = 0

    def obtener_ultimo_id_covid(self):
        # Implementa la lógica para obtener el último valor de Id_covid desde DynamoDB
        # Debe adaptarse a tu estructura de datos en DynamoDB

        # Ejemplo:
        # response = self.dynamodb.scan(
        #     TableName=self.table_name,
        #     Select='COUNT'
        # )
        # last_id_covid = response['Count'] + 1
        # return last_id_covid

        # En lugar de la lógica real, usamos un contador simple para demostrar
        self.last_id_covid += 1
        return self.last_id_covid

    def insertar_datos(self):
        try:
            # Conectar a AWS DynamoDB
            self.dynamodb = boto3.client(
                'dynamodb',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )

            # Abre el archivo CSV y procesa los datos
            with open(self.csv_file, 'r') as archivo_csv:
                lector_csv = csv.DictReader(archivo_csv)
                for fila in lector_csv:
                    # Obtén el próximo valor único para Id_covid
                    id_covid = self.obtener_ultimo_id_covid()

                    # Construye el registro a insertar
                    datos = {
                        'Id_': {'S': f'A{id_covid}'},
                    }

                    # Campos que se manejarán para evitar la inserción de campos vacíos
                    campos_a_manipular = ['age', 'infected_by']

                    # Itera sobre todos los campos del archivo CSV
                    for campo, valor in fila.items():
                        # Verifica si el campo está en la lista de campos a manipular
                        if campo in campos_a_manipular:
                            # Verifica si el valor es numérico antes de insertarlo como número
                            if valor.strip() and valor.replace('.', '', 1).isdigit():
                                datos[campo] = {'N': valor}
                        else:
                            # Inserta los otros campos de manera normal (sin verificación de campo vacío)
                            if valor.strip():
                                datos[campo] = {'S': valor}

                    # Inserta los datos en la tabla DynamoDB
                    self.dynamodb.put_item(
                        TableName=self.table_name,
                        Item=datos
                    )

            print("Datos insertados en AWS DynamoDB correctamente.")
        except Exception as e:
            print(f"Error al insertar datos en AWS DynamoDB: {e}")