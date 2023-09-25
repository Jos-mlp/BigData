import csv
import mysql.connector
from datetime import datetime


class InsertarSQL:
    def __init__(self, host, port, user, password, database, csv_file):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.csv_file = csv_file
        self.conexion = None  # Inicializa la conexión
        self.cursor = None  # Inicializa el cursor

    def insertar_datos(self):
        try:
            # Conectar a la base de datos SQL
            self.conexion = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )

            # Crear el cursor después de la conexión
            self.cursor = self.conexion.cursor()

            # Abre el archivo CSV y procesa los datos
            with open(self.csv_file, 'r', encoding='utf-8') as archivo_csv:
                lector_csv = csv.DictReader(archivo_csv)
                for fila in lector_csv:
                    # Construye la sentencia de inserción
                    datos = {
                        'age': fila['age'] if fila['age'] else None,
                        'city': fila['city'] if fila['city'] else None,
                        'confirmed_date': fila['confirmed_date'] if fila['confirmed_date'] else None,
                        'country': fila['country'] if fila['country'] else None,
                        'date_onset_symptoms': fila['date_onset_symptoms'] if fila['date_onset_symptoms'] else None,
                        'deceased_date': self.validar_fecha(fila['deceased_date']),
                        'infected_by': fila['infected_by'] if fila['infected_by'] else None,
                        'recovery_test': fila['recovery_test'] if fila['recovery_test'] else None,
                        'region': fila['region'] if fila['region'] else None,
                        'sex': fila['sex'] if fila['sex'] else None,
                        'smoking': fila['smoking'] if fila['smoking'] else None,
                        'deceased_date_D': self.validar_fecha(fila['deceased_date_D']),
                    }

                    # Sentencia de inserción SQL
                    consulta = "INSERT INTO covid_data (age, city, confirmed_date, country, date_onset_symptoms, " \
                               "deceased_date, infected_by, recovery_test, region, sex, smoking, deceased_date_D) " \
                               "VALUES (%(age)s, %(city)s, %(confirmed_date)s, %(country)s, " \
                               "%(date_onset_symptoms)s, %(deceased_date)s, %(infected_by)s, %(recovery_test)s, " \
                               "%(region)s, %(sex)s, %(smoking)s, %(deceased_date_D)s)"

                    # Inserta los datos en SQL
                    self.cursor.execute(consulta, datos)
                    self.conexion.commit()

            # Imprime un mensaje para indicar que se han insertado todos los datos
            print("Todos los datos han sido insertados en SQL correctamente.")
        except mysql.connector.Error as mysql_error:
            print(f"Error al insertar datos en SQL: {mysql_error}")
        except Exception as e:
            print(f"Error general: {e}")
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conexion:
                self.conexion.close()

    def validar_fecha(self, fecha_str):
        try:
            # Intenta convertir la cadena a una fecha válida
            fecha = datetime.strptime(fecha_str, '%Y-%m-%d')  # Ajusta el formato según tus datos
            return fecha.strftime('%Y-%m-%d')  # Convierte la fecha de nuevo a cadena con formato
        except ValueError:
            # Si la conversión falla, maneja el error según tus necesidades
            return None  # Puedes devolver None u otro valor predeterminado
