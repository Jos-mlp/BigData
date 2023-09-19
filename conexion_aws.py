import boto3

def upload_dynamo(data):
    #Configurando las credenciales de AWS
    session = boto3.Session(
        aws_access_key_id="AKIARWNB3T23E3RTZCZD",
        aws_secret_access_key="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",
        region_name="us-east-2"
    )

    #instacioa del cliente dynamoDB
    dynamodb= session.resource('dynamodb')

    #Noimbre de la tabla de dynamoDB
    table_name = 'casos_covid'

    #Crear un objeto de la table
    table = dynamodb.Table(table_name)

    #subir los datos a DynamoDB
    for item in data:
        Id_covid = item['Id_covid']
        Age = item['Age']
        City = item['City']
        Country = item['Country']
        Sex = item['Sex']

        table.put_item(
            Item={
                'Id_covid': Id_covid,
                'Age': Age,
                'City': City,
                'Country': Country,
                'Sex': Sex
            }
        )
    print("Se han subido los datos a dynamoDB")

data = [
    {
        'Id_covid': '1',
        'Age': 30,
        'City': 'Paris',
        'Country': 'France',
        'Sex': 'Male'
    },
    {
        'Id_covid': '2',
        'Age': 60,
        'City': 'Roma',
        'Country': 'Italia',
        'Sex': 'Male'
    },
    {
        'Id_covid': '3',
        'Age': 45,
        'City': 'Madrid',
        'Country': 'Spain',
        'Sex': 'Female'
    }
]

upload_dynamo(data)