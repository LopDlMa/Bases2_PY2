import pyodbc
import json
from pymongo import MongoClient,errors
from time import sleep
from daemonize import Daemonize

pid = "/tmp/test2.pid"

# CONEXION A SQL SERVER
direccion_servidor = 'pro2vacasbases2.database.windows.net'
nombre_bd = 'proyecto2'
nombre_usuario = 'proyecto'
password = 'Prueba1.'
try:
    conexion = pyodbc.connect('Driver={ODBC Driver 18 for SQL Server};Server=tcp:secundariovacas.database.windows.net,1433;Database=proyecto2;Uid=proyecto;Pwd=Prueba1.;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;' )
    print("CONECTADO A SQL")
    # OK! conexión exitosa
except Exception as e:
    # Atrapar error
    print("Ocurrió un error al conectar a SQL Server: ", e)


### CONEXION A MONGO

MONGODB_DATABASE= 'proyecto1'
MONGODB_COLLECTION= 'consultas1'
URI_CONNECTION =  "mongodb://34.125.83.244:27017"
    #db.createUser({user:"bases21",pwd:"bases2!12547l.deUsacBases2@",roles:[{role:"readWrite",db:"proyecto1"}]});
    # contra bases2@12547l.deUsacBases2@ñ
    #usuario bases2
  


try:
    client = MongoClient(URI_CONNECTION)
    client.server_info()
    print ('CONECTADO A MONGO')
    
    
except errors.ServerSelectionTimeoutError as error:
    print ('Error with MongoDB connection: %s' % error)
except errors.ConnectionFailure as error:
    print ('Could not connect to MongoDB: %s' % error)

collection = client[MONGODB_DATABASE][MONGODB_COLLECTION]


consulta = """
            SELECT t.id, t.primaryTitle, t.isAdult, t.startYear, t.runtime, g.name,p.id, n.primaryName as Actor
            , d.Name_id as IDDIRECTOR, (Select primaryName from Name where id = d.Name_id) as Director
            from Title t
            inner join TitleGenre tg
            on t.id = tg.Title_id
            inner join Genre g
            on g.id = tg.Genre_id
            inner join Principal p
            on p.Title_id = t.id
            inner join [dbo].[Name] n
            on n.id = p.Name_id
            inner join [dbo].[Diretor] d
            on d.Title_id = t.id 
            ;
"""


def query(sql):
    try:
        with conexion.cursor() as cursor:
            # En este caso no necesitamos limpiar ningún dato
            cursor.execute(sql)

            # Con fetchall traemos todas las filas
            dataGeneral = cursor.fetchall()
            for data in dataGeneral:
                stringRow = str(data)
                sin = stringRow.replace('(','')
                split1 = sin.split(')')
                split2 = split1[0].split(",")
                # insertar datos a mongo
                pelicula = {
                    "idPelicula" : split2[0],
                    "primaryTitle": split2[1],
                    "isAdult": split2[2],
                    "startYear": split2[3],
                    "runTime": split2[4],
                    "genre": split2[5],
                    "cast": split2[7],
                    "director": split2[9],
                }

                mongo(pelicula)

    except Exception as e:
        print("Ocurrió un error al consultar: ", e)
        conexion.close()

def mongo(data):
    try:
        collection.insert_one(data)
    except Exception as error:
        print ("Error saving data: %s" % str(error))

def start():
    while True:
        collection.drop()
        query(consulta)
        print("Funcionando")
        sleep(50)

daemon = Daemonize(app="test_app", pid=pid, action=start())
daemon.start()    

