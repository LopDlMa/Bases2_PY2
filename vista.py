from pymongo import MongoClient,errors
import os

clearConsole = lambda: os.system('cls' if os.name in ('nt', 'dos') else 'clear')


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


def Presentacion():
    print("------------------------------------------------")
    print("|            BASES DE DATOS 2                  |")
    print("| Dulce Lopez- 201800516                       |")
    print("| Mynor Saban - 201800516                      |")
    print("| Jimmy Noriega - 200915691                    |")
    print("------------------------------------------------\n\n")

def consulta1():
    clearConsole()
    #db.consultas1.find({director: " 'Tim Burton'"})
    director = input('Ingrese el nombre del director : ')   
    val =collection.distinct("primaryTitle",{'director': " '"+director+"'"})
    print("\n Resultado \n")
    for x in val:
        print(x)
    clearConsole()
    menu()

def consulta2():
    clearConsole()
    tipo = input('1. Pelicula Adulto \n2. Pelicula Niño : ') 

    val =collection.distinct("primaryTitle",{'isAdult': " "+tipo+""})
    print("\n Resultado \n")
    for x in val:
        print(x)
    clearConsole()
    menu()


def consulta3():
    clearConsole()
    actor = input('Ingrese el nombre de su Actor : ') 
    val =collection.distinct("primaryTitle",{'cast': " '"+actor+"'"})
    print("\n Resultado \n")
    for x in val:
        print(x)
    clearConsole()
    menu()


def salir():
    exit()


opcionMenu = {  
    1 : consulta1,
    2 : consulta2,
    3 : consulta3,
    4 : salir

}

def menu():
    print("------------------------------------------------")
    print(' 1. Consulta Director \n 2. Consulta Tipo Pelicula \n 3. Consulta de Actores \n 4. salir\n')
    print("------------------------------------------------\n")
    menu1 = input('Ingrese Opcion : ')
    opcionMenu[int(menu1)]()

def init():
    Presentacion();
    menu()

init()