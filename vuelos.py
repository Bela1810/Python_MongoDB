import pymongo
import time
from datetime import datetime
from bson import ObjectId


class MongoDBClient:
    def __init__(self, uri, db_name):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client.get_database(db_name)

    def calcular_ejecucion(self, inicio, final):
        tiempo_ejecucion = final - inicio
        print(F"TIEMPO DE EJECUCION: {tiempo_ejecucion}")

    def mostrar_documentos(self):
        contar_tiempo = time.time()
        colecciones = self.db.list_collection_names()

        for coleccion_nombre in colecciones:
            coleccion = self.db.get_collection(coleccion_nombre)
            print(f"Documentos en la colección '{coleccion_nombre}':")
            for documento in coleccion.find():
                print(documento)

        fin = time.time()
        print("CONSULTAS")
        self.calcular_ejecucion(contar_tiempo, fin)

    def buscar_vuelos_disponibles(self, fecha_inicio, fecha_fin):
        contar_tiempo = time.time()
        vuelos_disponibles = []
        vuelos_collection = self.db.get_collection("aviones")

        for vuelo in vuelos_collection.find({"estado": "No Reservado"}):
            fecha_vuelo_inicio = datetime.strptime(vuelo["fecha_salida"], "%Y-%m-%d")
            fecha_vuelo_fin = datetime.strptime(vuelo["fecha_llegada"], "%Y-%m-%d")

            if fecha_inicio <= fecha_vuelo_inicio <= fecha_fin or fecha_inicio <= fecha_vuelo_fin <= fecha_fin:
                vuelos_disponibles.append(vuelo)

        fin = time.time()
        print("BUSQUEDA")
        self.calcular_ejecucion(contar_tiempo, fin)
        return vuelos_disponibles

    def insertar_vuelo(self, vuelo_data):
        contar_tiempo = time.time()
        vuelos_collection = self.db.get_collection("aviones")
        insert_result = vuelos_collection.insert_one(vuelo_data)
        fin = time.time()
        print("INSERTAR")
        self.calcular_ejecucion(contar_tiempo, fin)
        return insert_result.inserted_id

    def eliminar_vuelo(self, vuelo_id):
        contar_tiempo = time.time()
        vuelos_collection = self.db.get_collection("aviones")
        delete_result = vuelos_collection.delete_one({"_id": ObjectId(vuelo_id)})
        fin = time.time()
        print("ELIMINAR")
        self.calcular_ejecucion(contar_tiempo, fin)
        return delete_result.deleted_count

    def actualizar_vuelo(self, vuelo_id, nuevos_datos):
        contar_tiempo = time.time()
        vuelos_collection = self.db.get_collection("aviones")
        update_result = vuelos_collection.update_one(
            {"_id": ObjectId(vuelo_id)},
            {"$set": nuevos_datos}
        )
        fin = time.time()
        print("ACTUALIZAR")
        self.calcular_ejecucion(contar_tiempo, fin)
        return update_result.modified_count

    def cerrar_conexion(self):
        self.client.close()


uri = "mongodb+srv://belacaste3:8P2aNJGrxPycwfPu@databaseparadigmas.wx7xmvm.mongodb.net/?retryWrites=true&w=majority" \
      "&appName=DataBaseParadigmas"
db_name = "DataBaseParadigmas"
mongo_client = MongoDBClient(uri, db_name)

mongo_client.mostrar_documentos()

fecha_inicio = datetime(2024, 6, 11)
fecha_fin = datetime(2024, 6, 15)

vuelos_disponibles = mongo_client.buscar_vuelos_disponibles(fecha_inicio, fecha_fin)
print("Busqueda:")
for vuelo in vuelos_disponibles:
    print(vuelos_disponibles)

vuelo_data = {
    "aerolinea": "vivaColombia",
    "origen": "Jose Maria Cordoba",
    "destino": "Madrid",
    "fecha_salida": "2024-06-10",
    "fecha_llegada": "2024-06-15",
    "precio": 800,
    "estado": "No Reservado",
    "adultos": 1,
    "niños": 1
}

nuevos_datos = {
    "aerolinea": "vivaColombia",
    "precio": 800,
    "estado": "Reservado",
    "adultos": 1,
    "niños": 1
}

vuelo_id_insertar = mongo_client.insertar_vuelo(vuelo_data)
print("ID del vuelo insertado:", vuelo_id_insertar)

vuelo_id_eliminar = "6642c43e0a4efffdb742e3c6"
cantidad_eliminada = mongo_client.eliminar_vuelo(vuelo_id_eliminar)
print("Cantidad de vuelos eliminados:", cantidad_eliminada)

vuelo_id_actualizar = "6642c4e80d4f515cf5a70eac"
cantidad_actualizada = mongo_client.actualizar_vuelo(vuelo_id_actualizar, nuevos_datos)
print("Cantidad de vuelos actualizados:", cantidad_actualizada)

mongo_client.cerrar_conexion()
