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
##
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

    def buscar_vehiculos_disponibles(self, fecha_inicio, fecha_fin):
        contar_tiempo = time.time()
        vehiculos_disponibles = []
        vehiculos_collection = self.db.get_collection("vehiculos")

        for vehiculo in vehiculos_collection.find({"estado": "No Reservado"}):
            fecha_vehiculo_inicio = datetime.strptime(vehiculo["fecha_inicio"], "%Y-%m-%d")
            fecha_vehiculo_fin = datetime.strptime(vehiculo["fecha_fin"], "%Y-%m-%d")

            if fecha_inicio <= fecha_vehiculo_inicio <= fecha_fin or fecha_inicio <= fecha_vehiculo_fin <= fecha_fin:
                vehiculos_disponibles.append(vehiculo)

        fin = time.time()
        print("BUSQUEDA")
        self.calcular_ejecucion(contar_tiempo, fin)
        return vehiculos_disponibles

    def insertar_vehiculo(self, vehiculo_data):
        contar_tiempo = time.time()
        vehiculos_collection = self.db.get_collection("vehiculos")
        insert_result = vehiculos_collection.insert_one(vehiculo_data)
        fin = time.time()
        print("INSERTAR")
        self.calcular_ejecucion(contar_tiempo, fin)
        return insert_result.inserted_id

    def eliminar_vehiculo(self, vehiculo_id):
        contar_tiempo = time.time()
        vehiculos_collection = self.db.get_collection("vehiculos")
        delete_result = vehiculos_collection.delete_one({"_id": ObjectId(vehiculo_id)})
        fin = time.time()
        print("ELIMINAR")
        self.calcular_ejecucion(contar_tiempo, fin)
        return delete_result.deleted_count

    def actualizar_vehiculo(self, vehiculo_id, nuevos_datos):
        contar_tiempo = time.time()
        vehiculos_collection = self.db.get_collection("vehiculos")
        update_result = vehiculos_collection.update_one(
            {"_id": ObjectId(vehiculo_id)},
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

vehiculos_disponibles = mongo_client.buscar_vehiculos_disponibles(fecha_inicio, fecha_fin)
print("Busqueda:")
for vehiculo in vehiculos_disponibles:
    print(vehiculos_disponibles)

vehiculo_data = {
    "año": 2022,
    "fecha_inicio": "2024-07-10",
    "fecha_fin": "2024-07-15",
    "precio": 800,
    "ciudad": "Barcelona",
    "estado": "No Reservado",
    "modelo": "Toyota"
}


nuevos_datos = {
    "estado": "No Reservado",
}

vehiculo_id_insertar = mongo_client.insertar_vehiculo(vehiculo_data)
print("ID del vehiculo insertado:", vehiculo_id_insertar)

vehiculo_id_eliminar = "6642c43e0a4efffdb742e3c6"
cantidad_eliminada = mongo_client.eliminar_vehiculo(vehiculo_id_eliminar)
print("Cantidad de vehiculos eliminados:", cantidad_eliminada)

vehiculo_id_actualizar = "6642c4e80d4f515cf5a70eac"
cantidad_actualizada = mongo_client.actualizar_vehiculo(vehiculo_id_actualizar, nuevos_datos)
print("Cantidad de vehiculos actualizados:", cantidad_actualizada)

mongo_client.cerrar_conexion()