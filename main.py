import pymongo
from datetime import datetime
from bson import ObjectId
import time


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

    def buscar_hoteles_disponibles(self, fecha_inicio, fecha_fin):
        contar_tiempo = time.time()
        hoteles_disponibles = []
        hoteles_collection = self.db.get_collection("hoteles")

        for hotel in hoteles_collection.find():
            fecha_hotel_inicio = datetime.strptime(hotel["fecha_inicio"], "%Y-%m-%d")
            fecha_hotel_fin = datetime.strptime(hotel["fecha_final"], "%Y-%m-%d")

            if fecha_inicio <= fecha_hotel_inicio <= fecha_fin or fecha_inicio <= fecha_hotel_fin <= fecha_fin:
                hoteles_disponibles.append(hotel)

        fin = time.time()
        print("BUSQUEDA")
        self.calcular_ejecucion(contar_tiempo, fin)
        return hoteles_disponibles

    def insertar_hotel(self, hotel_data):
        contar_tiempo = time.time()
        hoteles_collection = self.db.get_collection("hoteles")
        insert_result = hoteles_collection.insert_one(hotel_data)
        fin = time.time()
        print("INSERTAR")
        self.calcular_ejecucion(contar_tiempo, fin)
        return insert_result.inserted_id

    def eliminar_hotel(self, hotel_id):
        contar_tiempo = time.time()
        hoteles_collection = self.db.get_collection("hoteles")
        delete_result = hoteles_collection.delete_one({"_id": ObjectId(hotel_id)})
        fin = time.time()
        print("ELIMINAR")
        self.calcular_ejecucion(contar_tiempo, fin)
        return delete_result.deleted_count

    def actualizar_hotel(self, hotel_id, nuevos_datos):
        contar_tiempo = time.time()
        hoteles_collection = self.db.get_collection("hoteles")
        update_result = hoteles_collection.update_one(
            {"_id": ObjectId(hotel_id)},
            {"$set": nuevos_datos}
        )
        fin = time.time()
        print("INSERTAR")
        self.calcular_ejecucion(contar_tiempo, fin)
        return update_result.modified_count

    def cerrar_conexion(self):
        self.client.close()


uri = "mongodb+srv://belacaste3:8P2aNJGrxPycwfPu@databaseparadigmas.wx7xmvm.mongodb.net/?retryWrites=true&w=majority" \
      "&appName=DataBaseParadigmas"
db_name = "DataBaseParadigmas"
mongo_client = MongoDBClient(uri, db_name)

mongo_client.mostrar_documentos()

fecha_inicio = datetime(2024, 5, 2)
fecha_fin = datetime(2024, 5, 6)

hoteles_disponibles = mongo_client.buscar_hoteles_disponibles(fecha_inicio, fecha_fin)

print("Busqueda:")
for hotel in hoteles_disponibles:
    print(hotel)

hotel_data = {
    "nombre": "Hotel Arena",
    "ubicacion": "Cartagena",
    "precio_por_noche": 750,
    "estrellas": 5,
    "servicios": ["Wifi", "Piscina", "Gimnasio"],
    "fecha_inicio": "2024-06-05",
    "fecha_final": "2024-06-10",
    "adultos": 1,
    "niños": 0
}

nuevos_datos = {
    "servicios": ["Desayuno", "Piscina", "Gimnasio", "Restaurante"],
    "fecha_inicio": "2024-06-15",
    "fecha_final": "2024-06-25",
    "adultos": 2,
    "niños": 0
}

hotel_id_insertar = mongo_client.insertar_hotel(hotel_data)
print("ID del hotel insertado:", hotel_id_insertar)

hotel_id_eliminar = "6642c43e0a4efffdb742e3c6"
cantidad_eliminada = mongo_client.eliminar_hotel(hotel_id_eliminar)
print("Cantidad de hoteles eliminados:", cantidad_eliminada)

hotel_id_actualizar = "6642c4e80d4f515cf5a70eac"
cantidad_actualizada = mongo_client.actualizar_hotel(hotel_id_actualizar, nuevos_datos)
print("Cantidad de hoteles actualizados:", cantidad_actualizada)

mongo_client.cerrar_conexion()
