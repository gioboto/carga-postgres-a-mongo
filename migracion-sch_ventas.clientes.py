
import psycopg2
from pymongo import MongoClient
import datetime

def migrate_data(pg_conn_params, mongo_conn_params, table_name, batch_size=1000):
    try:
        # Conexión a PostgreSQL
        pg_conn = psycopg2.connect(**pg_conn_params)
        pg_cursor = pg_conn.cursor()

        # Conexión a MongoDB
        mongo_client = MongoClient(mongo_conn_params['uri'])
        mongo_db = mongo_client[mongo_conn_params['database']]
        mongo_collection = mongo_db[mongo_conn_params['collection']]

        # Obtener los nombres de las columnas
        pg_cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
        colnames = [desc[0] for desc in pg_cursor.description]

        offset = 0

        while True:
            pg_cursor.execute(f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}")
            rows = pg_cursor.fetchall()
            if not rows:
                break

            # Crear documentos de MongoDB
            docs = []
            for row in rows:
                doc = {}
                for i in range(len(colnames)):
                    value = row[i]
                    # Convertir datetime.date a datetime.datetime
                    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
                        value = datetime.datetime(value.year, value.month, value.day)
                    doc[colnames[i]] = value
                docs.append(doc)

            mongo_collection.insert_many(docs)

            print(f"Batch {offset // batch_size + 1} migradado OK hostias!.")

            offset += batch_size

        # Cerrar las conexiones
        pg_cursor.close()
        pg_conn.close()
        mongo_client.close()

        print("Migración completada exitosamente!")

    except Exception as e:
        print(f"Error durante la migración: {e}")

# Parámetros de conexión a PostgreSQL
pg_conn_params = {
    'dbname': 'postgresDB',
    'user': 'postgres',
    'password': 'clave123',
    'host': 'localhost',
    'port': '5432'
}

# Parámetros de conexión a MongoDB
mongo_conn_params = {
    'uri': 'mongodb://root:clave123@sitio.com:27017/',
    'database': 'test_db',
    'collection': 'sch_ventas.clientes'
}

# Nombre de la tabla en PostgreSQL
table_name = 'sch_ventas.cliente'

# Ejecutar la migración
migrate_data(pg_conn_params, mongo_conn_params, table_name)

