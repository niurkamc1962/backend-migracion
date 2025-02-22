from typing import List, Dict
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from os import getenv, path, makedirs
from db.database import get_db_connection, get_db_cursor
import pyodbc
import json
from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel

# from db.models import ConexionParams
from dotenv import load_dotenv


class ConexionParams(BaseModel):
    host: str


app = FastAPI()

origins = [
    getenv("ORIGIN"),
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Funcion para convertir objetos Decimal a str para no perder los decimales,
# las fechas a formato ISO porque los JSON no serializan campos decimal ni datetime
def convert_custom_types(obj):
    if isinstance(obj, Decimal):
        return float(obj)  # convierte decimal a flotante
    elif isinstance(obj, datetime):
        return obj.strftime(
            "%Y-%m-%d %H:%M:%S"
        )  # convertir datetime a string en formato ISO 8601
    raise TypeError(f"Tipo no serializable {type(obj)}")


@app.get("/", tags=["Root"])
async def hello():
    return "Hello, fastapi"


# # funcion que define la conexion a Siscont
# def db_connect_siscont():
#     db = {
#         "server": "172.20.0.3",
#         "port": "1433",
#         "database": "S5Principal",
#         "user": "sa",
#         "password": "S5Principal",
#     }
#     # preparando la cadena de conexion haciendo uso del driver ODBC 17
#     url_siscont = (
#         f'DRIVER=ODBC Driver 17 for SQL SERVER;SERVER={db["server"]};PORT={db["port"]};DATABASE'
#         f'={db["database"]};TDS_Version=8.0;UID={db["user"]};PWD={db["password"]}'
#     )
#     try:
#         connect_siscont = pyodbc.connect(url_siscont)
#         return connect_siscont
#     except Exception as e:
#         print("Error al crear conexion con sqlserver", e)


# @app.get(
#     "/conectar",
#     tags=["Database"],
#     summary="Prueba de conexion con la BD en el contenedor",
# )
# async def check_db_connection():
#     try:
#         conn = db_connect_siscont()
#         if conn:
#             conn.close()  # Cierra la conexión después de verificar
#             return {
#                 "status": "success",
#                 "message": "Conexión a la base de datos establecida correctamente.",
#             }
#     except HTTPException as e:
#         raise e
#     except Exception as e:
#         raise HTTPException(
#             status_code=500, detail=f"Error al conectar a la base de datos: {str(e)}"
#         )


# Endpoint para recibir los parametros del frontend quasar
@app.post(
    "/conectar-params",
    tags=["Database"],
    summary="Obtiene la ip del servidor de la BD desde el frontend para conectar",
)
async def conectar_parametros(params: ConexionParams):
    # url_siscont = (
    #     f"DRIVER=ODBC Driver 17 for SQL Server;"
    #     f"SERVER={params.host};"
    #     f"PORT={SQL_PORT};"
    #     f"DATABASE={SQL_DATABASE};"
    #     f"UID={SQL_USER};"
    #     f"PWD={SQL_PASS};"
    #     f"Timeout=0"  # Aumenta el tiempo de espera
    # )
    try:
        # Conecta a la base de datos con los parametros dinamicos
        # conn = pyodbc.connect(url_siscont)
        conn = get_db_connection(params.host)
        print("conn: ", conn)
        if not conn:
            raise HTTPException(
                status_code=500, detail="No se pudo establecer la conexión."
            )
            # Crea un cursor
        cursor = conn.cursor()

        # Ejemplo: Obtiene las tablas
        cursor.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        )
        tables = [row.TABLE_NAME for row in cursor.fetchall()]

        # Cierra la conexión
        conn.close()

        # Retorna la respuesta
        return {
            "status": "success",
            "message": "Conexión exitosa y tablas obtenidas.",
            "tables": tables,
            "parameters": params,  # devuelve los parametros que se usaron
        }
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        error_message = f"Error de conexión: {ex}. SQL State: {sqlstate}"
        raise HTTPException(status_code=500, detail=error_message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inesperado: {str(e)}")


# Endpoint que muestra todas las tablas de la BD Siscont
@app.post(
    "/tables",
    tags=["Database"],
    response_model=Dict[str, List[str] | int],
    summary="Retorna todas las tablas de la base de datos de Siscont",
)
async def get_tables(params: ConexionParams):
    """Retornando lista de los nombres de las tablas y el total de tablas"""
    # conn = get_db_connection()
    conn = get_db_connection(params.host)
    if not conn:
        raise HTTPException(status_code=500, detail="No se pudo conectar con la bd")

    cursor = get_db_cursor(conn)
    if not cursor:
        conn.close()
        raise HTTPException(
            status_code=500, detail="No se pudo crear el cursor de conexion a la bd"
        )

    try:
        cursor.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        )
        tables = [row.TABLE_NAME for row in cursor.fetchall()]

        # obteniendo el total de tablas
        total_tables = len(tables)

        return {"tables": tables, "total_tables": total_tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# Endpoint que muestra la estructura de la tabla seleccionada
@app.post(
    "/table-structure/{table_name}",
    tags=["Database"],
    summary="Muestra la estructura de la tabla especificada",
)
async def get_table_structure(table_name: str, params: ConexionParams):
    # conn = get_db_connection()
    conn = get_db_connection(params.host)
    if not conn:
        raise HTTPException(status_code=500, detail="No se pudo conectar con la bd")

    cursor = get_db_cursor(conn)
    if not cursor:
        conn.close()
        raise HTTPException(
            status_code=500, detail="No se pudo crear el cursor de conexion a la bd"
        )

    query = """
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = ? """

    cursor.execute(query, table_name)
    columns = cursor.fetchall()

    if not columns:
        raise HTTPException(status_code=404, detail="No existe la tabla")

    # Formateando la respuesta
    table_structure = []
    for column in columns:
        column_info = {
            "column_name": column.COLUMN_NAME,
            "data_type": column.DATA_TYPE,
            "max_length": column.CHARACTER_MAXIMUM_LENGTH,
            "is_nullable": column.IS_NULLABLE,
        }
        table_structure.append(column_info)

    conn.close()
    return {"table_name": table_name, "columns": table_structure}


# Endpoint que convierte la tabla seleccionada a fichero JSON
@app.post(
    "/table-data/{table_name}",
    tags=["Database"],
    summary="Convierte la informacion de la tabla especificada a un archivo JSON",
)
async def get_table_structure(table_name: str, params: ConexionParams):
    # conn = get_db_connection()
    conn = get_db_connection(params.host)
    if not conn:
        raise HTTPException(status_code=500, detail="No se pudo conectar con la bd")

    cursor = get_db_cursor(conn)
    if not cursor:
        conn.close()
        raise HTTPException(
            status_code=500, detail="No se pudo crear el cursor de conexion a la bd"
        )

    try:
        # definiendo la carpeta donde se guardaran los archivos json
        output_folder = "archivos_json"
        # creando la carpeta si no existe
        if not path.exists(output_folder):
            makedirs(output_folder)

        # Consulta para obtener los datos de la tabla
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        rows = cursor.fetchall()

        # Obteniendo los nombres de las columnas
        columns = [column[0] for column in cursor.description]

        # Convirtiendo los datos a formato JSON
        table_data = []
        for row in rows:
            row_data = dict(zip(columns, row))
            table_data.append(row_data)

        file_path = path.join(output_folder, f"{table_name}.json")

        # Guardar los datos en un archivo JSON
        with open(file_path, "w") as json_file:
            json.dump(
                {"table_name": table_name, "data": table_data},
                json_file,
                indent=4,
                default=convert_custom_types,
            )
        # Retornando la tabla en formato JSON
        return {"table_name": table_name, "data": table_data}
    except Exception as e:
        raise HTTPException(code=500, detail=f"Error al procesar la tabla: {e}")

    finally:
        cursor.close()
        conn.close()
