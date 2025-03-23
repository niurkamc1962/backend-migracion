from typing import List, Dict
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from os import getenv, path, makedirs
from db.database import (
    get_db_connection,
    get_db_cursor,
    relacion_tabla,
    relaciones_todas_tablas,
)
from db.models import ConexionParams, Payload
import pyodbc
import json
from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel


# from db.models import ConexionParams
# from dotenv import load_dotenv

app = FastAPI()

# origins = [
#     getenv("ORIGIN"),
# ]


# Obtener el dominio del frontend desde las variables de entorno
frontend_domain = getenv("FRONTEND_DOMAIN")


# Definir una función para validar si el origen es permitido
def is_valid_origin(origin: str):
    return origin.startswith(frontend_domain)


# Configurar CORS
origins = [frontend_domain]  # Inicializar con el dominio base
allow_all_origins = (
    getenv("ALLOW_ALL_ORIGINS", "false").lower() == "true"
)  # Nueva variable para permitir todos los origenes

if allow_all_origins:
    origins = ["*"]
else:
    # Si el dominio es localhost y estamos en desarrollo, permitir cualquier puerto en localhost
    if frontend_domain == "http://localhost":
        origins = [
            "http://localhost:8000",  # Puerto por defecto de Quasar
            "http://localhost:8080",  # Otro puerto comun de Quasar
            "http://localhost:9000",  # Agrega otros puertos comunes que uses
            "http://localhost:9006",  # Agrega otros puertos comunes que uses
        ]

        # Funcion para determinar si estamos en desarrollo
        def is_development():
            return getenv("ENVIRONMENT", "production") == "development"

        # Agrega cualquier origen que comience con localhost si estamos en desarrollo
        if is_development():
            origins.append("http://localhost")  # Agrega el dominio base
    else:
        origins = [frontend_domain]  # Solo permitir el dominio base en producción


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


# Función para verificar si un valor es serializable
def is_serializable(value):
    try:
        json.dumps(value)
        return True
    except (TypeError, OverflowError):
        return False


@app.get("/", tags=["Root"])
async def hello():
    return "Hello, fastapi"


# Endpoint para recibir los parametros del frontend quasar
@app.post(
    "/conectar-params",
    tags=["Database"],
    summary="Obtiene la ip del servidor de la BD desde el frontend para conectar",
)
async def conectar_parametros(params: ConexionParams):
    try:
        # Conecta a la base de datos con los parametros dinamicos
        # conn = pyodbc.connect(url_siscont)
        conn = get_db_connection(
            params.host,
            params.password,
            params.database,
            getenv("SQL_PORT"),
            getenv("SQL_USER"),
        )
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
    conn = get_db_connection(
        params.host,
        params.password,
        params.database,
        getenv("SQL_PORT"),
        getenv("SQL_USER"),
    )
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
    conn = get_db_connection(
        params.host,
        params.password,
        params.database,
        getenv("SQL_PORT"),
        getenv("SQL_USER"),
    )
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
async def get_table_data(table_name: str, payload: Payload):
    params = payload.params
    fields = payload.fields

    conn = get_db_connection(
        params.host,
        params.password,
        params.database,
        getenv("SQL_PORT"),
        getenv("SQL_USER"),
    )
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
        # query = f"SELECT * FROM {table_name}"

        # Filtrar los campos obligatorios y construir la consulta SQL
        selected_fields = [field.nombre_campo for field in fields if field.obligatorio]
        if not selected_fields:
            raise HTTPException(
                status_code=400, detail="No se proporcionaron campos obligatorios"
            )

        query_fields = ", ".join(selected_fields)
        query = f"SELECT {query_fields} FROM {table_name}"
        print(f"query: {query}")

        cursor.execute(query)
        rows = cursor.fetchall()

        # Obteniendo los nombres de las columnas
        # columns = [column[0] for column in cursor.description]
        columns = selected_fields

        # Convirtiendo los datos a formato JSON
        table_data = []
        for row in rows:
            row_data = dict(zip(columns, row))
            # table_data.append(row_data)
            serializable_row_data = {
                key: value for key, value in row_data.items() if is_serializable(value)
            }
            table_data.append(serializable_row_data)

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


# Endpoint que obtiene la relacion de la tabla especificada
@app.post(
    "/table-relation/{table_name}",
    tags=["Database"],
    summary="Muestra las relaciones de una tabla específica",
    response_model=list[dict],
)
async def get_table_relation(table_name: str, params: ConexionParams):
    # Obtiene la conexión a la base de datos
    print("******ENTRE EN get_table_relation ******")
    print(f"Tabla name: {table_name}")
    print(f"Params(body): {params}")
    print(f"Puerto: {getenv('SQL_PORT')}")
    print(f"usuario: {getenv('SQL_USER')}")

    conn = get_db_connection(
        params.host,
        params.password,
        params.database,
        getenv("SQL_PORT"),
        getenv("SQL_USER"),
    )

    if not conn:
        raise HTTPException(status_code=500, detail="No se pudo conectar con la bd")
    cursor = get_db_cursor(conn)
    if not cursor:
        conn.close()
        raise HTTPException(
            status_code=500, detail="No se pudo crear el cursor de conexion a la bd"
        )

    # Obtiene las relaciones
    relaciones = relacion_tabla(conn, table_name)
    print(f"Relaciones de la tabla: {table_name}")
    return relaciones


# Endpoint que obtiene las relaciones entre las tablas de una Base de datos
@app.post(
    "/all-relation",
    tags=["Database"],
    summary="Muestra las relaciones entre las tablas de la BD",
    response_model=list[dict],
)
async def get_all_relation(params: ConexionParams):
    # Obteniendo conexion con la base de datos
    conn = get_db_connection(
        params.host,
        params.password,
        params.database,
        getenv("SQL_PORT"),
        getenv("SQL_USER"),
    )

    if conn:
        # obteniendo las relaciones
        relaciones = relaciones_todas_tablas(conn)
        return relaciones  # devolviend la lista de diccionarios
    else:
        return [{"error": "No se pudo establecer conexion"}]
