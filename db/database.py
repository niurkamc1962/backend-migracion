from os import getenv
from dotenv import load_dotenv
import pyodbc
import pandas as pd
from typing import Optional, Dict, List

load_dotenv()


# Funcion que prepara la cadena conexion con la BD
def get_db_connection(host: str, password: str, database: str, port: str, user: str):
    """Establece la conexion y retorna la conexion a la BD SQL Server"""

    SQL_USER = getenv("SQL_USER")
    SQL_PORT = getenv("SQL_PORT")
    # SQL_HOST = getenv("SQL_HOST")
    # SQL_PASS = getenv("SQL_PASS")
    # SQL_DATABASE = getenv("SQL_DATABASE")

    # print(f"HOST: {SQL_HOST}")
    print(
        f"HOST: {host}, PASS: {password}, DATABASE: {database}, USER: {SQL_USER}, PORT: {SQL_PORT}"
    )

    # asegurando que todas las variables de entorno estan definidas y no vacias
    if not all([host, database, password, SQL_USER, SQL_PORT]):
        raise ValueError("Faltan variables de entorno por definir")

    # Preparando la cadena de conexion
    url_siscont = (
        f"DRIVER=ODBC Driver 17 for SQL Server;"
        f"SERVER={host};"
        f"PORT={SQL_PORT};"
        f"DATABASE={database};"
        f"UID={SQL_USER};"
        f"PWD={password};"
        f"Timeout=0"
    )
    print(f"URL_SISCONT: {url_siscont}")
    try:
        conn = pyodbc.connect(url_siscont)
        print(f"conn: {conn}")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print("Error al crear conexion con sqlserver")
        print(ex)
        print(sqlstate)
        return None


# Funcion para la conexion
def get_db_cursor(conn):
    """Retorna un cursor para ejecutar consultas con la conexion dada"""
    if conn:
        return conn.cursor()
    else:
        return None


# Funcion para obtener todas las tablas y sus relaciones
def relaciones_todas_tablas(conn):
    """Ejecuta la consulta para obtener las relaciones entre tablas"""
    query = """
        SELECT 
            OBJECT_NAME(f.parent_object_id) AS tabla_padre,
            COL_NAME(fc.parent_object_id, fc.parent_column_id) AS columna_padre,
            OBJECT_NAME(f.referenced_object_id) AS tabla_hija,
            COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS columna_hija
        FROM 
            sys.foreign_keys f
        INNER JOIN 
            sys.foreign_key_columns fc ON f.object_id = fc.constraint_object_id
    """

    df = pd.read_sql_query(query, conn)
    relaciones = df.to_dict(orient="records")
    return relaciones


# Funcion que obtiene las tablas que se relacionan con la tabla especificada
def relacion_tabla(conn, table_name):
    print(f"conn desde obtener relaciones: {conn}")
    query = f"""
        SELECT 
            OBJECT_NAME(f.parent_object_id) AS tabla_padre,
            COL_NAME(fc.parent_object_id, fc.parent_column_id) AS columna_padre,
            OBJECT_NAME(f.referenced_object_id) AS tabla_hija,
            COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS columna_hija
        FROM 
            sys.foreign_keys f
        INNER JOIN 
            sys.foreign_key_columns fc ON f.object_id = fc.constraint_object_id
        WHERE 
            OBJECT_NAME(f.parent_object_id) = '{table_name}'
        OR 
            OBJECT_NAME(f.referenced_object_id) = '{table_name}'
    """

    df = pd.read_sql_query(query, conn)
    relaciones = df.to_dict(orient="records")
    return relaciones


# Funcion para la relacion entre los campos sql-server a doctype
def map_sql_type_to_frappe(data_type: str) -> str:
    """Mapea tipos de datos SQL a tipos de campo de Frappe"""
    data_type = data_type.lower().strip()

    type_mapping = {
        # Texto
        "varchar": "Data",
        "nvarchar": "Data",
        "char": "Data",
        "nchar": "Data",
        "text": "Text",
        "ntext": "Text",
        "longtext": "Text",
        "mediumtext": "Text",
        "tinytext": "Text",
        # Números enteros
        "int": "Int",
        "integer": "Int",
        "bigint": "Int",
        "smallint": "Int",
        "tinyint": "Int",
        # Números decimales
        "decimal": "Float",
        "numeric": "Float",
        "float": "Float",
        "double": "Float",
        "real": "Float",
        # Fechas y tiempos
        "date": "Date",
        "datetime": "Datetime",
        "datetime2": "Datetime",
        "smalldatetime": "Datetime",
        "timestamp": "Datetime",
        "time": "Time",
        # Binarios
        "binary": "Data",
        "varbinary": "Data",
        "image": "Attach",
        # Booleanos
        "bit": "Check",
        "boolean": "Check",
        # JSON
        "json": "JSON",
        # Especiales
        "uniqueidentifier": "Data",  # UUID
    }

    # Buscar coincidencia exacta primero
    if data_type in type_mapping:
        return type_mapping[data_type]

    # Manejar tipos con parámetros como varchar(255), decimal(10,2), etc.
    base_type = data_type.split("(")[0]
    if base_type in type_mapping:
        return type_mapping[base_type]

    # Para tipos desconocidos, usar Data como predeterminado
    return "Data"


# Funcion para el caso de las llaves foraneas y el tipo link de Frappe
def detectar_relaciones(conn, tabla_sql, campo):
    cursor = get_db_cursor(conn)
    query = f"""
        SELECT referenced_object_id 
        FROM sys.foreign_keys 
        INNER JOIN sys.foreign_key_columns 
            ON sys.foreign_keys.object_id = sys.foreign_key_columns.constraint_object_id
        WHERE parent_object_id = OBJECT_ID('{tabla_sql}')
          AND parent_column_id = COL_NAME(OBJECT_ID('{tabla_sql}'), {campo.nombre_campo})
    """
    cursor.execute(query)
    resultado = cursor.fetchone()
    return "Link" if resultado else None


# Funcio para obtener la estructura de la tabla
def get_table_structure(conn, table_name: str) -> List[Dict]:
    """Obtiene la estructura de una tabla específica"""
    cursor = conn.cursor()
    try:
        query = """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
        """
        cursor.execute(query, table_name)
        columns = cursor.fetchall()

        return [
            {
                "column_name": column.COLUMN_NAME,
                "data_type": column.DATA_TYPE,
                "max_length": column.CHARACTER_MAXIMUM_LENGTH,
                "is_nullable": column.IS_NULLABLE,
            }
            for column in columns
        ]
    finally:
        cursor.close()
