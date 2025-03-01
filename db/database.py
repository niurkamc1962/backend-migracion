from os import getenv
from dotenv import load_dotenv
import pyodbc


def get_db_connection(host: str, password: str, database: str, port:str, user: str):
    """Establece la conexion y retorna la conexion a la BD SQL Server"""
    load_dotenv(".env")

    SQL_USER = getenv("SQL_USER")
    SQL_PORT = getenv("SQL_PORT")
    # SQL_HOST = getenv("SQL_HOST")
    # SQL_PASS = getenv("SQL_PASS")
    # SQL_DATABASE = getenv("SQL_DATABASE")

    # print(f"HOST: {SQL_HOST}")
    print(f"HOST: { host}, PASS: {password}, DATABASE: {database}")

    # asegurando que todas las variables de entorno estan definidas y no vacias
    if not all([host, database, password, SQL_USER, SQL_PORT ]):
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


def get_db_cursor(conn):
    """Retorna un cursor para ejecutar consultas con la conexion dada"""
    if conn:
        return conn.cursor()
    else:
        return None