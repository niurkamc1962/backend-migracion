from os import getenv, path, makedirs
from dotenv import load_dotenv
import pyodbc
import pandas as pd
from typing import Optional, Dict, List, Any
from contextlib import contextmanager
from db.models import ConexionParams
import json
from datetime import datetime, date
from decimal import Decimal

load_dotenv()


class DatabaseManager:
    def __init__(self, host: str, password: str, database: str, port: str, user: str):
        self.connection_params = {
            "host": host,
            "password": password,
            "database": database,
            "port": port,
            "user": user,
        }
        self._conn = None

    def __enter__(self):
        """Permite usar la clase con with statement"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Garantiza que la conexión se cierre al salir del with"""
        self.close()

    def connect(self):
        """Establece la conexión a la base de datos"""
        if self._conn is None:
            print(f"Conectando con parámetros: {self.connection_params}")
            self._conn = self._create_connection()
        return self._conn

    def close(self):
        """Cierra la conexión a la base de datos"""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _create_connection(self):
        """Crea una nueva conexión a la base de datos"""
        url = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.connection_params['host']};"
            f"PORT={self.connection_params['port']};"
            f"DATABASE={self.connection_params['database']};"
            f"UID={self.connection_params['user']};"
            f"PWD={self.connection_params['password']};"
            f"Timeout=0"
        )
        try:
            return pyodbc.connect(url)
        except pyodbc.Error as ex:
            print(f"Error al conectar: {ex}")
            raise

    @contextmanager
    def cursor(self):
        """Proporciona un cursor gestionado con context manager"""
        conn = self.connect()
        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def get_all_tables(self) -> Dict[str, Any]:
        """Obtiene todas las tablas de la base de datos"""
        with self.cursor() as cursor:
            cursor.execute(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
            )
            tables = [row.TABLE_NAME for row in cursor.fetchall()]
            return {"tables": tables, "total_tables": len(tables)}

    def get_table_structure(self, table_name: str) -> List[Dict]:
        """Obtiene la estructura de una tabla específica"""
        with self.cursor() as cursor:
            cursor.execute(
                """
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ?
                """,
                table_name,
            )
            return [
                {
                    "column_name": column.COLUMN_NAME,
                    "data_type": column.DATA_TYPE,
                    "max_length": column.CHARACTER_MAXIMUM_LENGTH,
                    "is_nullable": column.IS_NULLABLE,
                }
                for column in cursor.fetchall()
            ]

    def get_table_relations(self, table_name: str) -> List[Dict[str, Any]]:
        """Obtiene las relaciones de una tabla específica"""
        try:
            with self.cursor() as cursor:
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
                cursor.execute(query)

                # Convertimos directamente los resultados a diccionario
                columns = [column[0] for column in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except pyodbc.Error as e:
            print(f"Error SQL al obtener relaciones: {str(e)}")
            raise
        except Exception as e:
            print(f"Error inesperado en get_table_relations: {str(e)}")
            raise

    def get_all_relations(self) -> List[Dict]:
        """Obtiene todas las relaciones entre tablas"""
        try:
            with self.cursor() as cursor:
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
                cursor.execute(query)

                # Obtener nombres de columnas
                columns = [column[0] for column in cursor.description]

                # Convertir resultados a lista de diccionarios
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except pyodbc.Error as e:
            print(f"Error de base de datos al obtener relaciones: {str(e)}")
            raise
        except Exception as e:
            print(f"Error inesperado en get_all_relations: {str(e)}")
            raise

    @staticmethod
    def map_sql_type_to_frappe(data_type: str) -> str:
        """Mapea tipos de datos SQL a tipos de campo de Frappe"""
        # ... (implementación igual a la que ya tienes)
        pass

    def detect_foreign_keys(self, table_name: str, column_name: str) -> Optional[str]:
        """Detecta si un campo es una clave foránea"""
        with self.cursor() as cursor:
            query = f"""
                SELECT referenced_object_id 
                FROM sys.foreign_keys 
                INNER JOIN sys.foreign_key_columns 
                    ON sys.foreign_keys.object_id = sys.foreign_key_columns.constraint_object_id
                WHERE parent_object_id = OBJECT_ID('{table_name}')
                  AND parent_column_id = COL_NAME(OBJECT_ID('{table_name}'), '{column_name}')
            """
            cursor.execute(query)
            return "Link" if cursor.fetchone() else None

    def export_table_to_json(
        self, table_name: str, fields: List[str], output_folder: str = "formatos_json"
    ) -> Dict[str, Any]:
        """
        Exporta los datos de una tabla a un archivo JSON y retorna los datos

        Args:
            table_name: Nombre de la tabla a exportar
            fields: Lista de campos a seleccionar
            output_folder: Carpeta de destino para el JSON

        Returns:
            Diccionario con nombre de tabla y datos
        """
        if not path.exists(output_folder):
            makedirs(output_folder)

        with self.cursor() as cursor:
            query = f"SELECT {', '.join(fields)} FROM {table_name}"
            cursor.execute(query)

            columns = [column[0] for column in cursor.description]
            table_data = []

            for row in cursor.fetchall():
                row_data = {}
                for i, field_name in enumerate(columns):
                    value = row[i]
                    if not self.is_serializable(
                        value
                    ):  # Usa tu función de serialización
                        value = self.serialize_value(value)
                    row_data[field_name] = value
                table_data.append(row_data)

        file_path = path.join(output_folder, f"{table_name}.json")
        with open(file_path, "w", encoding="utf-8") as json_file:
            json.dump(
                {"table_name": table_name, "data": table_data},
                json_file,
                indent=4,
                ensure_ascii=False,
            )

        return {"table_name": table_name, "data": table_data}

    def serialize_value(self, value):
        """Serializa valores no estándar a formatos JSON compatibles."""
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        elif isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, bytes):
            return value.decode(
                "utf-8", errors="ignore"
            )  # Manejo de errores de decodificación
        return str(value)

    def is_serializable(self, value):
        """Verifica si un valor puede ser serializado a JSON."""
        try:
            json.dumps(value)
            return True
        except (TypeError, OverflowError):
            return False


def create_db_manager(params: ConexionParams) -> DatabaseManager:
    """Helper para crear instancias de DatabaseManager con configuración consistente"""
    return DatabaseManager(
        host=params.host,
        password=params.password,
        database=params.database,
        port=getenv("SQL_PORT"),
        user=getenv("SQL_USER"),
    )


# # Funciones legacy (puedes mantenerlas temporalmente para compatibilidad)
# def get_db_connection(host: str, password: str, database: str, port: str, user: str):
#     """Función legacy para compatibilidad"""
#     return DatabaseManager(host, password, database, port, user).connect()


# # ... (otras funciones legacy que quieras mantener)
