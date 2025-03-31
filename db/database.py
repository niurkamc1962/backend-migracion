from os import getenv, path, makedirs
from dotenv import load_dotenv
import pyodbc
import pandas as pd
from typing import Optional, Dict, List, Any
from contextlib import contextmanager
from db.models import ConexionParams, Payload
import json
from datetime import datetime, date
from decimal import Decimal
import re  # para usar expresiones regulares


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

    def export_table_to_json(
        self, table_name: str, fields: List[str], output_folder: str = "formatos_json"
    ) -> Dict[str, Any]:
        """
        Exporta los datos de una tabla SQL a un archivo JSON

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

    @staticmethod
    def map_sql_type_to_frappe(sql_type: str) -> str:
        """Mapea tipos de campo SQL a tipos de campo en Frappe"""
        mapping = {
            "varchar": "Data",
            "nvarchar": "Data",
            "char": "Data",
            "text": "Text",
            "ntext": "Text",
            "int": "Int",
            "smallint": "Int",
            "bigint": "Int",
            "decimal": "Float",
            "numeric": "Float",
            "float": "Float",
            "real": "Float",
            "date": "Date",
            "datetime": "Datetime",
            "datetime2": "Datetime",
            "smalldatetime": "Datetime",
            "time": "Time",
            "bit": "Check",
            "tinyint": "Int",
            "binary": "Data",
            "varbinary": "Data",
            "uniqueidentifier": "Data",
        }
        return mapping.get(sql_type.lower(), "Data")

    def generate_doctype_json(
        self, table_name: str, payload: Payload, output_folder: str = "formatos_json"
    ) -> dict:
        """Genera el formato Doctype JSON para la tabla especificada"""
        print("Entre en generate_dcotype_json")
        doctype_json = {
            "module": payload.module,
            "name": table_name.lower(),
            "nsm_parent_field": "",
            "owner": "Administrator",
            "actions": [],
            "allow_events_in_timeline": 1,
            "allow_import": 1,
            "allow_rename": 1,
            "autoname": "",
            "creation": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "description": "",
            "doctype": "DocType",
            "document_type": "Setup",
            "engine": "InnoDB",
            "field_order": [],
            "permissions": [
                {
                    "role": "System Manager",
                    "permlevel": 0,
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1,
                    "report": 1,
                }
            ],
            "fields": [],  # inicializa la lista de campos
            "is_child_table": (
                payload.is_child_table if payload.is_child_table is not None else False
            ),
        }

        # Procesa los campos  SQL que se pasan en el payload
        fields = []
        for field in payload.fields:
            print(f"entre en field: {field}")
            field_data = self._process_field(field)
            doctype_json["fields"].append(field_data)
            doctype_json["field_order"].append(field_data["fieldname"])

        # Salvando archivo doctype
        file_path = path.join(output_folder, f"doctype_{table_name}.json")
        try:
            with open(file_path, "w", encoding="utf-8") as json_file:
                json.dump(doctype_json, json_file, indent=4, ensure_ascii=False)
            print(f"Doctype guardado en: {file_path}")
        except Exception as e:
            print(f"Error al guardar el archivo: {e}")
            return {"table_name": table_name, "error": str(e)}


    def _process_field(self, field: Any) -> Dict[str, Any]:
        """Procesa un campo individual para determinar su tipo y nombre."""
        # Lista de tipos de campo válidos en Frappe (puedes ampliarla)
        valid_frappe_fieldtypes = [
            "Data",
            "Text",
            "Int",
            "Float",
            "Date",
            "Datetime",
            "Time",
            "Check",
            "Link",
            "Select",
            # Agrega más tipos de campo válidos aquí
        ]
        print("entre en _process_field con {field}")
        # Determinar el tipo de campo
        if field.tipo_campo_erp:
            print(f"field ERP: {field.tipo_campo_erp}")
            if field.tipo_campo_erp in valid_frappe_fieldtypes:
                fieldtype = field.tipo_campo_erp  # Usar directamente tipo_campo_erp
            else:
                print(
                    f"Advertencia: tipo_campo_erp '{field.tipo_campo_erp}' no es un tipo de campo válido en Frappe. Mapeando tipo_campo en su lugar que es '{field.tipo_campo}'."
                )
                fieldtype = self.map_sql_type_to_frappe(
                    field.tipo_campo
                )  # Mapear tipo_campo
        else:
            print(f"tipo_campo: {field.tipo_campo}")
            fieldtype = self.map_sql_type_to_frappe(
                field.tipo_campo
            )  # Mapear tipo_campo

        # Determinar el nombre del campo
        if field.nombre_campo_erp:
            print(f"entre por nombre_campo_erp: {field.nombre_campo_erp}")
            fieldname = field.nombre_campo_erp.lower()
            fieldlabel = field.nombre_campo_erp
        else:
            # Usar nombre_campo y convertirlo a formato de Frappe
            print(f"nombre_campo: {field.nombre_campo}")
            fieldname = self.format_frappe_fieldname(field.nombre_campo)
            fieldlabel = field.nombre_campo

        return {
            "fieldname": fieldname,
            "label": fieldlabel,
            "fieldtype": fieldtype,
            "reqd": field.obligatorio,
        }


    def format_frappe_fieldname(self, field_name: str) -> str:
        """Convierte un nombre de campo a formato Frappe (snake_case)"""
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", field_name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()



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
