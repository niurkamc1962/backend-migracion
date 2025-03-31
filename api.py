from typing import List, Dict, Any
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from os import getenv
from db.database import DatabaseManager, create_db_manager
from db.models import ConexionParams, GenerateDoctype, Payload
import pyodbc
import json

from datetime import datetime, date
from decimal import Decimal
from pydantic import BaseModel

app = FastAPI()

# Configuración CORS (se mantiene igual)
frontend_domain = getenv("FRONTEND_DOMAIN")


def is_valid_origin(origin: str):
    return origin.startswith(frontend_domain)


origins = [frontend_domain]
allow_all_origins = getenv("ALLOW_ALL_ORIGINS", "false").lower() == "true"

if allow_all_origins:
    origins = ["*"]
elif frontend_domain == "http://localhost":
    origins.extend(
        [
            "http://localhost:3000",
            "http://localhost:8000",
            "http://localhost:8080",
        ]
    )
    if getenv("ENVIRONMENT", "production") == "development":
        origins.append("http://localhost")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Endpoints refactorizados
@app.get("/", tags=["Root"])
async def hello():
    return {"message": "Hello, FastAPI"}


@app.post("/conectar-params", tags=["Database"])
async def conectar_parametros(params: ConexionParams):
    try:
        with create_db_manager(params) as db:
            tables = db.get_all_tables()
            table_count = len(tables)
            return {"tables": tables, "table_count": table_count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/tables", tags=["Database"], response_model=Dict[str, Any])
async def get_tables_endpoint(params: ConexionParams):
    try:
        with create_db_manager(params) as db:
            return db.get_all_tables()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/table-structure/{table_name}", tags=["Database"])
async def get_table_structure_endpoint(table_name: str, params: ConexionParams):
    try:
        with create_db_manager(params) as db:
            structure = db.get_table_structure(table_name)
            if not structure:
                raise HTTPException(status_code=404, detail="Tabla no encontrada")
            return {"table_name": table_name, "columns": structure}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/table-data/{table_name}", tags=["Database"])
async def get_table_data_endpoint(table_name: str, payload: Payload):
    try:
        with create_db_manager(payload.params) as db:
            fields = [field.nombre_campo for field in payload.fields]
            return db.export_table_to_json(table_name=table_name, fields=fields)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error al procesar la tabla {table_name}: {str(e)}"
        )


@app.post(
    "/table-relation/{table_name}",
    tags=["Database"],
    response_model=List[Dict[str, Any]],
)
async def get_table_relation_endpoint(table_name: str, params: ConexionParams):
    try:
        with create_db_manager(params) as db:
            relations = db.get_table_relations(table_name)
            if not relations:
                return []
            return relations
    except pyodbc.Error as e:
        raise HTTPException(status_code=400, detail=f"Error de base de datos: {str(e)}")
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener relaciones para {table_name}: {str(e)}",
        )


@app.post("/all-relation", tags=["Database"], response_model=List[Dict])
async def get_all_relation_endpoint(params: ConexionParams):
    try:
        with create_db_manager(params) as db:
            return db.get_all_relations()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/generate-doctype-json/{table_name}", tags=["Database"])
async def generate_doctype_json(table_name: str, payload: GenerateDoctype):
    try:
        with create_db_manager(payload.params) as db:
            return db.generate_doctype_json(table_name, payload)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al generar el JSON para {table_name}: {str(e)}"
        )

