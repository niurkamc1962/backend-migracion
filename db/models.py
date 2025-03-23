from pydantic import BaseModel
from typing import List


class ConexionParams(BaseModel):
    host: str
    database: str
    password: str


class Relacion(BaseModel):
    tabla_padre: str
    columna_padre: str
    tabla_hija: str
    columna_hija: str


class Field(BaseModel):
    nombre_campo: str
    tipo_campo: str
    obligatorio: bool


class Payload(BaseModel):
    params: ConexionParams
    fields: List[Field]
