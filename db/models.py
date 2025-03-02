from pydantic import BaseModel, field_validator


class ConexionParams(BaseModel):
    host: str
    database: str
    password: str

class Relacion(BaseModel):
    tabla_padre: str
    columna_padre: str
    tabla_hija: str
    columna_hija: str