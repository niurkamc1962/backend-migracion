from pydantic import BaseModel, field_validator


class ConexionParams(BaseModel):
    host: str
    # port: int
    # database: str
    # user: str
    # password: str

    # @field_validator("port")
    # def port_must_be_valid_port(cls, v):
    #     if not (1 <= v <= 65535):
    #         raise ValueError("Puerto tiene que estar entre 1 y 65535")
    #     return v
