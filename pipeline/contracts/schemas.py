from datetime import datetime
from typing import List
from pydantic import BaseModel, Field, ConfigDict, field_validator


class InputRecord(BaseModel):
    """
    Esquema de Registro de Entrada CSV
    Representa una única fila del archivo CSV de entrada.
    """

    # Limpia los espacios en blanco de los valores str
    model_config = ConfigDict(str_strip_whitespace=True)

    id: int = Field(..., gt=0)
    timestamp: str = Field(...)
    value: float = Field(...)
    category: str = Field(..., min_length=1)

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        """
        Valida que la marca de tiempo esté en formato ISO 8601 válido
        """
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"Marca de tiempo ISO 8601 inválida: {v}")
        return v

    @field_validator("category")
    @classmethod
    def validate_category(cls, v: str) -> str:
        """
        Asegura que la categoría no esté vacía después de limpiar espacios
        """
        if not v.strip():
            raise ValueError("La categoría no puede estar vacía")
        return v.strip()


class TransformedRecord(BaseModel):
    """
    Esquema de Registro Transformado
    Representa un registro procesado después de la etapa de transformación.
    """

    id: int = Field(...)
    timestamp: str = Field(...)
    original_value: float = Field(...)
    normalized_value: float = Field(..., ge=-1.0, le=1.0)
    category: str = Field(...)
    processed_at: str = Field(...)

    @field_validator("timestamp", "processed_at")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        """
        Valida que la marca de tiempo esté en formato ISO 8601 válido
        """
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"Marca de tiempo ISO 8601 inválida: {v}")
        return v


class OutputData(BaseModel):
    """
    Esquema JSON de Salida Final
    Contenedor para todos los registros transformados con metadata.
    """

    records: List[TransformedRecord] = Field(...)
    metadata: "OutputMetadata" = Field(...)

    @field_validator("records")
    @classmethod
    def validate_records_not_empty(
        cls, v: List[TransformedRecord]
    ) -> List[TransformedRecord]:
        """
        Asegura que exista al menos un registro
        """
        if not v:
            raise ValueError("La salida debe contener al menos un registro")
        return v


class OutputMetadata(BaseModel):
    """
    Esquema de Metadata de Salida
    Contiene información sobre la ejecución del procesamiento.
    """

    total_records: int = Field(..., ge=0)
    execution_time_seconds: float = Field(..., ge=0.0)
    data_hash: str = Field(..., min_length=64, max_length=64)
    generated_at: str = Field(...)

    @field_validator("generated_at")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        """
        Valida que la marca de tiempo esté en formato ISO 8601 válido
        """
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"Marca de tiempo ISO 8601 inválida: {v}")
        return v

    @field_validator("data_hash")
    @classmethod
    def validate_hash(cls, v: str) -> str:
        """
        Valida que el hash sea hexadecimal
        """
        if not all(c in "0123456789abcdef" for c in v.lower()):
            raise ValueError("El hash debe ser hexadecimal")
        return v.lower()
