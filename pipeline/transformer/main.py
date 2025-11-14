import copy
import hashlib
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
from pipeline.config import INTERMEDIATE_DIR, LOG_LEVEL, OUTPUT_DIR
from pipeline.contracts.schemas import OutputData, OutputMetadata, TransformedRecord

log_level = LOG_LEVEL
logging.basicConfig(level=getattr(logging, log_level))
logger = logging.getLogger(__name__)


# Patrón prototype para transformaciones
class TransformationPrototype:
    """Prototipo base para transformaciones"""

    def __init__(self):
        self.transformations = []

    def clone(self):
        """Crear una copia profunda del prototipo"""
        return copy.deepcopy(self)

    def add_transformation(self, transform_func):
        """Agregar una transformación al pipeline"""
        self.transformations.append(transform_func)

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """Aplicar todas las transformaciones"""
        result = data.copy()
        for transform in self.transformations:
            result = transform(result)
        return result


class Transformer:
    """Componente de transformación determinista"""

    def __init__(self, input_dir: str | None = None, output_dir: str | None = None):
        self.input_dir = Path(input_dir or INTERMEDIATE_DIR)
        self.output_dir = Path(output_dir or OUTPUT_DIR)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.prototype = self._create_prototype()

    def _create_prototype(self) -> TransformationPrototype:
        """Crear el prototipo de transformaciones"""
        prototype = TransformationPrototype()

        # Agregar transformaciones deterministas
        prototype.add_transformation(self._clean_data)
        prototype.add_transformation(self._normalize_values)
        prototype.add_transformation(self._add_metadata)

        return prototype

    @staticmethod
    def _normalize_values(df: pd.DataFrame) -> pd.DataFrame:
        """Normalizar valores entre -1 y 1 de forma determinista"""
        df = df.copy()
        if "value" in df.columns and len(df) > 0:
            min_val = df["value"].min()
            max_val = df["value"].max()
            if max_val > min_val:
                df["normalized_value"] = (
                    2 * (df["value"] - min_val) / (max_val - min_val) - 1
                )
            else:
                df["normalized_value"] = 0.5
            df.rename(columns={"value": "original_value"}, inplace=True)
        return df

    @staticmethod
    def _clean_data(df: pd.DataFrame) -> pd.DataFrame:
        """Limpiar datos de forma determinista"""
        df = df.copy()
        # Eliminar duplicados manteniendo el primero
        df = df.drop_duplicates(subset=["id"], keep="first")
        # Llenar valores nulos de forma determinista
        df = df.fillna(
            {"value": 0.0, "category": "unknown", "timestamp": "1970-01-01T00:00:00"}
        )
        return df

    @staticmethod
    def _add_metadata(df: pd.DataFrame) -> pd.DataFrame:
        """Agregar metadata de forma determinista"""
        df = df.copy()
        df["processed_at"] = datetime.now().isoformat()
        return df

    def _calculate_output_hash(self, data: List[Dict]) -> str:
        """Calcular hash determinista de la salida"""
        # Serializar con orden determinista
        json_str = json.dumps(data, sort_keys=True, ensure_ascii=True)
        return hashlib.sha256(json_str.encode()).hexdigest()

    def transform(self):
        """Proceso principal de transformación"""
        start_time = time.time()
        json_files = sorted(
            [
                f
                for f in self.input_dir.glob("*.json")
                if f.name != ".processed_hashes.json"
            ]
        )
        print(f"{json_files}")
        logger.info(f"Encontrados {len(json_files)} archivos para transformar")

        all_records = []

        for json_file in json_files:
            try:
                with open(json_file, "r") as f:
                    data = json.load(f)

                df = pd.DataFrame(data)

                # Aplicar transformaciones usando el prototipo
                transform_pipeline = self.prototype.clone()
                df_transformed = transform_pipeline.apply(df)

                # Ordenar por ID para determinismo
                df_transformed = df_transformed.sort_values("id")

                all_records.extend(df_transformed.to_dict("records"))
                logger.info(f"Transformado: {json_file.name}")

            except Exception as e:
                logger.error(f"Error transformando {json_file.name}: {str(e)}")

        if all_records:
            # Ordenar todos los registros por ID para determinismo final
            all_records.sort(key=lambda x: x["id"])

            # Validar y estructurar con Pydantic
            try:
                validated_records = [TransformedRecord(**rec) for rec in all_records]
                records_as_dict = [
                    rec.model_dump(exclude={"processed_at"})
                    for rec in validated_records
                ]
                output_hash = self._calculate_output_hash(records_as_dict)
                execution_time = time.time() - start_time
                generated_at = datetime.now().isoformat()

                output_data = OutputData(
                    records=validated_records,
                    metadata=OutputMetadata(
                        total_records=len(validated_records),
                        execution_time_seconds=execution_time,
                        data_hash=output_hash,
                        generated_at=generated_at,
                    ),
                )

                # Guardar resultado
                output_file = self.output_dir / f"transformed_{int(start_time)}.json"
                with open(output_file, "w") as f:
                    f.write(output_data.model_dump_json(indent=2))

                logger.info(f"Transformación completa. Hash: {output_hash[:16]}...")
                logger.info(f"Archivo guardado: {output_file}")

            except Exception as e:
                logger.error(f"Error de validación o guardado: {e}")


if __name__ == "__main__":
    transformer = Transformer()
    transformer.transform()
