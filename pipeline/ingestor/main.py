import hashlib
import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from pipeline.contracts.schemas import InputRecord

log_level = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=getattr(logging, log_level))
logger = logging.getLogger(__name__)


# Factory Pattern para CSV
class DataSourceFactory:
    """Factory para crear fuentes de datos CSV"""

    @staticmethod
    def create_source(source_type: str = "csv"):
        if source_type == "csv":
            return CSVDataSource()
        else:
            raise ValueError(f"Tipo de fuente no soportado: {source_type}")


class DataSource(ABC):
    """Interfaz abstracta para fuentes de datos"""

    @abstractmethod
    def read(self, filepath: Path) -> pd.DataFrame:
        pass


class CSVDataSource(DataSource):
    """Implementación para archivos CSV con manejo robusto de encoding"""

    def read(self, filepath: Path) -> pd.DataFrame:
        try:
            return pd.read_csv(filepath, encoding="utf-8")
        except UnicodeDecodeError:
            return pd.read_csv(filepath, encoding="latin-1")


class Ingestor:
    """Componente principal de ingesta con idempotencia"""

    def __init__(
        self
    ):
        self.input_dir = Path(os.getenv("INPUT_DIR", "data/input"))
        self.output_dir = Path(os.getenv("INTERMEDIATE_DIR", "data/intermediate"))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.processed_hashes = self._load_processed_hashes()
        self.factory = DataSourceFactory()

    def _load_processed_hashes(self) -> set:
        """Cargar hashes de archivos ya procesados"""
        hash_file = self.output_dir / ".processed_hashes.json"
        if hash_file.exists():
            with open(hash_file, "r") as f:
                return set(json.load(f))
        return set()

    def _save_processed_hashes(self):
        """Guardar hashes de archivos procesados"""
        hash_file = self.output_dir / ".processed_hashes.json"
        with open(hash_file, "w") as f:
            json.dump(list(self.processed_hashes), f)

    def _calculate_file_hash(self, filepath: Path) -> str:
        """Calcular hash SHA256 de un archivo"""
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def _validate_record(self, row: Dict) -> Optional[InputRecord]:
        """Validar una fila usando el schema de Pydantic"""
        try:
            return InputRecord(**row)
        except Exception as e:
            logger.warning(f"Registro inválido: {e}")
            return None

    def ingest(self):
        """Proceso principal de ingesta idempotente"""
        csv_files = list(self.input_dir.glob("*.csv"))
        logger.info(f"Encontrados {len(csv_files)} archivos CSV")

        for csv_file in csv_files:
            file_hash = self._calculate_file_hash(csv_file)

            # Idempotencia: skip si ya fue procesado
            if file_hash in self.processed_hashes:
                logger.info(
                    f"Archivo {csv_file.name} ya procesado (hash: {file_hash[:8]}...)"
                )
                continue

            try:
                # Leer datos usando factory
                source = self.factory.create_source("csv")
                df = source.read(csv_file)

                # Validar esquema básico
                required_columns = ["id", "timestamp", "value", "category"]
                if not all(col in df.columns for col in required_columns):
                    logger.error(
                        f"Archivo {csv_file.name} no tiene las columnas requeridas"
                    )
                    continue

                # Validar cada registro con pydantic
                valid_records = []
                for _, row in df.iterrows():
                    record = self._validate_record(row.to_dict())
                    if record:
                        valid_records.append(record.model_dump())

                if not valid_records:
                    logger.error(f"Archivo {csv_file.name} no tiene registros válidos")
                    continue

                # Guardar en intermediate con timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = self.output_dir / f"{csv_file.stem}_{timestamp}.json"

                # Convertir a JSON manteniendo orden
                df_valid = pd.DataFrame(valid_records)
                df_sorted = df_valid.sort_values("id")
                df_sorted.to_json(output_file, orient="records", indent=2)

                # Marcar como procesado
                self.processed_hashes.add(file_hash)
                self._save_processed_hashes()

                logger.info(f"Procesado: {csv_file.name} → {output_file.name}")

            except Exception as e:
                logger.error(f"Error procesando {csv_file.name}: {str(e)}")

        logger.info(f"Ingesta completa. Total procesados: {len(self.processed_hashes)}")


if __name__ == "__main__":
    ingestor = Ingestor()
    ingestor.ingest()
