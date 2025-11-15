import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from pipeline.config import LOG_LEVEL, OUTPUT_DIR
from pipeline.contracts.schemas import OutputData

log_level = LOG_LEVEL
logging.basicConfig(level=getattr(logging, log_level))
logger = logging.getLogger(__name__)


class PublisherMetadata:
    """Metadata de publicación"""

    def __init__(
        self,
        published_at: str,
        source_file: str,
        total_records: int,
        data_hash: str,
    ):
        self.published_at = published_at
        self.source_file = source_file
        self.total_records = total_records
        self.data_hash = data_hash

    def to_dict(self):
        """Convertir a diccionario"""
        return {
            "published_at": self.published_at,
            "source_file": self.source_file,
            "total_records": self.total_records,
            "data_hash": self.data_hash,
        }


class Publisher:
    """Componente de publicación del pipeline ETL"""

    def __init__(self, output_dir: str | None = None):
        self.output_dir = Path(output_dir or OUTPUT_DIR)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _find_latest_transformed_file(self) -> Optional[Path]:
        """Encuentra el archivo transformado más reciente"""
        transformed_files = sorted(
            [f for f in self.output_dir.glob("transformed_*.json")],
            key=lambda x: x.stat().st_mtime,
            reverse=True,
        )

        if not transformed_files:
            logger.warning("No se encontraron archivos transformados para publicar")
            return None

        latest_file = transformed_files[0]
        logger.info(f"Archivo transformado encontrado: {latest_file.name}")
        return latest_file

    def _validate_transformed_data(self, file_path: Path) -> OutputData:
        """Valida datos transformados contra el esquema OutputData"""
        if not file_path.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {file_path}")

        try:
            with open(file_path, "r") as f:
                data = json.load(f)

            # Validar con Pydantic
            validated_data = OutputData(**data)
            logger.info(
                f"Datos validados: {validated_data.metadata.total_records} registros"
            )
            return validated_data

        except json.JSONDecodeError as e:
            logger.error(f"Error al parsear JSON: {e}")
            raise ValueError(f"JSON inválido en {file_path}: {e}")
        except Exception as e:
            logger.error(f"Error de validación: {e}")
            raise ValueError(f"Datos no cumplen esquema OutputData: {e}")

    def _atomic_write(self, data: str, target_path: Path) -> None:
        """Escritura atómica de archivo usando tmp con rename"""
        tmp_path = target_path.with_suffix(".tmp")

        try:
            # Escribir a archivo temporal
            with open(tmp_path, "w") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())  # Forzar escritura a disco

            # Renombrar atómicamente
            shutil.move(str(tmp_path), str(target_path))
            logger.info(f"Archivo escrito atómicamente: {target_path.name}")

        except Exception as e:
            # Limpiar archivo temporal en caso de error
            if tmp_path.exists():
                tmp_path.unlink()
            logger.error(f"Error en escritura atómica: {e}")
            raise IOError(f"Fallo al escribir {target_path}: {e}")

    def _generate_published_filename(self) -> str:
        """Genera nombre de archivo con timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"published_{timestamp}.json"

    def _create_metadata(
        self, source_file: Path, output_data: OutputData
    ) -> PublisherMetadata:
        """Crea metadata de publicación"""
        return PublisherMetadata(
            published_at=datetime.now().isoformat(),
            source_file=source_file.name,
            total_records=output_data.metadata.total_records,
            data_hash=output_data.metadata.data_hash,
        )

    def publish(self) -> bool:
        """Proceso principal de publicación"""
        try:
            logger.info("Iniciando proceso de publicación:")

            # Encontrar archivo transformado
            source_file = self._find_latest_transformed_file()
            if source_file is None:
                logger.error("No hay archivos para publicar")
                return False

            # Validar datos
            validated_data = self._validate_transformed_data(source_file)

            # Generar nombre de archivo publicado
            published_filename = self._generate_published_filename()
            published_path = self.output_dir / published_filename

            # Renombrar archivo transformado a publicado
            logger.info(f"Publicando a: {published_filename}")
            try:
                shutil.move(str(source_file), str(published_path))
            except Exception as e:
                logger.error(f"Error al renombrar archivo: {e}")
                raise IOError(f"Fallo al renombrar {source_file}: {e}")

            # Generar metadata.json
            metadata = self._create_metadata(source_file, validated_data)
            metadata_path = self.output_dir / "metadata.json"
            metadata_json = json.dumps(metadata.to_dict(), indent=2)
            self._atomic_write(metadata_json, metadata_path)

            # Log de éxito
            logger.info("Publicación completada exitosament")

            return True

        except FileNotFoundError as e:
            logger.error(f"Archivo no encontrado: {e}")
            return False
        except ValueError as e:
            logger.error(f"Error de validación: {e}")
            return False
        except IOError as e:
            logger.error(f"Error de I/O: {e}")
            return False
        except Exception as e:
            logger.error(f"Error inesperado en publicación: {e}")
            return False


if __name__ == "__main__":
    publisher = Publisher()
    success = publisher.publish()
    exit(0 if success else 1)
