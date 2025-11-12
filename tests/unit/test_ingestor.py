import pytest
import tempfile
import json
from pathlib import Path
from pipeline.ingestor.main import Ingestor, DataSourceFactory


def test_data_source_factory_csv():
    # Arrange y Act
    source = DataSourceFactory.create_source("csv")

    # Assert
    assert source is not None
    assert source.__class__.__name__ == "CSVDataSource"


def test_data_source_factory_invalid():
    # Arrange, Act y Assert
    with pytest.raises(ValueError, match="Tipo de fuente no soportado"):
        DataSourceFactory.create_source("xml")


def test_ingestor_calculate_file_hash():
    # Arrange
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = Path(tmpdir) / "test.csv"
        test_file.write_text("id,value\n1,100\n")

        ingestor = Ingestor(input_dir=tmpdir, output_dir=tmpdir)

        # Act
        hash1 = ingestor._calculate_file_hash(test_file)
        hash2 = ingestor._calculate_file_hash(test_file)

        # Assert
        assert len(hash1) == 64  # SHA256 hash length
        assert hash1 == hash2  # Mismo archivo = mismo hash


def test_ingestor_idempotency():
    # Arrange
    with tempfile.TemporaryDirectory() as tmpdir:
        input_dir = Path(tmpdir) / "input"
        output_dir = Path(tmpdir) / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        # Crear CSV de prueba
        csv_file = input_dir / "test.csv"
        csv_file.write_text(
            "id,timestamp,value,category\n1,2024-01-15T10:30:00Z,42.5,sensor_a\n"
        )

        ingestor = Ingestor(input_dir=str(input_dir), output_dir=str(output_dir))

        # Act - Primera ejecución
        ingestor.ingest()
        data_files_first = [
            f for f in output_dir.glob("*.json") if not f.name.startswith(".")
        ]

        # Act - Segunda ejecución (idempotencia)
        ingestor.ingest()
        data_files_second = [
            f for f in output_dir.glob("*.json") if not f.name.startswith(".")
        ]

        # Assert
        assert len(data_files_first) == 1  # Solo un archivo procesado
        assert len(data_files_second) == 1  # No se duplica
        assert len(ingestor.processed_hashes) == 1  # Solo un hash registrado


def test_ingestor_valid_csv_processing():
    # Arrange
    with tempfile.TemporaryDirectory() as tmpdir:
        input_dir = Path(tmpdir) / "input"
        output_dir = Path(tmpdir) / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        # Crear CSV válido
        csv_file = input_dir / "data.csv"
        csv_file.write_text(
            "id,timestamp,value,category\n"
            "1,2024-01-15T10:30:00Z,42.5,sensor_a\n"
            "2,2024-01-15T10:31:00Z,38.2,sensor_b\n"
        )

        ingestor = Ingestor(input_dir=str(input_dir), output_dir=str(output_dir))

        # Act
        ingestor.ingest()

        # Assert
        data_files = [
            f for f in output_dir.glob("*.json") if not f.name.startswith(".")
        ]
        assert len(data_files) == 1

        # Verificar contenido
        with open(data_files[0], "r") as f:
            data = json.load(f)
        assert len(data) == 2
        assert data[0]["id"] == 1
        assert data[0]["category"] == "sensor_a"
