import os
import tempfile
import json
from pathlib import Path
from pipeline.publisher.main import Publisher, PublisherMetadata
from pipeline.contracts.schemas import OutputData


def test_publisher_metadata_creation():
    # Arrange y Act
    metadata = PublisherMetadata(
        published_at="2024-01-15T10:30:00Z",
        source_file="transformed_123.json",
        total_records=5,
        data_hash="a" * 64,
    )

    # Assert
    assert metadata.to_dict()["total_records"] == 5


def test_publisher_finds_latest_transformed_file():
    # Arrange
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)

        # Crear archivos transformados
        file1 = output_dir / "transformed_1000.json"
        file2 = output_dir / "transformed_2000.json"
        file1.write_text('{"test": 1}')
        file2.write_text('{"test": 2}')

        # Modificar tiempos
        os.utime(file1, (1000, 1000))
        os.utime(file2, (2000, 2000))

        publisher = Publisher(output_dir=str(output_dir))

        # Act
        latest_file = publisher._find_latest_transformed_file()

        # Assert
        assert latest_file.name == "transformed_2000.json"


def test_publisher_validates_transformed_data():
    # Arrange
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        transformed_file = output_dir / "transformed_123.json"

        valid_data = {
            "records": [
                {
                    "id": 1,
                    "timestamp": "2024-01-01T00:00:00Z",
                    "original_value": 42.5,
                    "normalized_value": 0.5,
                    "category": "sensor_a",
                    "processed_at": "2024-01-15T10:30:00Z",
                }
            ],
            "metadata": {
                "total_records": 1,
                "execution_time_seconds": 0.123,
                "data_hash": "a" * 64,
                "generated_at": "2024-01-15T10:30:00Z",
            },
        }

        with open(transformed_file, "w") as f:
            json.dump(valid_data, f)

        publisher = Publisher(output_dir=str(output_dir))

        # Act
        validated_data = publisher._validate_transformed_data(transformed_file)

        # Assert
        assert isinstance(validated_data, OutputData)
        assert len(validated_data.records) == 1


def test_publisher_end_to_end_publish():
    # Arrange
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        transformed_file = output_dir / "transformed_123.json"

        transformed_data = {
            "records": [
                {
                    "id": 1,
                    "timestamp": "2024-01-01T00:00:00Z",
                    "original_value": 42.5,
                    "normalized_value": 0.5,
                    "category": "sensor_a",
                    "processed_at": "2024-01-15T10:30:00Z",
                },
                {
                    "id": 2,
                    "timestamp": "2024-01-01T00:01:00Z",
                    "original_value": 38.2,
                    "normalized_value": -0.3,
                    "category": "sensor_b",
                    "processed_at": "2024-01-15T10:30:00Z",
                },
            ],
            "metadata": {
                "total_records": 2,
                "execution_time_seconds": 0.456,
                "data_hash": "b" * 64,
                "generated_at": "2024-01-15T10:30:00Z",
            },
        }

        with open(transformed_file, "w") as f:
            json.dump(transformed_data, f)

        publisher = Publisher(output_dir=str(output_dir))

        # Act
        success = publisher.publish()

        # Assert
        assert success is True

        # Verificar archivo publicado existe
        published_files = list(output_dir.glob("published_*.json"))
        assert len(published_files) == 1

        # Verificar archivo transformado fue renombrado (no existe)
        transformed_files = list(output_dir.glob("transformed_*.json"))
        assert len(transformed_files) == 0

        # Verificar metadata.json
        metadata_file = output_dir / "metadata.json"
        assert metadata_file.exists()

        with open(metadata_file, "r") as f:
            metadata = json.load(f)
        assert metadata["source_file"] == "transformed_123.json"
        assert metadata["total_records"] == 2


def test_publisher_fails_when_no_files():
    # Arrange
    with tempfile.TemporaryDirectory() as tmpdir:
        publisher = Publisher(output_dir=tmpdir)

        # Act
        success = publisher.publish()

        # Assert
        assert success is False
