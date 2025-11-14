import pytest
import tempfile
import json
from pathlib import Path
from pipeline.transformer.main import Transformer, TransformationPrototype
from pipeline.contracts.schemas import OutputData


def test_transformer_prototype_creation():
    # Arrange
    with tempfile.TemporaryDirectory() as tmpdir:
        input_dir = Path(tmpdir) / "input"
        output_dir = Path(tmpdir) / "output"
        input_dir.mkdir()
        output_dir.mkdir()
        transformer = Transformer(str(input_dir), str(output_dir))

        # Act
        prototype = transformer.prototype

        # Assert
        assert isinstance(prototype, TransformationPrototype)
        assert len(prototype.transformations) == 3


def test_transformer_deterministic_output_hash():
    # Arrange
    with tempfile.TemporaryDirectory() as tmpdir:
        input_dir = Path(tmpdir) / "input"
        output_dir = Path(tmpdir) / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        # Crear archivo de entrada
        input_file = input_dir / "test_data.json"
        input_data = [
            {
                "id": 1,
                "value": 100,
                "timestamp": "2024-01-01T00:00:00Z",
                "category": "A",
            },
            {
                "id": 2,
                "value": 150,
                "timestamp": "2024-01-01T00:01:00Z",
                "category": "B",
            },
        ]
        with open(input_file, "w") as f:
            json.dump(input_data, f)

        # Act
        # Primera ejecución
        transformer1 = Transformer(str(input_dir), str(output_dir))
        transformer1.transform()
        output_files1 = list(output_dir.glob("*.json"))
        with open(output_files1[0], "r") as f:
            data1 = json.load(f)

        # Limpiar y segunda ejecución
        for f in output_dir.glob("*.json"):
            f.unlink()

        transformer2 = Transformer(str(input_dir), str(output_dir))
        transformer2.transform()
        output_files2 = list(output_dir.glob("*.json"))
        with open(output_files2[0], "r") as f:
            data2 = json.load(f)

        # Assert
        assert data1["metadata"]["data_hash"] == data2["metadata"]["data_hash"]


def test_transformer_end_to_end_transformation():
    # Arrange
    with tempfile.TemporaryDirectory() as tmpdir:
        input_dir = Path(tmpdir) / "input"
        output_dir = Path(tmpdir) / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        input_file = input_dir / "test_data.json"
        input_data = [
            {
                "id": 1,
                "value": 10,
                "timestamp": "2024-01-01T00:00:00Z",
                "category": "A",
            },
            {
                "id": 1,
                "value": 15,
                "timestamp": "2024-01-01T00:00:00Z",
                "category": "A",
            },  # Duplicado
            {
                "id": 2,
                "value": 1000,
                "timestamp": "2024-01-01T00:01:00Z",
                "category": "B",
            },  # Outlier
        ]
        with open(input_file, "w") as f:
            json.dump(input_data, f)

        transformer = Transformer(str(input_dir), str(output_dir))

        # Act
        transformer.transform()

        # Assert
        output_files = list(output_dir.glob("*.json"))
        assert len(output_files) == 1

        with open(output_files[0], "r") as f:
            output_data = json.load(f)

        assert len(output_data["records"]) == 2
        assert output_data["records"][0]["id"] == 1
        assert (
            output_data["records"][0]["original_value"] == 10.0
        )  # Mantiene el primero
        assert (
            output_data["records"][0]["normalized_value"] == -1
        )  # Mantiene el primero
        assert output_data["records"][1]["id"] == 2
        assert output_data["records"][1]["original_value"] == 1000.0
        assert output_data["records"][1]["normalized_value"] == 1


def test_transformer_handles_empty_input():
    # Arrange
    with tempfile.TemporaryDirectory() as tmpdir:
        input_dir = Path(tmpdir) / "input"
        output_dir = Path(tmpdir) / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        transformer = Transformer(str(input_dir), str(output_dir))

        # Act
        transformer.transform()

        # Assert
        output_files = list(output_dir.glob("*.json"))
        assert len(output_files) == 0


def test_transformer_output_schema_validation():
    # Arrange
    with tempfile.TemporaryDirectory() as tmpdir:
        input_dir = Path(tmpdir) / "input"
        output_dir = Path(tmpdir) / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        input_file = input_dir / "test_data.json"
        input_data = [
            {
                "id": 1,
                "value": 10,
                "timestamp": "2024-01-01T00:00:00Z",
                "category": "A",
            },
        ]
        with open(input_file, "w") as f:
            json.dump(input_data, f)

        transformer = Transformer(str(input_dir), str(output_dir))

        # Act
        transformer.transform()
        output_files = list(output_dir.glob("*.json"))
        with open(output_files[0], "r") as f:
            output_json = json.load(f)

        # Assert
        try:
            OutputData(**output_json)
        except Exception as e:
            pytest.fail(f"La validación del esquema de salida falló: {e}")
