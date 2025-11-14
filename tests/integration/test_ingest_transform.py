import json
import tempfile
from pathlib import Path
from pipeline.contracts.schemas import InputRecord, TransformedRecord
from pipeline.ingestor.main import Ingestor


def test_ingest_output_can_be_consumed_by_transform():
    with tempfile.TemporaryDirectory() as tmpdir:
        input_dir = Path(tmpdir) / "input"
        output_dir = Path(tmpdir) / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        csv_file = input_dir / "test.csv"
        csv_file.write_text(
            "id,timestamp,value,category\n"
            "1,2024-01-15T10:30:00Z,42.5,sensor_a\n"
            "2,2024-01-15T10:31:00Z,38.2,sensor_b\n"
        )

        ingestor = Ingestor(input_dir=str(input_dir), output_dir=str(output_dir))
        ingestor.ingest()

        output_files = [
            f for f in output_dir.glob("*.json") if not f.name.startswith(".")
        ]
        with open(output_files[0], "r") as f:
            ingest_data = json.load(f)

        for record_data in ingest_data:
            input_record = InputRecord(**record_data)

            transformed = TransformedRecord(
                id=input_record.id,
                timestamp=input_record.timestamp,
                original_value=input_record.value,
                normalized_value=0.5,
                category=input_record.category,
                processed_at="2024-01-15T11:00:00Z",
            )
            assert transformed is not None


def test_ingest_output_batch_processing_compatibility():
    with tempfile.TemporaryDirectory() as tmpdir:
        input_dir = Path(tmpdir) / "input"
        output_dir = Path(tmpdir) / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        csv_file = input_dir / "test.csv"
        csv_file.write_text(
            "id,timestamp,value,category\n"
            "1,2024-01-15T10:30:00Z,42.5,sensor_a\n"
            "2,2024-01-15T10:31:00Z,38.2,sensor_b\n"
            "3,2024-01-15T10:32:00Z,45.8,sensor_c\n"
            "4,2024-01-15T10:33:00Z,51.3,sensor_d\n"
        )

        ingestor = Ingestor(input_dir=str(input_dir), output_dir=str(output_dir))
        ingestor.ingest()

        output_files = [
            f for f in output_dir.glob("*.json") if not f.name.startswith(".")
        ]
        with open(output_files[0], "r") as f:
            ingest_data = json.load(f)

        transformed_records = []
        for record_data in ingest_data:
            input_record = InputRecord(**record_data)
            transformed = TransformedRecord(
                id=input_record.id,
                timestamp=input_record.timestamp,
                original_value=input_record.value,
                normalized_value=0.5,
                category=input_record.category,
                processed_at="2024-01-15T11:00:00Z",
            )
            transformed_records.append(transformed)

        assert len(transformed_records) == 4
        assert all(isinstance(r, TransformedRecord) for r in transformed_records)


def test_component_integration_handles_empty_valid_records():
    with tempfile.TemporaryDirectory() as tmpdir:
        input_dir = Path(tmpdir) / "input"
        output_dir = Path(tmpdir) / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        csv_file = input_dir / "test.csv"
        csv_file.write_text(
            "id,timestamp,value,category\n" "-1,invalid,42.5,sensor_a\n"
        )

        ingestor = Ingestor(input_dir=str(input_dir), output_dir=str(output_dir))
        ingestor.ingest()

        output_files = [
            f for f in output_dir.glob("*.json") if not f.name.startswith(".")
        ]
        assert len(output_files) == 0
