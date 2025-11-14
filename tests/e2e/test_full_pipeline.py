import json
import tempfile
from pathlib import Path
from pipeline.contracts.schemas import (
    InputRecord,
    OutputData,
    OutputMetadata,
    TransformedRecord,
)
from pipeline.ingestor.main import Ingestor


def test_end_to_end_csv_to_transformed_records():
    with tempfile.TemporaryDirectory() as tmpdir:
        input_dir = Path(tmpdir) / "input"
        intermediate_dir = Path(tmpdir) / "intermediate"
        input_dir.mkdir()
        intermediate_dir.mkdir()

        csv_file = input_dir / "test.csv"
        csv_file.write_text(
            "id,timestamp,value,category\n"
            "1,2024-01-15T10:30:00Z,42.5,sensor_a\n"
            "2,2024-01-15T10:31:00Z,38.2,sensor_b\n"
            "3,2024-01-15T10:32:00Z,45.8,sensor_c\n"
        )

        ingestor = Ingestor(input_dir=str(input_dir), output_dir=str(intermediate_dir))
        ingestor.ingest()

        output_files = [
            f for f in intermediate_dir.glob("*.json") if not f.name.startswith(".")
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
                normalized_value=input_record.value / 100.0,
                category=input_record.category,
                processed_at="2024-01-15T11:00:00Z",
            )
            transformed_records.append(transformed)

        assert len(transformed_records) == 3
        assert all(isinstance(r, TransformedRecord) for r in transformed_records)
        assert all(-1.0 <= r.normalized_value <= 1.0 for r in transformed_records)


def test_end_to_end_with_output_data_schema():
    with tempfile.TemporaryDirectory() as tmpdir:
        input_dir = Path(tmpdir) / "input"
        intermediate_dir = Path(tmpdir) / "intermediate"
        input_dir.mkdir()
        intermediate_dir.mkdir()

        csv_file = input_dir / "test.csv"
        csv_file.write_text(
            "id,timestamp,value,category\n"
            "1,2024-01-15T10:30:00Z,42.5,sensor_a\n"
            "2,2024-01-15T10:31:00Z,38.2,sensor_b\n"
        )

        ingestor = Ingestor(input_dir=str(input_dir), output_dir=str(intermediate_dir))
        ingestor.ingest()

        output_files = [
            f for f in intermediate_dir.glob("*.json") if not f.name.startswith(".")
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

        metadata = OutputMetadata(
            total_records=len(transformed_records),
            execution_time_seconds=1.234,
            data_hash="a" * 64,
            generated_at="2024-01-15T11:00:05Z",
        )

        output_data = OutputData(records=transformed_records, metadata=metadata)

        assert len(output_data.records) == 2
        assert output_data.metadata.total_records == 2


def test_end_to_end_idempotency_check():
    with tempfile.TemporaryDirectory() as tmpdir:
        input_dir = Path(tmpdir) / "input"
        intermediate_dir = Path(tmpdir) / "intermediate"
        input_dir.mkdir()
        intermediate_dir.mkdir()

        csv_file = input_dir / "test.csv"
        csv_file.write_text(
            "id,timestamp,value,category\n" "1,2024-01-15T10:30:00Z,42.5,sensor_a\n"
        )

        ingestor = Ingestor(input_dir=str(input_dir), output_dir=str(intermediate_dir))
        ingestor.ingest()

        first_run_files = [
            f for f in intermediate_dir.glob("*.json") if not f.name.startswith(".")
        ]
        assert len(first_run_files) == 1

        ingestor.ingest()

        second_run_files = [
            f for f in intermediate_dir.glob("*.json") if not f.name.startswith(".")
        ]
        assert len(second_run_files) == 1
        assert len(ingestor.processed_hashes) == 1
