import hashlib
import json
import tempfile
import time
from pathlib import Path

import pytest

from pipeline.contracts.schemas import (
    InputRecord,
    OutputData,
    OutputMetadata,
    TransformedRecord,
)
from pipeline.ingestor.main import Ingestor
from pipeline.publisher.main import Publisher
from pipeline.transformer.main import Transformer


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


@pytest.fixture
def temp_dirs():
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        dirs = {
            "input": base / "input",
            "intermediate": base / "intermediate",
            "output": base / "output",
        }
        for d in dirs.values():
            d.mkdir()
        yield dirs


def create_csv_data(num_records: int, base_id: int = 1) -> str:
    lines = ["id,timestamp,value,category"]
    for i in range(num_records):
        record_id = base_id + i
        timestamp = f"2024-01-15T{10 + i % 14:02d}:{i % 60:02d}:00Z"
        value = 40.0 + (i % 20)
        category = f"sensor_{chr(97 + (i % 10))}"
        lines.append(f"{record_id},{timestamp},{value},{category}")
    return "\n".join(lines)


@pytest.mark.parametrize(
    "num_records,expected_count",
    [
        (100, 100),
        (1000, 1000),
    ],
)
def test_pipeline_with_different_volumes(
    temp_dirs, num_records, expected_count
):
    csv_file = temp_dirs["input"] / f"test_{num_records}.csv"
    csv_file.write_text(create_csv_data(num_records))

    ingestor = Ingestor(
        input_dir=str(temp_dirs["input"]),
        output_dir=str(temp_dirs["intermediate"]),
    )
    ingestor.ingest()

    transformer = Transformer(
        input_dir=str(temp_dirs["intermediate"]),
        output_dir=str(temp_dirs["output"]),
    )
    transformer.transform()

    publisher = Publisher(output_dir=str(temp_dirs["output"]))
    result = publisher.publish()

    assert result is True
    published_files = list(temp_dirs["output"].glob("published_*.json"))
    assert len(published_files) == 1

    with open(published_files[0], "r") as f:
        output_data = OutputData(**json.load(f))

    assert output_data.metadata.total_records == expected_count
    assert len(output_data.records) == expected_count


def calculate_file_hash(file_path: Path) -> str:
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def test_complete_idempotency_multiple_executions(temp_dirs):
    csv_file = temp_dirs["input"] / "test.csv"
    csv_file.write_text(create_csv_data(50))

    hashes = []
    for _ in range(3):
        ingestor = Ingestor(
            input_dir=str(temp_dirs["input"]),
            output_dir=str(temp_dirs["intermediate"]),
        )
        ingestor.ingest()

        intermediate_files = list(temp_dirs["intermediate"].glob("*.json"))
        non_hash_files = [
            f for f in intermediate_files if not f.name.startswith(".")
        ]

        assert len(non_hash_files) == 1
        file_hash = calculate_file_hash(non_hash_files[0])
        hashes.append(file_hash)

    assert len(set(hashes)) == 1


def test_complete_determinism_identical_hash(temp_dirs):
    csv_file = temp_dirs["input"] / "test.csv"
    csv_file.write_text(create_csv_data(100))

    data_hashes = []
    for _ in range(3):
        for f in temp_dirs["intermediate"].glob("*"):
            f.unlink()
        for f in temp_dirs["output"].glob("*"):
            f.unlink()

        ingestor = Ingestor(
            input_dir=str(temp_dirs["input"]),
            output_dir=str(temp_dirs["intermediate"]),
        )
        ingestor.ingest()

        transformer = Transformer(
            input_dir=str(temp_dirs["intermediate"]),
            output_dir=str(temp_dirs["output"]),
        )
        transformer.transform()

        transformed_files = list(temp_dirs["output"].glob("transformed_*.json"))
        with open(transformed_files[0], "r") as f:
            output_data = OutputData(**json.load(f))

        data_hashes.append(output_data.metadata.data_hash)

    assert len(set(data_hashes)) == 1
