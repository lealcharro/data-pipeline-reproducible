import json
import tempfile
from pathlib import Path
from pipeline.contracts.schemas import InputRecord
from pipeline.ingestor.main import Ingestor


def test_ingest_output_is_valid_json_array():
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

        output_files = [f for f in output_dir.glob("*.json") if not f.name.startswith(".")]
        assert len(output_files) == 1

        with open(output_files[0], "r") as f:
            data = json.load(f)

        assert isinstance(data, list)
        assert len(data) > 0


def test_ingest_output_records_match_input_record_schema():
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
        )

        ingestor = Ingestor(input_dir=str(input_dir), output_dir=str(output_dir))
        ingestor.ingest()

        output_files = [f for f in output_dir.glob("*.json") if not f.name.startswith(".")]
        with open(output_files[0], "r") as f:
            data = json.load(f)

        for record_data in data:
            record = InputRecord(**record_data)
            assert record.id > 0
            assert isinstance(record.timestamp, str)
            assert isinstance(record.value, float)
            assert isinstance(record.category, str)
            assert len(record.category) > 0


def test_ingest_output_records_are_sorted_by_id():
    with tempfile.TemporaryDirectory() as tmpdir:
        input_dir = Path(tmpdir) / "input"
        output_dir = Path(tmpdir) / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        csv_file = input_dir / "test.csv"
        csv_file.write_text(
            "id,timestamp,value,category\n"
            "3,2024-01-15T10:32:00Z,45.8,sensor_c\n"
            "1,2024-01-15T10:30:00Z,42.5,sensor_a\n"
            "2,2024-01-15T10:31:00Z,38.2,sensor_b\n"
        )

        ingestor = Ingestor(input_dir=str(input_dir), output_dir=str(output_dir))
        ingestor.ingest()

        output_files = [f for f in output_dir.glob("*.json") if not f.name.startswith(".")]
        with open(output_files[0], "r") as f:
            data = json.load(f)

        ids = [record["id"] for record in data]
        assert ids == sorted(ids)


def test_ingest_output_rejects_invalid_records():
    with tempfile.TemporaryDirectory() as tmpdir:
        input_dir = Path(tmpdir) / "input"
        output_dir = Path(tmpdir) / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        csv_file = input_dir / "test.csv"
        csv_file.write_text(
            "id,timestamp,value,category\n"
            "1,2024-01-15T10:30:00Z,42.5,sensor_a\n"
            "-5,2024-01-15T10:31:00Z,38.2,sensor_b\n"
            "3,invalid_timestamp,45.8,sensor_c\n"
        )

        ingestor = Ingestor(input_dir=str(input_dir), output_dir=str(output_dir))
        ingestor.ingest()

        output_files = [f for f in output_dir.glob("*.json") if not f.name.startswith(".")]
        with open(output_files[0], "r") as f:
            data = json.load(f)

        assert len(data) == 1
        assert data[0]["id"] == 1
