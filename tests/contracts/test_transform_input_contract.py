import pytest
from pipeline.contracts.schemas import InputRecord, TransformedRecord


def test_input_record_can_be_transformed_to_transformed_record():
    input_record = InputRecord(
        id=1, timestamp="2024-01-15T10:30:00Z", value=42.5, category="sensor_a"
    )

    transformed = TransformedRecord(
        id=input_record.id,
        timestamp=input_record.timestamp,
        original_value=input_record.value,
        normalized_value=0.5,
        category=input_record.category,
        processed_at="2024-01-15T11:00:00Z",
    )

    assert transformed.id == input_record.id
    assert transformed.timestamp == input_record.timestamp
    assert transformed.original_value == input_record.value
    assert transformed.category == input_record.category


def test_transform_input_validates_timestamp_format():
    valid_record = InputRecord(
        id=1, timestamp="2024-01-15T10:30:00Z", value=42.5, category="sensor_a"
    )
    assert valid_record.timestamp == "2024-01-15T10:30:00Z"

    with pytest.raises(ValueError):
        InputRecord(
            id=2, timestamp="invalid_timestamp", value=38.2, category="sensor_b"
        )


def test_transform_input_validates_positive_id():
    valid_record = InputRecord(
        id=1, timestamp="2024-01-15T10:30:00Z", value=42.5, category="sensor_a"
    )
    assert valid_record.id > 0

    with pytest.raises(ValueError):
        InputRecord(
            id=0, timestamp="2024-01-15T10:30:00Z", value=42.5, category="sensor_a"
        )

    with pytest.raises(ValueError):
        InputRecord(
            id=-1, timestamp="2024-01-15T10:30:00Z", value=42.5, category="sensor_a"
        )


def test_transform_input_validates_non_empty_category():
    valid_record = InputRecord(
        id=1, timestamp="2024-01-15T10:30:00Z", value=42.5, category="sensor_a"
    )
    assert len(valid_record.category) > 0

    with pytest.raises(ValueError):
        InputRecord(id=1, timestamp="2024-01-15T10:30:00Z", value=42.5, category="")

    with pytest.raises(ValueError):
        InputRecord(id=1, timestamp="2024-01-15T10:30:00Z", value=42.5, category="   ")


def test_transform_output_normalized_value_bounds():
    valid_record = TransformedRecord(
        id=1,
        timestamp="2024-01-15T10:30:00Z",
        original_value=42.5,
        normalized_value=0.5,
        category="sensor_a",
        processed_at="2024-01-15T11:00:00Z",
    )
    assert -1.0 <= valid_record.normalized_value <= 1.0

    with pytest.raises(ValueError):
        TransformedRecord(
            id=1,
            timestamp="2024-01-15T10:30:00Z",
            original_value=42.5,
            normalized_value=1.5,
            category="sensor_a",
            processed_at="2024-01-15T11:00:00Z",
        )

    with pytest.raises(ValueError):
        TransformedRecord(
            id=1,
            timestamp="2024-01-15T10:30:00Z",
            original_value=42.5,
            normalized_value=-1.5,
            category="sensor_a",
            processed_at="2024-01-15T11:00:00Z",
        )
