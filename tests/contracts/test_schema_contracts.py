import pytest
from pipeline.contracts.schemas import (
    InputRecord,
    TransformedRecord,
    OutputData,
    OutputMetadata,
)


def test_input_record_valido():
    # Arrange y Act
    record = InputRecord(
        id=1, timestamp="2024-01-15T10:30:00Z", value=42.5, category="sensor_a"
    )

    # Assert
    assert record.id == 1
    assert record.timestamp == "2024-01-15T10:30:00Z"
    assert record.value == 42.5
    assert record.category == "sensor_a"


def test_transformed_record_validacion():
    # Arrange y Act
    valid_record = TransformedRecord(
        id=1,
        timestamp="2024-01-15T10:30:00Z",
        original_value=42.5,
        normalized_value=0.5,
        category="sensor_a",
        processed_at="2024-01-15T11:00:00Z",
    )

    # Assert
    assert valid_record.normalized_value == 0.5

    # Validación de captura de error
    with pytest.raises(ValueError):
        TransformedRecord(
            id=2,
            timestamp="2024-01-15T10:30:00Z",
            original_value=100.0,
            normalized_value=1.5,  # Valor invalido > 1.0
            category="sensor_b",
            processed_at="2024-01-15T11:00:00Z",
        )


def test_output_data_with_metadata():
    # Arrange y Act
    record = TransformedRecord(
        id=1,
        timestamp="2024-01-15T10:30:00Z",
        original_value=42.5,
        normalized_value=0.65,
        category="sensor_a",
        processed_at="2024-01-15T11:00:00Z",
    )

    metadata = OutputMetadata(
        total_records=1,
        execution_time_seconds=1.234,
        data_hash="a" * 64,  # Hash SHA256 valido (64 caracteres hex)
        generated_at="2024-01-15T11:00:05Z",
    )

    # Crear salida completa
    output = OutputData(records=[record], metadata=metadata)

    # Assert
    assert len(output.records) == 1
    assert output.records[0].id == 1
    assert output.metadata.total_records == 1
    assert output.metadata.data_hash == "a" * 64
