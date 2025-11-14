import json
from pathlib import Path
import pytest
from scripts.verify_reproducibility import (
    run_verification,
    get_intermediate_hashes,
    calculate_file_hash,
)


@pytest.fixture
def temp_dir(tmp_path: Path) -> Path:
    """Crea un directorio temporal para los tests"""
    return tmp_path


@pytest.fixture
def setup_test_environment(temp_dir: Path):
    """Configura un entorno de prueba con directorios y archivos"""
    intermediate_dir = temp_dir / "data" / "intermediate"
    intermediate_dir.mkdir(parents=True, exist_ok=True)

    # Crear archivos dummy
    (intermediate_dir / "file1.json").write_text('{"key": "value1"}')
    (intermediate_dir / "file2.json").write_text('{"key": "value2"}')

    return temp_dir


def test_run_verification_no_reference_file(setup_test_environment: Path, capsys):
    """
    Prueba que el archivo de hashes de referencia se crea si no existe
    """
    # Arrange
    reference_file = (
        setup_test_environment / "data" / "intermediate" / ".reference_hashes.json"
    )

    # Act
    run_verification(setup_test_environment)

    # Assert
    assert reference_file.exists()

    with open(reference_file, "r") as f:
        reference_hashes = json.load(f)

    assert "file1.json" in reference_hashes
    assert "file2.json" in reference_hashes

    captured = capsys.readouterr()
    assert "No se encontró el archivo de hashes de referencia" in captured.out


def test_run_verification_success(setup_test_environment: Path, capsys):
    """
    Prueba que la verificación es exitosa cuando los hashes coinciden
    """
    # Arrange
    intermediate_dir = setup_test_environment / "data" / "intermediate"
    reference_hashes = get_intermediate_hashes(intermediate_dir)
    with open(intermediate_dir / ".reference_hashes.json", "w") as f:
        json.dump(reference_hashes, f)

    # Act
    run_verification(setup_test_environment)

    # Assert
    captured = capsys.readouterr()
    assert "Verificación de reproducibilidad exitosa" in captured.out


def test_run_verification_failure(setup_test_environment: Path):
    """
    Prueba que la verificación falla cuando los hashes no coinciden
    """
    # Arrange
    intermediate_dir = setup_test_environment / "data" / "intermediate"
    reference_hashes = {"file1.json": "incorrect_hash"}
    with open(intermediate_dir / ".reference_hashes.json", "w") as f:
        json.dump(reference_hashes, f)

    # Act & Assert
    with pytest.raises(SystemExit) as e:
        run_verification(setup_test_environment)

    assert e.type == SystemExit
    assert e.value.code == 1


def test_get_intermediate_hashes(setup_test_environment: Path):
    """
    Prueba que la función get_intermediate_hashes funciona correctamente
    """
    # Arrange
    intermediate_dir = setup_test_environment / "data" / "intermediate"

    # Act
    hashes = get_intermediate_hashes(intermediate_dir)

    # Assert
    assert "file1.json" in hashes
    assert "file2.json" in hashes
    assert len(hashes) == 2


def test_calculate_file_hash(setup_test_environment: Path):
    """
    Prueba que la función calculate_file_hash funciona correctamente
    """
    # Arrange
    file_path = setup_test_environment / "data" / "intermediate" / "file1.json"
    expected_hash = "92e4b3ee7ed894314bfd93d15592a1853c8fb6a1133881019a0e94d5a32cbb7f"

    # Act
    hash_value = calculate_file_hash(file_path)

    # Assert
    assert hash_value == expected_hash
