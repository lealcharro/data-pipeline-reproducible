import hashlib
import json
from pathlib import Path
import os


def calculate_file_hash(filepath: Path) -> str:
    """Calcula el hash SHA256 de un archivo."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def get_intermediate_hashes(intermediate_dir: Path) -> dict:
    """Obtiene los hashes de los archivos en el directorio intermedio."""
    hashes = {}
    for f in sorted(intermediate_dir.glob("*.json")):
        if f.name not in [".processed_hashes.json", ".reference_hashes.json"]:
            hashes[f.name] = calculate_file_hash(f)
    return hashes


def run_verification(base_dir: Path):
    """Compara los hashes de los artefactos con los hashes de referencia."""
    intermediate_dir = base_dir / "data" / "intermediate"

    # Hashes de referencia
    reference_hashes_file = intermediate_dir / ".reference_hashes.json"

    if not reference_hashes_file.exists():
        print("No se encontró el archivo de hashes de referencia. Creando uno nuevo.")
        current_hashes = get_intermediate_hashes(intermediate_dir)
        with open(reference_hashes_file, "w") as f:
            json.dump(current_hashes, f, indent=2)
        print(f"Hashes de referencia guardados en {reference_hashes_file}")
        return

    # Hashes actuales
    current_hashes = get_intermediate_hashes(intermediate_dir)

    with open(reference_hashes_file, "r") as f:
        reference_hashes = json.load(f)

    # Comparamos los hashes
    if current_hashes == reference_hashes:
        print("Verificación de reproducibilidad exitosa: Los hashes coinciden.")
    else:
        print("Error: Los hashes no coinciden.")
        print("Hashes actuales:", json.dumps(current_hashes, indent=2))
        print("Hashes de referencia:", json.dumps(reference_hashes, indent=2))
        exit(1)


if __name__ == "__main__":
    project_root = Path(os.getcwd())
    run_verification(project_root)
