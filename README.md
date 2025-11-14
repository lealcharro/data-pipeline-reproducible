# Proyecto 12 - Data Pipeline Reproducible

## Descripción

Pipeline ETL local idempotente y determinista para ingesta, transformación y publicación de datos con garantias de reproducibilidad mediante hash de artefactos y validación de contratos.

**Sprint 1 - Alcance:**
- Contratos de entrada/salida con validación de esquemas
- Ingesta idempotente con detección de archivos procesados
- Tests unitarios, integración y contratos

**Sprint 2 - Alcance:**
- Transformaciones deterministas con ordenamiento por ID
- Verificador de reproducibilidad mediante comparación de hashes
- Tests de contrato entre componentes (Ingestor → Transformer)
- Integración completa del pipeline ETL

---

## Objetivos

1. **Idempotencia**: Multiples ejecuciones con mismos datos producen identicos resultados
2. **Determinismo**: Orden de procesamiento garantizado mediante ordenamiento por ID
3. **Contratos**: Validación estricta de esquemas con Pydantic (InputRecord, TransformedRecord, OutputData)
4. **Reproducibilidad**: Hash SHA-256 de artefactos para verificación

---

## Arquitectura

### Componentes (Sprint 1 + Sprint 2)

```
data/input/          -> Archivos CSV raw
    |
    v
[Ingestor]           -> Validación + Deduplicación + Hash
    |
    v
data/intermediate/   -> JSON validados (InputRecord)
    |
    v
[Transformer]        -> Normalización + Agregación + Metadata
    |
    v
data/output/         -> JSON transformados (TransformedRecord + OutputData)
```

**Verificador:** Script `verify_reproducibility.py` compara hashes SHA-256 entre ejecuciones

### Patrones de Diseño

**Factory Pattern (DataSourceFactory)**
- Abstracción para fuentes de datos (CSV, futuro: JSON, Parquet)
- `DataSource` (interfaz) -> `CSVDataSource` (implementación)
- Facilita extensión sin modificar Ingestor

**Dependency Inversión Principle (DIP)**
- `Ingestor` depende de abstracción `DataSource`, no de implementación concreta
- Permite inyección de dependencias para testing (mock data sources)

**Repository Pattern**
- Registro de hashes procesados en `.processed_hashes.json`
- Garantiza idempotencia entre ejecuciones

**Prototype Pattern (TransformationPrototype)** - Sprint 2
- Clonación de pipelines de transformación
- Composición de transformaciones deterministas: normalización, limpieza, metadata
- Permite aplicar mismas transformaciones a múltiples datasets sin mutación
- Transformaciones incluyen: `_clean_data()`, `_normalize_values()`, `_add_metadata()`

**Configuración Centralizada (config.py)** - Sprint 2
- Variables de entorno con `.env` para INPUT_DIR, INTERMEDIATE_DIR, OUTPUT_DIR, LOG_LEVEL
- Principio DRY: single source of truth para paths y configuración
- Facilita testing con directorios temporales

---

## Como Correr

```bash
# Opción 1: Docker (IaC)
make build
make run

# Opción 2: Local
make setup
source .venv/bin/activate
python pipeline/ingestor/main.py      # Paso 1: Ingesta
python pipeline/transformer/main.py    # Paso 2: Transformación
python scripts/verify_reproducibility.py  # Paso 3: Verificación
```

**Comandos disponibles:**

| Comando | Descripción |
|---------|-------------|
| `make setup` | Crear virtualenv, instalar dependencias, crear directorios |
| `make build` | Construir contenedores Docker (IaC local) |
| `make run` | Ejecutar pipeline completo en Docker |
| `make test` | Ejecutar suite completa de tests con cobertura |
| `make clean` | Limpiar artefactos y cache |
| `make hooks` | Instalar git hooks para validación previa |
| `make verify-hash` | Verificar reproducibilidad comparando hashes SHA-256 |
| `make run-all` | Pipeline completo: clean + build + run + verify-hash |

---

## Infraestructura como Código (IaC)

### Validación Previa

```bash
# Instalar hooks de validación
make hooks

# Se ejecuta automaticamente en cada commit:
- black (formateo)
- flake8 (linting)
- mypy (type checking)
- pytest (tests rapidos)
```

### Aplicar IaC Local

```bash
# 1. Validar sintaxis de docker-compose
docker-compose config

# 2. Construir servicios
make build

# 3. Ejecutar pipeline
make run
```

---

## Ejecución de Tests y Gates

### Tests

```bash
# Suite completa con cobertura
make test

# Por tipo
pytest tests/unit/ -v                    # Tests unitarios
pytest tests/integration/ -v             # Tests de integración
pytest tests/contracts/ -v               # Contract tests (Sprint 2)
pytest tests/e2e/ -v                     # Tests end-to-end
```

#### Tests de Contrato (Sprint 2)

Validan interfaces entre componentes:
- `test_ingest_output_contract.py`: Valida salida del Ingestor
- `test_transform_input_contract.py`: Valida entrada/salida del Transformer
- `test_ingest_transform.py`: Tests de integración Ingestor → Transformer

Ver `docs/contracts.md` para detalles completos

### Quality Gates

| Gate | Criterio | Comando |
|------|----------|---------|
| **Tests** | 100% pass | `make test` |
| **Cobertura** | 84% >= 80% | `pytest --cov` |
| **Linting** | 0 errores flake8 | `flake8 pipeline/` |
| **Type Checking** | 0 errores mypy | `mypy pipeline/` |

---

## Videos

- **Sprint 1**: https://drive.google.com/file/d/1FagOMbz9wplF_5zPWm5tcINZJQou5mS4/view?usp=drive_link
- **Sprint 2**: https://drive.google.com/file/d/113OxXJtw3Bj42z_xLeh2NvQekQwHgUYL/view?usp=sharing

---

## Equipo 7

- Aaron Flores Alberca
- Diego Delgado
- Leonardo Chacon Roque
