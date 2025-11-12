# Proyecto 12 - Data Pipeline Reproducible

## Descripción

Pipeline ETL local idempotente y determinista para ingesta, transformación y publicación de datos con garantias de reproducibilidad mediante hash de artefactos y validación de contratos.

**Sprint 1 - Alcance:**
- Contratos de entrada/salida con validación de esquemas
- Ingesta idempotente con detección de archivos procesados
- Tests unitarios, integración y contratos

---

## Objetivos

1. **Idempotencia**: Multiples ejecuciones con mismos datos producen identicos resultados
2. **Determinismo**: Orden de procesamiento garantizado mediante ordenamiento por ID
3. **Contratos**: Validación estricta de esquemas con Pydantic (InputRecord, TransformedRecord, OutputData)
4. **Reproducibilidad**: Hash SHA-256 de artefactos para verificación

---

## Arquitectura

### Componentes (Sprint 1)

```
data/input/          -> Archivos CSV raw
    |
    v
[Ingestor]           -> Validación + Deduplicación
    |
    v
data/intermediate/   -> JSON validados
```

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

---

## Como Correr

```bash
# Opción 1: Docker (IaC)
make build
make run

# Opción 2: Local
make setup
source .venv/bin/activate
python pipeline/ingestor/main.py
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
pytest tests/contracts/ -v               # Contract tests
pytest tests/e2e/ -v                     # Tests end-to-end
```

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

---

## Equipo 7

- Aaron Flores Alberca
- Diego Delgado
- Leonardo Chacon Roque
