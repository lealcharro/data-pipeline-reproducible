# Contratos de Datos - Pipeline ETL

## Esquemas Definidos

### 1. InputRecord (Entrada CSV)

Esquema para los registros de entrada del archivo CSV.

**Campos:**

| Campo | Tipo | Validación |
|-------|------|------------|
| `id` | int | - |
| `timestamp` | str | Formato ISO 8601 |
| `value` | float | Cualquier número flotante |
| `category` | str | No puede estar vacía |

**Validaciones:**
- El `timestamp` debe estar en formato ISO 8601
- El `category` no puede estar vacío después de eliminar espacios en blanco

---

### 2. TransformedRecord (Registro Transformado)

Esquema para registros después de la transformación.

**Campos:**

| Campo | Tipo | Validación |
|-------|------|------------|
| `id` | int | - |
| `timestamp` | str | Formato ISO 8601 |
| `original_value` | float | - |
| `normalized_value` | float | Debe estar entre -1.0 y 1.0 |
| `category` | str | - |
| `processed_at` | str | Formato ISO 8601 |

**Validaciones:**
- `normalized_value` debe estar en el rango [-1.0, 1.0]
- `timestamp` y `processed_at` deben ser ISO 8601 válidos

---

### 3. OutputData (Salida Final)

Esquema contenedor para la salida completa del pipeline.

**Campos:**

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `records` | List[TransformedRecord] | Lista de registros transformados |
| `metadata` | OutputMetadata | Metadata del procesamiento |

**Validaciones:**
- La lista `records` debe contener al menos un registro

---

### 4. OutputMetadata (Metadata de Salida)

Metadata sobre la ejecución del pipeline.

**Campos:**

| Campo | Tipo | Validación |
|-------|------|------------|
| `total_records` | int | Debe ser ≥ 0 |
| `execution_time_seconds` | float | Debe ser ≥ 0.0 |
| `data_hash` | str | Debe ser hexadecimal de 64 caracteres (SHA256) |
| `generated_at` | str | Formato ISO 8601 |

---

## Flujo de Datos

```
┌─────────────────┐
│  Input CSV      │  InputRecord
│  (Raw Data)     │  - id, timestamp, value, category
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Ingestor       │  Validación + Hash
│  (Idempotente)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Transformer    │  TransformedRecord
│  (Determinista) │  - normalized_value, processed_at
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Output JSON    │  OutputData + OutputMetadata
│  (Final)        │  - records[], metadata{}, hash
└─────────────────┘
```
