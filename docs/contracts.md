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

---

## Tests de Contrato

Los tests de contrato validan que las interfaces entre componentes del pipeline se mantengan estables y correctas.

### test_schema_contracts.py

Valida los esquemas Pydantic base.

**Tests incluidos:**
- `test_input_record_valido`: Verifica creación correcta de InputRecord
- `test_transformed_record_validacion`: Valida TransformedRecord y rango de normalized_value
- `test_output_data_with_metadata`: Verifica OutputData con metadata completa

**Qué valida:**
- Campos requeridos presentes
- Tipos de datos correctos
- Validaciones de rangos (normalized_value entre -1.0 y 1.0)
- Validaciones de formato (timestamps ISO 8601, hash SHA256)

---

### test_ingest_output_contract.py

Valida el contrato de salida del componente de ingesta.

**Tests incluidos:**
- `test_ingest_output_is_valid_json_array`: Salida es array JSON válido
- `test_ingest_output_records_match_input_record_schema`: Registros conformes a InputRecord
- `test_ingest_output_records_are_sorted_by_id`: Registros ordenados por ID ascendente
- `test_ingest_output_rejects_invalid_records`: Registros inválidos son filtrados

**Garantías del contrato:**
- Formato JSON válido con estructura de array
- Todos los registros cumplen esquema InputRecord
- Ordenamiento consistente por ID
- Filtrado automático de registros que no cumplen validaciones

---

### test_transform_input_contract.py

Valida el contrato de entrada/salida del componente de transformación.

**Tests incluidos:**
- `test_input_record_can_be_transformed_to_transformed_record`: Conversión InputRecord → TransformedRecord
- `test_transform_input_validates_timestamp_format`: Validación estricta de timestamps ISO 8601
- `test_transform_input_validates_positive_id`: IDs deben ser positivos (> 0)
- `test_transform_input_validates_non_empty_category`: Categorías no pueden estar vacías
- `test_transform_output_normalized_value_bounds`: normalized_value debe estar en [-1.0, 1.0]

**Garantías del contrato:**
- InputRecord puede convertirse a TransformedRecord sin pérdida de datos
- Preservación de campos originales (id, timestamp, value, category)
- Validaciones estrictas de formato y rangos
- Rechazo de datos inválidos con excepciones claras

---

## Ejecución de Tests

```bash
# Ejecutar todos los tests de contrato
pytest tests/contracts/ -v

# Ejecutar archivo específico
pytest tests/contracts/test_ingest_output_contract.py -v

# Con cobertura
pytest tests/contracts/ --cov=pipeline.contracts --cov-report=html
```
