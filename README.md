# RetailStart Big Data Pipeline

Simulación de una arquitectura Lambda Big Data para **RetailStart Chile S.A.**, implementando ingesta incremental, procesamiento distribuido con Spark y almacenamiento en un Data Warehouse con modelo estrella.

## Tecnologías

- Python 3.11+
- Apache Spark (PySpark)
- SQLite
- Kafka simulado (JSON topic)
- Flask + Chart.js + Bootstrap

## Arquitectura

```text
┌─────────────────────────────┐
│ FUENTES                     │
├─────────────────────────────┤
│ CSV (ventas, clientes,      │
│ productos) + JSON (eventos) │
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│ INGESTA — ADF Simulado      │
├─────────────────────────────┤
│ 02_adf_ingesta.py           │
│ Kafka topic (log eventos)   │
│ → data_lake/raw/<fecha>/    │
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│ DATA LAKE                   │
├─────────────────────────────┤
│ raw/      → archivos origen │
│ processed → Parquet         │
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│ PROCESAMIENTO ELT — Spark   │
├─────────────────────────────┤
│ 03_spark_elt_dwh.py         │
│ Limpieza, transformación    │
│ y carga incremental al DWH  │
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│ DATA WAREHOUSE              │
├─────────────────────────────┤
│ SQLite — Modelo Estrella    │
│ Dim_Cliente, Dim_Producto   │
│ Dim_Tiempo, Dim_Canal       │
│ Fact_Ventas                 │
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│ VISUALIZACIÓN               │
├─────────────────────────────┤
│ Flask Dashboard             │
│ KPIs y Estadísticas         │
└─────────────────────────────┘
```

## Estructura del proyecto

```text
bigdata-sim-pipeline/
├── fuentes/
│   ├── dia_1/          → archivos fuente día 1
│   ├── dia_2/          → archivos fuente día 2
│   └── dia_3/          → archivos fuente día 3
├── data_lake/
│   ├── raw/            → datos crudos por fecha de ingesta
│   └── processed/      → datos transformados en Parquet
├── warehouse/
│   └── retailstart.db  → Data Warehouse SQLite
├── kafka/
│   └── topics/
│       └── topic_ingesta.json  → log de eventos de ingesta
├── scripts/
│   ├── 01_crear_dwh.py         → crea tablas del DWH
│   ├── 02_adf_ingesta.py       → ingesta incremental por día
│   └── 03_spark_elt_dwh.py     → ELT Spark + carga al DWH
└── dashboard/
    └── app.py                  → visualización Flask
```

## Ejecución del pipeline

### 1. Crear el Data Warehouse
```bash
python3 scripts/01_crear_dwh.py
```

### 2. Ingestar un día de datos
```bash
python3 scripts/02_adf_ingesta.py
# → seleccionar: dia_1, dia_2 o dia_3
```

### 3. Procesar y cargar al DWH
```bash
python3 scripts/03_spark_elt_dwh.py
# → seleccionar la fecha correspondiente al día ingresado
```

### 4. Verificar datos en el DWH
```bash
sqlite3 warehouse/retailstart.db
```
```sql
.headers on
.mode column
SELECT * FROM Fact_Ventas;
```

Para ver los datos con nombres legibles:
```sql
SELECT
    f.id_fact,
    c.nombre || ' ' || c.apellido AS cliente,
    p.nombre_producto,
    t.fecha,
    ca.canal,
    f.cantidad,
    f.precio_unitario,
    f.monto_total
FROM Fact_Ventas f
LEFT JOIN Dim_Cliente  c  ON f.sk_cliente  = c.sk_cliente
LEFT JOIN Dim_Producto p  ON f.sk_producto = p.sk_producto
LEFT JOIN Dim_Tiempo   t  ON f.sk_tiempo   = t.sk_tiempo
LEFT JOIN Dim_Canal    ca ON f.sk_canal    = ca.sk_canal;
```

### 5. Repetir pasos 2 y 3 para cada día
Cada ejecución agrega datos nuevos al DWH sin duplicar los anteriores.

### 6. Dashboard
```bash
cd dashboard
python3 app.py
```
Abrir en el navegador: `http://localhost:5000`

## Limpiar el pipeline

```bash
# Limpiar RAW, processed y Kafka
rm -rf data_lake/raw/* data_lake/processed/* && echo "[]" > kafka/topics/topic_ingesta.json

# Recrear la BD desde cero
rm warehouse/retailstart.db
python3 scripts/01_crear_dwh.py
```

## Ver historial de ingestas (Kafka)

```bash
python3 -c "
import json
from pathlib import Path
topic = Path('kafka/topics/topic_ingesta.json')
eventos = json.loads(topic.read_text())
resumen = [(e['timestamp'], e.get('dia_fuente','sin metadata'), e['ruta_raw'].split('raw/')[1].split('/')[1])
           for e in eventos if e['fuente'] == 'ventas_pos']
print(f\"{'TIMESTAMP':<30} {'DIA FUENTE':<12} {'FECHA CARGA'}\")
print('-'*60)
for ts, dia, fecha in resumen:
    print(f\"{ts:<30} {dia:<12} {fecha}\")
"
```