# =============================================================================
# SCRIPT: 03_spark_elt_dwh.py
# DESCRIPCIÓN: Pipeline completo Lambda - Capa Batch (Modo Incremental)
#              FASE 1 → Lee archivos RAW de la fecha indicada,
#                        limpia y transforma a Parquet
#              FASE 2 → Carga los Parquet al Data Warehouse (SQLite)
# PROYECTO: RetailStart Chile S.A.
#
# USO: python3 03_spark_elt_dwh.py
#      → Mostrará las fechas disponibles con su día fuente y pedirá selección
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pathlib import Path
import sqlite3
import pandas as pd
import json

# -----------------------------------------------------------------------------
# RUTAS BASE
# -----------------------------------------------------------------------------

BASE_DIR      = Path(__file__).resolve().parent.parent
RAW_DIR       = BASE_DIR / "data_lake" / "raw"
PROCESSED_DIR = BASE_DIR / "data_lake" / "processed"
DB_PATH       = BASE_DIR / "warehouse" / "retailstart.db"
META_DIR      = RAW_DIR / "meta"

# -----------------------------------------------------------------------------
# LEER METADATA — relaciona fecha_carga con dia_fuente
# -----------------------------------------------------------------------------

def leer_metadata() -> dict:
    """Retorna dict {fecha_carga: dia_fuente} leyendo los metadata.json"""
    meta = {}
    if META_DIR.exists():
        for carpeta in sorted(META_DIR.iterdir()):
            archivo = carpeta / "metadata.json"
            if archivo.exists():
                with open(archivo, "r", encoding="utf-8") as f:
                    datos = json.load(f)
                    meta[datos["fecha_carga"]] = datos["dia_fuente"]
    return meta

# -----------------------------------------------------------------------------
# SELECCIÓN DE FECHA
# -----------------------------------------------------------------------------

metadata = leer_metadata()

# Fechas disponibles en RAW (basado en ventas_pos como referencia)
fechas_disponibles = sorted([
    f.name for f in (RAW_DIR / "ventas_pos").iterdir()
    if f.is_dir()
])

if not fechas_disponibles:
    print("\n  ❌ No hay fechas disponibles en el RAW. Ejecute primero 02_adf_ingesta.py")
    exit(1)

print("\nFechas disponibles en RAW:")
for f in fechas_disponibles:
    dia_fuente = metadata.get(f, "sin metadata")
    print(f"  - {f}  →  {dia_fuente}")

FECHA_CARGA = input("\nIngrese la fecha a procesar (ej: 2026-06-29): ").strip()

if FECHA_CARGA not in fechas_disponibles:
    print(f"\n  ❌ Fecha '{FECHA_CARGA}' no encontrada en el RAW. Abortando.")
    exit(1)

dia_fuente = metadata.get(FECHA_CARGA, "sin metadata")
print(f"\n  ✓ Procesando fecha: {FECHA_CARGA}  ({dia_fuente})")

# -----------------------------------------------------------------------------
# HELPER: construye ruta RAW para la fecha seleccionada
# -----------------------------------------------------------------------------

def ruta_raw(subcarpeta: str) -> str:
    ruta = RAW_DIR / subcarpeta / FECHA_CARGA
    if not ruta.exists():
        print(f"  ⚠️  No existe {ruta}. Omitiendo.")
        return None
    return str(ruta)

# -----------------------------------------------------------------------------
# INICIAR SPARK
# -----------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("RetailStart-ELT-DWH")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# =============================================================================
# FASE 1: ELT — RAW → PROCESSED (Parquet)
# =============================================================================

print("\n" + "="*60)
print("  FASE 1: ELT — Transformación RAW → PROCESSED")
print(f"  Fecha: {FECHA_CARGA}  ({dia_fuente})")
print("="*60)


# -----------------------------------------------------------------------------
# 1.1 CLIENTES
# -----------------------------------------------------------------------------

print("\n[1/4] Procesando Clientes...")

clientes = (
    spark.read
    .option("header", "true")
    .csv(ruta_raw("clientes_crm"))
)

clientes = clientes.dropDuplicates(["id_cliente"])
clientes.write.mode("overwrite").parquet(str(PROCESSED_DIR / "clientes"))

print(f"  ✓ Clientes → {clientes.count()} registros guardados en Parquet")


# -----------------------------------------------------------------------------
# 1.2 PRODUCTOS
# -----------------------------------------------------------------------------

print("\n[2/4] Procesando Productos...")

productos = (
    spark.read
    .option("header", "true")
    .csv(ruta_raw("productos_erp"))
)

productos = productos.dropDuplicates(["id_producto"])
productos.write.mode("overwrite").parquet(str(PROCESSED_DIR / "productos"))

print(f"  ✓ Productos → {productos.count()} registros guardados en Parquet")


# -----------------------------------------------------------------------------
# 1.3 VENTAS POS + ONLINE
# -----------------------------------------------------------------------------

print("\n[3/4] Procesando Ventas...")

ventas_pos = (
    spark.read
    .option("header", "true")
    .csv(ruta_raw("ventas_pos"))
)

ventas_pos = (
    ventas_pos
    .withColumn("cantidad",        col("cantidad").cast("int"))
    .withColumn("precio_unitario", col("precio_unitario").cast("double"))
    .withColumn("monto_total",     col("cantidad") * col("precio_unitario"))
    .withColumn("canal",           lit("POS"))
)

ventas_online = (
    spark.read
    .option("header", "true")
    .csv(ruta_raw("ventas_online"))
)

ventas_online = (
    ventas_online
    .withColumn("cantidad",        lit(1))
    .withColumn("precio_unitario", col("total").cast("double"))
    .withColumn("monto_total",     col("total").cast("double"))
    .withColumn("id_producto",     lit(None).cast("string"))
)

cols = ["fecha", "id_cliente", "id_producto",
        "cantidad", "precio_unitario", "monto_total", "canal"]

ventas_pos    = ventas_pos.select(cols)
ventas_online = ventas_online.select(cols)

ventas = ventas_pos.unionByName(ventas_online).dropDuplicates()
ventas.write.mode("overwrite").parquet(str(PROCESSED_DIR / "ventas"))

print(f"  ✓ Ventas POS     → {ventas_pos.count()} registros")
print(f"  ✓ Ventas Online  → {ventas_online.count()} registros")
print(f"  ✓ Ventas unidas  → {ventas.count()} registros guardados en Parquet")


# -----------------------------------------------------------------------------
# 1.4 EVENTOS APP
# -----------------------------------------------------------------------------

print("\n[4/4] Procesando Eventos App...")

eventos = (
    spark.read
    .option("multiline", "true")
    .json(ruta_raw("eventos_app"))
)

eventos.write.mode("overwrite").parquet(str(PROCESSED_DIR / "eventos"))

print(f"  ✓ Eventos → {eventos.count()} registros guardados en Parquet")

print("\n  ✅ FASE 1 COMPLETADA — Archivos transformados a Parquet")


# =============================================================================
# FASE 2: CARGA DWH — PROCESSED → SQLite
# =============================================================================

print("\n" + "="*60)
print("  FASE 2: Carga Data Warehouse (SQLite)")
print("="*60)

clientes_pd  = spark.read.parquet(str(PROCESSED_DIR / "clientes")).toPandas()
productos_pd = spark.read.parquet(str(PROCESSED_DIR / "productos")).toPandas()
ventas_pd    = spark.read.parquet(str(PROCESSED_DIR / "ventas")).toPandas()

conn   = sqlite3.connect(DB_PATH)
cursor = conn.cursor()


# -----------------------------------------------------------------------------
# 2.1 DIM_CLIENTE
# -----------------------------------------------------------------------------

print("\n[1/5] Cargando Dim_Cliente...")

for _, row in clientes_pd.iterrows():
    cursor.execute("""
        INSERT OR IGNORE INTO Dim_Cliente
        (id_cliente, nombre, apellido, email, segmento, ciudad)
        VALUES (?,?,?,?,?,?)
    """, (
        int(row["id_cliente"]),
        row["nombre"], row["apellido"],
        row["email"], row["segmento"], row["ciudad"]
    ))

print("  ✓ Dim_Cliente cargada")


# -----------------------------------------------------------------------------
# 2.2 DIM_PRODUCTO
# -----------------------------------------------------------------------------

print("\n[2/5] Cargando Dim_Producto...")

for _, row in productos_pd.iterrows():
    cursor.execute("""
        INSERT OR IGNORE INTO Dim_Producto
        (id_producto, nombre_producto, categoria, precio_base, proveedor)
        VALUES (?,?,?,?,?)
    """, (
        int(row["id_producto"]),
        row["nombre_producto"], row["categoria"],
        float(row["precio_base"]), row["proveedor"]
    ))

print("  ✓ Dim_Producto cargada")


# -----------------------------------------------------------------------------
# 2.3 DIM_TIEMPO
# -----------------------------------------------------------------------------

print("\n[3/5] Cargando Dim_Tiempo...")

fechas = ventas_pd["fecha"].drop_duplicates()

for fecha in fechas:
    dt = pd.to_datetime(fecha)
    cursor.execute("""
        INSERT OR IGNORE INTO Dim_Tiempo (fecha, dia, mes, anio)
        VALUES (?,?,?,?)
    """, (fecha, dt.day, dt.month, dt.year))

print(f"  ✓ Dim_Tiempo cargada ({len(fechas)} fechas únicas)")

conn.commit()


# -----------------------------------------------------------------------------
# 2.4 MAPEO DE SURROGATE KEYS (SK)
# -----------------------------------------------------------------------------

print("\n[4/5] Construyendo mapeo de claves (SK)...")

clientes_sk  = pd.read_sql("SELECT id_cliente, sk_cliente FROM Dim_Cliente", conn)
productos_sk = pd.read_sql("SELECT id_producto, sk_producto FROM Dim_Producto", conn)
tiempo_sk    = pd.read_sql("SELECT fecha, sk_tiempo FROM Dim_Tiempo", conn)
canal_sk     = pd.read_sql("SELECT canal, sk_canal FROM Dim_Canal", conn)

map_cliente  = dict(zip(clientes_sk.id_cliente,  clientes_sk.sk_cliente))
map_producto = dict(zip(productos_sk.id_producto, productos_sk.sk_producto))
map_tiempo   = dict(zip(tiempo_sk.fecha,          tiempo_sk.sk_tiempo))
map_canal    = dict(zip(canal_sk.canal,           canal_sk.sk_canal))

print("  ✓ Mapas SK construidos")


# -----------------------------------------------------------------------------
# 2.5 FACT_VENTAS
# -----------------------------------------------------------------------------

print("\n[5/5] Cargando Fact_Ventas...")

for _, row in ventas_pd.iterrows():

    canal       = str(row["canal"]).upper()
    sk_cliente  = map_cliente.get(int(row["id_cliente"]))
    sk_tiempo   = map_tiempo.get(row["fecha"])
    sk_canal    = map_canal.get(canal)
    sk_producto = None

    if pd.notna(row["id_producto"]):
        sk_producto = map_producto.get(int(row["id_producto"]))

    cursor.execute("""
        INSERT OR IGNORE INTO Fact_Ventas
        (sk_cliente, sk_producto, sk_tiempo, sk_canal,
         cantidad, precio_unitario, monto_total)
        VALUES (?,?,?,?,?,?,?)
    """, (
        sk_cliente, sk_producto, sk_tiempo, sk_canal,
        int(row["cantidad"]),
        float(row["precio_unitario"]),
        float(row["monto_total"])
    ))

conn.commit()
print("  ✓ Fact_Ventas cargada")


# =============================================================================
# RESUMEN FINAL
# =============================================================================

print("\n" + "="*60)
print("  RESUMEN — Registros en el Data Warehouse")
print("="*60)

tablas = ["Dim_Cliente", "Dim_Producto", "Dim_Tiempo", "Dim_Canal", "Fact_Ventas"]

for tabla in tablas:
    n = cursor.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
    print(f"  {tabla:<20} → {n:>4} registros")

conn.close()
spark.stop()

print("\n  ✅ PIPELINE COMPLETADO — DWH actualizado correctamente\n")