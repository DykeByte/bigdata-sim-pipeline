# =============================================================================
# SCRIPT: 01_crear_dwh.py
# DESCRIPCIÓN: Crea el Data Warehouse con el modelo estrella para EV4
#              RetailStart Chile S.A.
# TABLAS:
#   DimCliente, DimProducto, DimTiempo, DimCanal, DimTienda, FactVentas
# =============================================================================

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "warehouse" / "retailstart.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

conn   = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Activar integridad referencial en SQLite
cursor.execute("PRAGMA foreign_keys = ON")

# =============================================================================
# DimCliente
# =============================================================================
cursor.execute("""
CREATE TABLE IF NOT EXISTS DimCliente (
    cliente_key  INTEGER PRIMARY KEY,
    id_cliente   INTEGER UNIQUE,
    nombre       TEXT,
    apellido     TEXT,
    email        TEXT,
    segmento     TEXT,
    ciudad       TEXT
)
""")

# =============================================================================
# DimProducto
# =============================================================================
cursor.execute("""
CREATE TABLE IF NOT EXISTS DimProducto (
    producto_key   INTEGER PRIMARY KEY,
    id_producto    INTEGER UNIQUE,
    nombre_producto TEXT,
    categoria      TEXT,
    proveedor      TEXT,
    precio_base    REAL
)
""")

# =============================================================================
# DimTiempo
# =============================================================================
cursor.execute("""
CREATE TABLE IF NOT EXISTS DimTiempo (
    fecha_key   INTEGER PRIMARY KEY,
    fecha       DATE UNIQUE,
    dia         INTEGER,
    mes         INTEGER,
    nombre_mes  TEXT,
    trimestre   INTEGER,
    anio        INTEGER,
    dia_semana  TEXT,
    fin_semana  INTEGER
)
""")

# =============================================================================
# DimCanal
# =============================================================================
cursor.execute("""
CREATE TABLE IF NOT EXISTS DimCanal (
    canal_key  INTEGER PRIMARY KEY,
    canal      TEXT UNIQUE,
    tipo_canal TEXT
)
""")

# =============================================================================
# DimTienda
# =============================================================================
cursor.execute("""
CREATE TABLE IF NOT EXISTS DimTienda (
    tienda_key   INTEGER PRIMARY KEY,
    nombre_tienda TEXT,
    region       TEXT,
    tipo_tienda  TEXT
)
""")

# =============================================================================
# FactVentas
# =============================================================================
cursor.execute("""
CREATE TABLE IF NOT EXISTS FactVentas (
    id_fact_venta   INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha_key       INTEGER,
    cliente_key     INTEGER,
    producto_key    INTEGER,
    canal_key       INTEGER,
    tienda_key      INTEGER,
    cantidad        INTEGER,
    precio_unitario REAL,
    total_venta     REAL,
    tipo_venta      TEXT,

    FOREIGN KEY(fecha_key)    REFERENCES DimTiempo(fecha_key),
    FOREIGN KEY(cliente_key)  REFERENCES DimCliente(cliente_key),
    FOREIGN KEY(producto_key) REFERENCES DimProducto(producto_key),
    FOREIGN KEY(canal_key)    REFERENCES DimCanal(canal_key),
    FOREIGN KEY(tienda_key)   REFERENCES DimTienda(tienda_key)
)
""")

# =============================================================================
# Datos iniciales — DimCanal
# =============================================================================
canales = [
    (1, "Web",           "Digital"),
    (2, "App",           "Digital"),
    (3, "Tienda Física", "Presencial"),
]
cursor.executemany("""
    INSERT OR IGNORE INTO DimCanal (canal_key, canal, tipo_canal)
    VALUES (?, ?, ?)
""", canales)

conn.commit()
conn.close()

print("✅ Data Warehouse EV4 creado correctamente.")
print("   Tablas: DimCliente, DimProducto, DimTiempo, DimCanal, DimTienda, FactVentas")