import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "warehouse" / "retailstart.db"

DB_PATH.parent.mkdir(parents=True, exist_ok=True)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS Dim_Cliente (
    sk_cliente INTEGER PRIMARY KEY AUTOINCREMENT,
    id_cliente INTEGER UNIQUE,
    nombre TEXT,
    apellido TEXT,
    email TEXT,
    segmento TEXT,
    ciudad TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Dim_Producto (
    sk_producto INTEGER PRIMARY KEY AUTOINCREMENT,
    id_producto INTEGER UNIQUE,
    nombre_producto TEXT,
    categoria TEXT,
    precio_base REAL,
    proveedor TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Dim_Tiempo (
    sk_tiempo INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha DATE UNIQUE,
    dia INTEGER,
    mes INTEGER,
    anio INTEGER
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Dim_Canal (
    sk_canal INTEGER PRIMARY KEY AUTOINCREMENT,
    canal TEXT UNIQUE
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Fact_Ventas (
    id_fact INTEGER PRIMARY KEY AUTOINCREMENT,

    sk_cliente  INTEGER,
    sk_producto INTEGER,
    sk_tiempo   INTEGER,
    sk_canal    INTEGER,

    cantidad        INTEGER,
    precio_unitario REAL,
    monto_total     REAL,

    -- Evita duplicados: una venta es única por la combinación de
    -- cliente, producto, tiempo, canal, cantidad y precio
    UNIQUE (
        sk_cliente,
        sk_producto,
        sk_tiempo,
        sk_canal,
        cantidad,
        precio_unitario
    ),

    FOREIGN KEY(sk_cliente)  REFERENCES Dim_Cliente(sk_cliente),
    FOREIGN KEY(sk_producto) REFERENCES Dim_Producto(sk_producto),
    FOREIGN KEY(sk_tiempo)   REFERENCES Dim_Tiempo(sk_tiempo),
    FOREIGN KEY(sk_canal)    REFERENCES Dim_Canal(sk_canal)
)
""")

cursor.execute("""
INSERT OR IGNORE INTO Dim_Canal(canal)
VALUES ('POS'), ('WEB'), ('APP')
""")

conn.commit()
conn.close()

print("Data Warehouse creado correctamente.")