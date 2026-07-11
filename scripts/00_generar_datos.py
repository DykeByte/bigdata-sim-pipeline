# =============================================================================
# SCRIPT: 00_generar_datos.py
# DESCRIPCIÓN: Genera datos simulados directamente en el DWH
#              RetailStart Chile S.A.
# VOLUMEN:
#   DimTiempo   → 10 días
#   DimCliente  → 20 clientes
#   DimProducto → 15 productos
#   DimCanal    → 3 canales (ya cargados en 01_crear_dwh.py)
#   DimTienda   → 7 tiendas
#   FactVentas  → 100+ registros
# =============================================================================

import sqlite3
import random
from pathlib import Path
from datetime import date, timedelta

DB_PATH = Path(__file__).resolve().parent.parent / "warehouse" / "retailstart.db"

conn   = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
cursor.execute("PRAGMA foreign_keys = ON")

print("=== GENERANDO DATOS SIMULADOS ===\n")

# =============================================================================
# DimTiempo — 10 días consecutivos desde 2026-04-01
# =============================================================================

MESES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}

DIAS_SEMANA = {
    0: "Lunes", 1: "Martes", 2: "Miércoles", 3: "Jueves",
    4: "Viernes", 5: "Sábado", 6: "Domingo"
}

fecha_inicio = date(2026, 4, 1)
tiempos = []

for i in range(10):
    f        = fecha_inicio + timedelta(days=i)
    trimestre = (f.month - 1) // 3 + 1
    fin_semana = 1 if f.weekday() >= 5 else 0
    tiempos.append((
        int(f.strftime("%Y%m%d")),  # fecha_key: 20260401
        f.isoformat(),
        f.day,
        f.month,
        MESES[f.month],
        trimestre,
        f.year,
        DIAS_SEMANA[f.weekday()],
        fin_semana
    ))

cursor.executemany("""
    INSERT OR IGNORE INTO DimTiempo
    (fecha_key, fecha, dia, mes, nombre_mes, trimestre, anio, dia_semana, fin_semana)
    VALUES (?,?,?,?,?,?,?,?,?)
""", tiempos)

print(f"✓ DimTiempo     → {len(tiempos)} registros")

# =============================================================================
# DimCliente — 20 clientes
# =============================================================================

clientes = [
    (101, "Juan",     "Pérez",     "juan.perez@email.com",     "Premium",  "Santiago"),
    (102, "Ana",      "Gómez",     "ana.gomez@email.com",      "Regular",  "Valparaíso"),
    (103, "Carlos",   "Rojas",     "carlos.rojas@email.com",   "Premium",  "Concepción"),
    (104, "María",    "López",     "maria.lopez@email.com",    "Nuevo",    "Santiago"),
    (105, "Pedro",    "Díaz",      "pedro.diaz@email.com",     "Regular",  "Temuco"),
    (106, "Laura",    "Martínez",  "laura.martinez@email.com", "Premium",  "Santiago"),
    (107, "Diego",    "Soto",      "diego.soto@email.com",     "Nuevo",    "Antofagasta"),
    (108, "Sofía",    "Reyes",     "sofia.reyes@email.com",    "Regular",  "Santiago"),
    (109, "Andrés",   "Castro",    "andres.castro@email.com",  "Premium",  "La Serena"),
    (110, "Camila",   "Vega",      "camila.vega@email.com",    "Nuevo",    "Santiago"),
    (111, "Matías",   "Herrera",   "matias.herrera@email.com", "Nuevo",    "Santiago"),
    (112, "Camila",   "Navarro",   "camila.navarro@email.com", "Premium",  "Concepción"),
    (113, "Felipe",   "Rojas",     "felipe.rojas@email.com",   "Regular",  "Santiago"),
    (114, "Isidora",  "Vega",      "isidora.vega@email.com",   "Nuevo",    "Temuco"),
    (115, "Sebastián","Morales",   "sebastian.m@email.com",    "Premium",  "Viña del Mar"),
    (116, "Valentina","Fuentes",   "valentina.f@email.com",    "Regular",  "Santiago"),
    (117, "Ignacio",  "Muñoz",     "ignacio.munoz@email.com",  "Nuevo",    "Rancagua"),
    (118, "Catalina", "Flores",    "catalina.f@email.com",     "Premium",  "Santiago"),
    (119, "Rodrigo",  "Salinas",   "rodrigo.s@email.com",      "Regular",  "Iquique"),
    (120, "Fernanda", "Torres",    "fernanda.t@email.com",     "Nuevo",    "Santiago"),
]

# Unificamos clave natural y subrogada (cliente_key = id_cliente = c[0])
clientes_mapeados = [(c[0], c[0], c[1], c[2], c[3], c[4], c[5]) for c in clientes]

cursor.executemany("""
    INSERT OR IGNORE INTO DimCliente
    (cliente_key, id_cliente, nombre, apellido, email, segmento, ciudad)
    VALUES (?,?,?,?,?,?,?)
""", clientes_mapeados)

print(f"✓ DimCliente    → {len(clientes)} registros")

# =============================================================================
# DimProducto — 15 productos
# =============================================================================

productos = [
    (2001, "Notebook Lenovo",    "Tecnología", "Lenovo",   140000),
    (2002, "Smartphone Samsung", "Tecnología", "Samsung",  280000),
    (2003, "Polera Hombre",      "Vestuario",  "Nike",      30000),
    (2004, "Silla Oficina",      "Hogar",      "Ikea",      15000),
    (2005, "Audífonos Sony",     "Tecnología", "Sony",      70000),
    (2006, "Tablet Huawei",      "Tecnología", "Huawei",   100000),
    (2007, "Zapatos Mujer",      "Vestuario",  "Adidas",    50000),
    (2008, "Microondas",         "Hogar",      "LG",        80000),
    (2009, "Monitor Dell",       "Tecnología", "Dell",     120000),
    (2010, "Mochila",            "Vestuario",  "Puma",      25000),
    (2011, "Chaqueta Hombre",    "Vestuario",  "Nike",      60000),
    (2012, "Licuadora",          "Hogar",      "Oster",     35000),
    (2013, "Teclado Mecánico",   "Tecnología", "Logitech",  55000),
    (2014, "Lámpara LED",        "Hogar",      "Philips",   20000),
    (2015, "Zapatillas Running", "Vestuario",  "New Balance",45000),
]

# Mapeo consistente: producto_key e id_producto comparten p[0]
cursor.executemany("""
    INSERT OR IGNORE INTO DimProducto
    (id_producto, nombre_producto, categoria, proveedor, precio_base, producto_key)
    VALUES (?,?,?,?,?,?)
""", [(p[0], p[1], p[2], p[3], p[4], p[0]) for p in productos])

print(f"✓ DimProducto   → {len(productos)} registros")

# =============================================================================
# DimTienda — 7 tiendas
# =============================================================================

tiendas = [
    (1, "Tienda Santiago Centro", "Metropolitana", "Flagship"),
    (2, "Tienda Providencia",     "Metropolitana", "Estándar"),
    (3, "Tienda Maipú",           "Metropolitana", "Estándar"),
    (4, "Tienda Valparaíso",      "Valparaíso",    "Estándar"),
    (5, "Tienda Concepción",      "Biobío",        "Estándar"),
    (6, "Tienda Antofagasta",     "Antofagasta",   "Express"),
    (7, "Tienda Temuco",          "Araucanía",     "Express"),
]

cursor.executemany("""
    INSERT OR IGNORE INTO DimTienda
    (tienda_key, nombre_tienda, region, tipo_tienda)
    VALUES (?,?,?,?)
""", tiendas)

print(f"✓ DimTienda     → {len(tiendas)} registros")

# =============================================================================
# FactVentas — 100+ registros
# =============================================================================

random.seed(42)

fechas_keys   = [t[0] for t in tiempos]
cliente_keys  = [c[0] for c in clientes]
producto_keys = [p[0] for p in productos]
canal_keys    = [1, 2, 3]  # 1: Web, 2: App, 3: Tienda Física (Cargados en script 01)
tienda_keys   = [t[0] for t in tiendas]

precios = {p[0]: p[4] for p in productos}

ventas = []

for _ in range(120):
    canal_key   = random.choice(canal_keys)
    cliente_key = random.choice(cliente_keys)
    fecha_key   = random.choice(fechas_keys)
    
    # Cada venta DEBE tener un producto válido para no violar la Foreign Key en el DWH
    producto_key = random.choice(producto_keys)

    if canal_key in (1, 2):
        tipo_venta   = "Online"
        tienda_key   = None  # Las ventas online no pertenecen a una tienda física
    else:
        tipo_venta   = "Presencial"
        tienda_key   = random.choice(tienda_keys)

    cantidad        = random.randint(1, 4)
    precio_unitario = precios[producto_key]
    total_venta     = cantidad * precio_unitario

    ventas.append((
        fecha_key,
        cliente_key,
        producto_key,
        canal_key,
        tienda_key,
        cantidad,
        precio_unitario,
        total_venta,
        tipo_venta
    ))

cursor.executemany("""
    INSERT INTO FactVentas
    (fecha_key, cliente_key, producto_key, canal_key, tienda_key,
     cantidad, precio_unitario, total_venta, tipo_venta)
    VALUES (?,?,?,?,?,?,?,?,?)
""", ventas)

print(f"✓ FactVentas    → {len(ventas)} registros")

conn.commit()

# =============================================================================
# RESUMEN
# =============================================================================

print("\n=== RESUMEN FINAL ===")
for tabla in ["DimTiempo", "DimCliente", "DimProducto", "DimCanal", "DimTienda", "FactVentas"]:
    n = cursor.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
    print(f"  {tabla:<15} → {n:>4} registros")

conn.close()
print("\n✅ Datos simulados cargados correctamente.")