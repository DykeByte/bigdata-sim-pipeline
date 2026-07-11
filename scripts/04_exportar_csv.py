# =============================================================================
# SCRIPT: 04_exportar_csv.py
# DESCRIPCIÓN: Exporta todas las tablas del DWH a archivos .csv para Power BI
# =============================================================================

import sqlite3
import pandas as pd
from pathlib import Path

# Configuración de rutas
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "warehouse" / "retailstart.db"
OUTPUT_DIR = BASE_DIR / "data_output"

# Crear carpeta de salida si no existe
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Conectar al DWH
conn = sqlite3.connect(DB_PATH)

tablas = ["DimCliente", "DimProducto", "DimTiempo", "DimCanal", "DimTienda", "FactVentas"]

print("=== EXPORTANDO TABLAS A CSV ==?\n")

for tabla in tablas:
    query = f"SELECT * FROM {tabla}"
    # Leer tabla con Pandas
    df = pd.read_sql_query(query, conn)
    
    # Ruta de destino
    csv_path = OUTPUT_DIR / f"{tabla}.csv"
    
    # Exportar (usamos sep=';' e index=False para que quede óptimo en Power BI)
    df.to_csv(csv_path, sep=';', index=False, encoding='utf-8-sig')
    print(f"✓ {tabla:<15} exportada exitosamente en: {csv_path.name}")

conn.close()
print("\n✅ Proceso terminado. Todos los archivos están listos en 'data_output/'.")