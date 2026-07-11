from pathlib import Path
import shutil
import json
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent

FUENTES_DIR = BASE_DIR / "fuentes"
RAW_DIR     = BASE_DIR / "data_lake" / "raw"
TOPIC_FILE  = BASE_DIR / "kafka" / "topics" / "topic_ingesta.json"


def cargar_topic():
    if not TOPIC_FILE.exists():
        return []
    with open(TOPIC_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except:
            return []


def guardar_topic(eventos):
    with open(TOPIC_FILE, "w", encoding="utf-8") as f:
        json.dump(eventos, f, indent=4, ensure_ascii=False)


def guardar_metadata(fecha_carga, nombre_dia):
    """
    Guarda un archivo metadata.json en data_lake/raw/meta/<fecha_carga>/
    que registra a qué día fuente corresponde esta ingesta.
    """
    meta_dir = RAW_DIR / "meta" / fecha_carga
    meta_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "fecha_carga": fecha_carga,
        "dia_fuente": nombre_dia,
        "timestamp": datetime.now().isoformat()
    }

    with open(meta_dir / "metadata.json", "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4, ensure_ascii=False)


def procesar_dia(nombre_dia):

    origen      = FUENTES_DIR / nombre_dia
    eventos     = cargar_topic()
    fecha_carga = datetime.now().strftime("%Y-%m-%d")

    #%Y-%m-%d  -->fecha x defecto 

    for archivo in origen.iterdir():

        if not archivo.is_file():
            continue

        fuente      = archivo.stem
        destino_dir = RAW_DIR / fuente / fecha_carga
        destino_dir.mkdir(parents=True, exist_ok=True)
        destino     = destino_dir / archivo.name

        shutil.copy2(archivo, destino)

        evento = {
            "timestamp": datetime.now().isoformat(),
            "fuente":    fuente,
            "archivo":   archivo.name,
            "dia_fuente": nombre_dia,
            "ruta_raw":  str(destino)
        }

        eventos.append(evento)
        print(f"[OK] {archivo.name} -> {destino}")

    # Guardar metadata de esta ingesta
    guardar_metadata(fecha_carga, nombre_dia)

    guardar_topic(eventos)
    print(f"\nEventos publicados: {len(eventos)}")


if __name__ == "__main__":

    dias_disponibles = sorted([
        d.name for d in FUENTES_DIR.iterdir()
        if d.is_dir() and d.name.startswith("dia_")
    ])

    if not dias_disponibles:
        print("[ERROR] No se encontraron carpetas de días en fuentes/")
        exit(1)

    print("Días disponibles:", ", ".join(dias_disponibles))
    dia = input("Ingrese el día a procesar: ").strip()

    if dia not in dias_disponibles:
        print(f"[ERROR] '{dia}' no es un día válido. Opciones: {dias_disponibles}")
        exit(1)

    procesar_dia(dia)