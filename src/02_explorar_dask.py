from pathlib import Path
import time
import dask.dataframe as dd


# ============================================================
# CONFIGURACIÓN
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "synthea23m"
    / "csv"
)

PERSON_FILE = DATA_DIR / "person.csv"

VISIT_FILES = [
    DATA_DIR / "visit_occurrence_0.csv",
    DATA_DIR / "visit_occurrence_1.csv",
]

# visit_occurrence_1.csv no tiene encabezado.
VISIT_COLUMNS = [
    "visit_occurrence_id",
    "person_id",
    "visit_concept_id",
    "visit_start_date",
    "visit_start_datetime",
    "visit_end_date",
    "visit_end_datetime",
    "visit_type_concept_id",
    "provider_id",
    "care_site_id",
    "visit_source_value",
    "visit_source_concept_id",
    "admitting_source_concept_id",
    "admitting_source_value",
    "discharge_to_concept_id",
    "discharge_to_source_value",
    "preceding_visit_occurrence_id",
]


def detectar_encoding(path):
    """UTF-16 si el archivo empieza con BOM UTF-16; si no, UTF-8."""
    with open(path, "rb") as file:
        bom = file.read(2)
    return "utf-16" if bom in (b"\xff\xfe", b"\xfe\xff") else "utf-8"


def opciones_csv(path):
    """UTF-8 se lee en bloques de 64 MB; UTF-16 no puede dividirse."""
    encoding = detectar_encoding(path)
    return {
        "encoding": encoding,
        "blocksize": "64MB" if encoding == "utf-8" else None,
    }


# ============================================================
# INICIO
# ============================================================

print("=" * 70)
print("EXPLORACIÓN DEL DATASET SYNTHEA CON DASK")
print("=" * 70)

print(f"\nDirectorio de datos:")
print(DATA_DIR)


# ============================================================
# PERSON
# ============================================================

print("\n" + "=" * 70)
print("1. TABLA PERSON")
print("=" * 70)

start = time.perf_counter()

person = dd.read_csv(
    PERSON_FILE,
    assume_missing=True,
    **opciones_csv(PERSON_FILE)
)

print(f"\nColumnas: {len(person.columns)}")
print("\nColumnas disponibles:")
for column in person.columns:
    print(f"  - {column}")

print(f"\nParticiones Dask: {person.npartitions}")

print("\nTipos de datos:")
print(person.dtypes)

person_rows = person.shape[0].compute()

elapsed = time.perf_counter() - start

print(f"\nNúmero de filas: {person_rows:,}")
print(f"Tiempo de lectura/conteo: {elapsed:.2f} segundos")


# ============================================================
# VISIT OCCURRENCE
# ============================================================

print("\n" + "=" * 70)
print("2. TABLA VISIT_OCCURRENCE")
print("=" * 70)

start = time.perf_counter()

visits = dd.concat([
    dd.read_csv(
        VISIT_FILES[0],
        assume_missing=True,
        **opciones_csv(VISIT_FILES[0])
    ),
    dd.read_csv(
        VISIT_FILES[1],
        header=None,
        names=VISIT_COLUMNS,
        assume_missing=True,
        **opciones_csv(VISIT_FILES[1])
    ),
])

print(f"\nColumnas: {len(visits.columns)}")

print("\nColumnas disponibles:")
for column in visits.columns:
    print(f"  - {column}")

print(f"\nParticiones Dask: {visits.npartitions}")

print("\nTipos de datos:")
print(visits.dtypes)

visit_rows = visits.shape[0].compute()

elapsed = time.perf_counter() - start

print(f"\nNúmero de filas: {visit_rows:,}")
print(f"Tiempo de lectura/conteo: {elapsed:.2f} segundos")


# ============================================================
# MUESTRA
# ============================================================

print("\n" + "=" * 70)
print("3. MUESTRA DE VISITAS")
print("=" * 70)

sample = visits.head(5)

print("\n")
print(sample.to_string())


# ============================================================
# RESUMEN
# ============================================================

print("\n" + "=" * 70)
print("RESUMEN")
print("=" * 70)

print(f"\nPersonas: {person_rows:,}")
print(f"Visitas:  {visit_rows:,}")

print(f"\nParticiones Dask de PERSON: {person.npartitions}")
print(f"Particiones Dask de VISIT_OCCURRENCE: {visits.npartitions}")

print("\nExploración completada.")