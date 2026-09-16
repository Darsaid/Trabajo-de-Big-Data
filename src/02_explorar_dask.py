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

VISIT_FILES = str(DATA_DIR / "visit_occurrence_*.csv")


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
    blocksize="64MB",
    encoding="utf-16"
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

visits = dd.read_csv(
    VISIT_FILES,
    assume_missing=True,
    blocksize="64MB",
    encoding="utf-16"
)

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