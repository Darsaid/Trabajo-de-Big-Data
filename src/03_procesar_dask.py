from pathlib import Path
import time
import shutil

import dask.dataframe as dd
import pandas as pd


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

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "visits_parquet"
)

VISIT_FILES = [
    DATA_DIR / "visit_occurrence_0.csv",
    DATA_DIR / "visit_occurrence_1.csv",
]


# ============================================================
# FUNCIÓN DE PROCESAMIENTO POR PARTICIÓN
# ============================================================

def procesar_particion(df):
    """
    Limpia y transforma una partición individual.
    Esta función se ejecuta de manera distribuida sobre
    las particiones creadas por Dask.
    """

    # Selección de columnas
    columns = [
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
        "visit_source_value"
    ]

    df = df[columns].copy()

    # --------------------------------------------------------
    # Fechas
    # --------------------------------------------------------

    df["visit_start_datetime"] = pd.to_datetime(
        df["visit_start_datetime"],
        errors="coerce"
    )

    df["visit_end_datetime"] = pd.to_datetime(
        df["visit_end_datetime"],
        errors="coerce"
    )

    # --------------------------------------------------------
    # Valores faltantes
    # --------------------------------------------------------

    df = df.dropna(
        subset=[
            "visit_occurrence_id",
            "person_id",
            "visit_start_datetime"
        ]
    )

    df["provider_id"] = df["provider_id"].fillna(-1)

    df["care_site_id"] = df["care_site_id"].fillna(-1)

    df["visit_source_value"] = (
        df["visit_source_value"]
        .fillna("UNKNOWN")
    )

    # --------------------------------------------------------
    # Variables derivadas
    # --------------------------------------------------------

    df["visit_year"] = (
        df["visit_start_datetime"].dt.year
    )

    df["visit_month"] = (
        df["visit_start_datetime"].dt.month
    )

    df["visit_duration_days"] = (
        (
            df["visit_end_datetime"]
            - df["visit_start_datetime"]
        ).dt.total_seconds() / 86400
    )

    # Evitar duraciones negativas
    df["visit_duration_days"] = (
        df["visit_duration_days"]
        .clip(lower=0)
    )

    # --------------------------------------------------------
    # Estructura final
    # --------------------------------------------------------

    final_columns = [
        "visit_occurrence_id",
        "person_id",
        "visit_concept_id",
        "visit_start_datetime",
        "visit_end_datetime",
        "visit_type_concept_id",
        "provider_id",
        "care_site_id",
        "visit_source_value",
        "visit_year",
        "visit_month",
        "visit_duration_days"
    ]

    return df[final_columns]


# ============================================================
# INICIO
# ============================================================

print("=" * 70)
print("PROCESAMIENTO DE VISIT_OCCURRENCE CON DASK")
print("=" * 70)

print(f"\nEntrada:")
print(DATA_DIR)

print(f"\nSalida:")
print(OUTPUT_DIR)

start_total = time.perf_counter()


# ============================================================
# 1. LECTURA
# ============================================================

print("\n" + "=" * 70)
print("1. LECTURA Y PARTICIONAMIENTO")
print("=" * 70)

visit_columns = [
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

visits_with_header = dd.read_csv(
    str(VISIT_FILES[0]),
    assume_missing=True,
    blocksize=None,
    encoding="utf-16"
)

visits_without_header = dd.read_csv(
    str(VISIT_FILES[1]),
    header=None,
    names=visit_columns,
    assume_missing=True,
    blocksize=None,
    encoding="utf-16"
)

visits = dd.concat([visits_with_header, visits_without_header])

print(f"\nParticiones originales: {visits.npartitions}")


# ============================================================
# 2. PROCESAMIENTO POR PARTICIÓN
# ============================================================

print("\n" + "=" * 70)
print("2. LIMPIEZA Y TRANSFORMACIÓN")
print("=" * 70)

# Metadata vacía para que Dask conozca exactamente
# el esquema final sin ejecutar todo el dataset.
meta = pd.DataFrame({
    "visit_occurrence_id": pd.Series(dtype="float64"),
    "person_id": pd.Series(dtype="float64"),
    "visit_concept_id": pd.Series(dtype="float64"),
    "visit_start_datetime": pd.Series(dtype="datetime64[ns]"),
    "visit_end_datetime": pd.Series(dtype="datetime64[ns]"),
    "visit_type_concept_id": pd.Series(dtype="float64"),
    "provider_id": pd.Series(dtype="float64"),
    "care_site_id": pd.Series(dtype="float64"),
    "visit_source_value": pd.Series(dtype="string"),
    "visit_year": pd.Series(dtype="float64"),
    "visit_month": pd.Series(dtype="float64"),
    "visit_duration_days": pd.Series(dtype="float64")
})

visits_processed = visits.map_partitions(
    procesar_particion,
    meta=meta
)

print("Limpieza configurada.")

print("Variables derivadas:")
print("  - visit_year")
print("  - visit_month")
print("  - visit_duration_days")

print(f"\nParticiones resultantes: {visits_processed.npartitions}")


# ============================================================
# 3. GUARDAR PARQUET
# ============================================================

print("\n" + "=" * 70)
print("3. GENERACIÓN DE PARQUET")
print("=" * 70)

if OUTPUT_DIR.exists():
    print("\nEliminando salida anterior...")
    shutil.rmtree(OUTPUT_DIR)

print("\nProcesando particiones y guardando...")

start_write = time.perf_counter()

visits_processed.to_parquet(
    str(OUTPUT_DIR),
    engine="pyarrow",
    write_index=False,
    compression="snappy",
    overwrite=True
)

write_time = time.perf_counter() - start_write


# ============================================================
# 4. RESUMEN
# ============================================================

total_time = time.perf_counter() - start_total

print("\n" + "=" * 70)
print("PROCESAMIENTO COMPLETADO")
print("=" * 70)

print(f"\nParticiones Dask: {visits_processed.npartitions}")

print(f"Tiempo de escritura: {write_time:.2f} segundos")

print(f"Tiempo total: {total_time:.2f} segundos")

print("\nSalida:")
print(OUTPUT_DIR)

print("\nVariables derivadas:")
print("  - visit_year")
print("  - visit_month")
print("  - visit_duration_days")

print("\nPipeline Dask completado correctamente.")