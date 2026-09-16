from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import psutil


# ============================================================
# RUTAS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "synthea23m"
    / "csv"
)

VISITS_OUTPUT = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "visits_parquet"
)

RESULTS_DIR = (
    PROJECT_ROOT
    / "data"
    / "results"
)

VISIT_FILES = [
    DATA_DIR / "visit_occurrence_0.csv",
    DATA_DIR / "visit_occurrence_1.csv",
]


# ============================================================
# COLUMNAS ORIGINALES DE VISIT_OCCURRENCE
# ============================================================

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


# ============================================================
# MEMORIA
# ============================================================

def memory_mb() -> float:
    """
    Memoria RSS actual del proceso Python en MB.
    """
    return psutil.Process().memory_info().rss / (1024**2)


# ============================================================
# PROCESAMIENTO DASK POR PARTICIÓN
# ============================================================

def procesar_particion(df: pd.DataFrame) -> pd.DataFrame:

    selected_columns = [
        "visit_occurrence_id",
        "person_id",
        "visit_concept_id",
        "visit_start_datetime",
        "visit_end_datetime",
        "visit_type_concept_id",
        "provider_id",
        "care_site_id",
        "visit_source_value",
    ]

    df = df[selected_columns].copy()

    # --------------------------------------------------------
    # Conversión de fechas
    # --------------------------------------------------------

    df["visit_start_datetime"] = pd.to_datetime(
        df["visit_start_datetime"],
        errors="coerce",
    )

    df["visit_end_datetime"] = pd.to_datetime(
        df["visit_end_datetime"],
        errors="coerce",
    )

    # --------------------------------------------------------
    # Eliminación de registros esenciales incompletos
    # --------------------------------------------------------

    df = df.dropna(
        subset=[
            "visit_occurrence_id",
            "person_id",
            "visit_start_datetime",
        ]
    )

    # --------------------------------------------------------
    # Tratamiento de valores faltantes
    # --------------------------------------------------------

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
        df["visit_start_datetime"]
        .dt.year
    )

    df["visit_month"] = (
        df["visit_start_datetime"]
        .dt.month
    )

    df["visit_duration_days"] = (
        (
            df["visit_end_datetime"]
            - df["visit_start_datetime"]
        )
        .dt.total_seconds()
        .div(86400)
        .clip(lower=0)
    )

    # --------------------------------------------------------
    # Compatibilidad con Parquet
    # --------------------------------------------------------

    df["visit_start_datetime"] = (
        df["visit_start_datetime"]
        .astype("datetime64[ms]")
    )

    df["visit_end_datetime"] = (
        df["visit_end_datetime"]
        .astype("datetime64[ms]")
    )

    return df[
        [
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
            "visit_duration_days",
        ]
    ]


# ============================================================
# ETAPA DASK
# ============================================================

def ejecutar_dask() -> tuple[float, float]:

    print("\n" + "=" * 70)
    print("ETAPA 1 - DASK")
    print("=" * 70)

    start = time.perf_counter()

    before_mb = memory_mb()

    # --------------------------------------------------------
    # Lectura del primer CSV
    # Tiene encabezado.
    # --------------------------------------------------------

    with_header = dd.read_csv(
        str(VISIT_FILES[0]),
        assume_missing=True,
        blocksize=None,
        encoding="utf-16",
    )

    # --------------------------------------------------------
    # Lectura del segundo CSV
    # No tiene encabezado.
    # --------------------------------------------------------

    without_header = dd.read_csv(
        str(VISIT_FILES[1]),
        header=None,
        names=VISIT_COLUMNS,
        assume_missing=True,
        blocksize=None,
        encoding="utf-16",
    )

    # --------------------------------------------------------
    # Unión de ambos archivos
    # --------------------------------------------------------

    visits = dd.concat(
        [
            with_header,
            without_header,
        ]
    )

    print(
        f"\nParticiones Dask de entrada: "
        f"{visits.npartitions}"
    )

    # --------------------------------------------------------
    # Metadata para map_partitions
    # --------------------------------------------------------

    meta = pd.DataFrame(
        {
            "visit_occurrence_id": pd.Series(
                dtype="float64"
            ),
            "person_id": pd.Series(
                dtype="float64"
            ),
            "visit_concept_id": pd.Series(
                dtype="float64"
            ),
            "visit_start_datetime": pd.Series(
                dtype="datetime64[ms]"
            ),
            "visit_end_datetime": pd.Series(
                dtype="datetime64[ms]"
            ),
            "visit_type_concept_id": pd.Series(
                dtype="float64"
            ),
            "provider_id": pd.Series(
                dtype="float64"
            ),
            "care_site_id": pd.Series(
                dtype="float64"
            ),
            "visit_source_value": pd.Series(
                dtype="string"
            ),
            "visit_year": pd.Series(
                dtype="float64"
            ),
            "visit_month": pd.Series(
                dtype="float64"
            ),
            "visit_duration_days": pd.Series(
                dtype="float64"
            ),
        }
    )

    # --------------------------------------------------------
    # Limpieza y transformación
    # --------------------------------------------------------

    processed = visits.map_partitions(
        procesar_particion,
        meta=meta,
    )

    print("\nTransformaciones Dask configuradas:")
    print("  - Conversión de fechas")
    print("  - Eliminación de registros inválidos")
    print("  - Tratamiento de valores faltantes")
    print("  - visit_year")
    print("  - visit_month")
    print("  - visit_duration_days")

    # --------------------------------------------------------
    # Generación de Parquet
    # --------------------------------------------------------

    if VISITS_OUTPUT.exists():
        shutil.rmtree(VISITS_OUTPUT)

    print("\nGenerando Parquet...")

    processed.to_parquet(
        str(VISITS_OUTPUT),
        engine="pyarrow",
        write_index=False,
        compression="snappy",
        overwrite=True,
    )

    elapsed = (
        time.perf_counter()
        - start
    )

    memory_delta = max(
        0.0,
        memory_mb() - before_mb,
    )

    print(
        f"\nDask completado en "
        f"{elapsed:.2f} segundos"
    )

    print(
        f"Particiones Parquet: "
        f"{processed.npartitions}"
    )

    print(
        f"Delta de memoria: "
        f"{memory_delta:.2f} MB"
    )

    return elapsed, memory_delta


# ============================================================
# COMPARACIÓN EQUIVALENTE CON DASK
# ============================================================

def comparar_dask() -> tuple[dict, float, float]:

    print("\n" + "-" * 70)
    print("COMPARACIÓN DASK")
    print("-" * 70)

    start = time.perf_counter()

    before_mb = memory_mb()

    result = (
        dd.read_parquet(
            str(VISITS_OUTPUT)
        )
        .groupby("visit_year")
        .size()
        .compute()
        .sort_index()
    )

    elapsed = (
        time.perf_counter()
        - start
    )

    memory_delta = max(
        0.0,
        memory_mb() - before_mb,
    )

    values = {
        str(int(year)): int(count)
        for year, count in result.items()
    }

    print(
        f"Dask groupBy visit_year: "
        f"{elapsed:.2f} segundos"
    )

    print(
        f"Delta de memoria: "
        f"{memory_delta:.2f} MB"
    )

    return values, elapsed, memory_delta


# ============================================================
# ETAPA SPARK
# ============================================================

def ejecutar_spark(
    shuffle_partitions: int,
) -> tuple[dict, float, float, float]:

    try:
        from pyspark.sql import (
            SparkSession,
            functions as F,
        )

        from pyspark.sql.window import Window

    except ImportError as error:

        raise RuntimeError(
            "Instala pyspark antes de ejecutar Spark."
        ) from error

    if shutil.which("java") is None:

        raise RuntimeError(
            "Spark requiere Java 8+ en PATH o JAVA_HOME."
        )

    # --------------------------------------------------------
    # Hadoop local para Windows
    # --------------------------------------------------------

    local_hadoop = (
        PROJECT_ROOT
        / "tools"
        / "hadoop"
    )

    if (
        os.name == "nt"
        and (
            local_hadoop
            / "bin"
            / "winutils.exe"
        ).exists()
    ):

        os.environ.setdefault(
            "HADOOP_HOME",
            str(local_hadoop),
        )

        os.environ.setdefault(
            "hadoop.home.dir",
            str(local_hadoop),
        )

        hadoop_bin = str(
            local_hadoop / "bin"
        )

        if (
            hadoop_bin
            not in os.environ["PATH"].split(
                os.pathsep
            )
        ):

            os.environ["PATH"] = (
                hadoop_bin
                + os.pathsep
                + os.environ["PATH"]
            )

    print("\n" + "=" * 70)
    print(
        f"ETAPA 2 - SPARK "
        f"(shuffle.partitions={shuffle_partitions})"
    )
    print("=" * 70)

    start = time.perf_counter()

    before_mb = memory_mb()

    # --------------------------------------------------------
    # SparkSession
    # --------------------------------------------------------

    spark = (
        SparkSession.builder
        .appName(
            "PipelineIntegradoDaskSpark"
        )
        .master("local[*]")
        .config(
            "spark.sql.shuffle.partitions",
            str(shuffle_partitions),
        )
        .config(
            "spark.driver.memory",
            "4g",
        )
        .config(
            "spark.hadoop.io.native.lib.available",
            "false",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    # --------------------------------------------------------
    # Spark lee el Parquet producido por Dask
    # --------------------------------------------------------

    print(
        "\nLeyendo Parquet generado por Dask..."
    )

    visits = (
        spark.read
        .parquet(
            str(VISITS_OUTPUT)
        )
        .withColumn(
            "person_id",
            F.col("person_id").cast(
                "long"
            ),
        )
    )

    print(
        f"Particiones Spark: "
        f"{visits.rdd.getNumPartitions()}"
    )

    # --------------------------------------------------------
    # FILTROS
    # --------------------------------------------------------

    filtered = visits.filter(
        (
            F.col(
                "visit_duration_days"
            )
            >= 0
        )
        & F.col(
            "visit_year"
        ).isNotNull()
    )

    filtered_count = filtered.count()

    print(
        f"Registros después del filtro: "
        f"{filtered_count:,}"
    )

    # --------------------------------------------------------
    # AGREGACIONES SPARK
    # --------------------------------------------------------

    monthly = (
        filtered
        .groupBy(
            "visit_year",
            "visit_month",
        )
        .agg(
            F.count(
                "visit_occurrence_id"
            ).alias(
                "visit_count"
            ),
            F.avg(
                "visit_duration_days"
            ).alias(
                "avg_duration_days"
            ),
        )
    )

    monthly.write.mode(
        "overwrite"
    ).parquet(
        str(
            RESULTS_DIR
            / "spark_monthly"
        )
    )

    # --------------------------------------------------------
    # JOIN CON PERSON.CSV
    # --------------------------------------------------------

    print(
        "\nEjecutando JOIN con person.csv..."
    )

    person = (
        spark.read
        .option(
            "header",
            True,
        )
        .option(
            "encoding",
            "UTF-16",
        )
        .csv(
            str(
                DATA_DIR
                / "person.csv"
            )
        )
        .select(
            "person_id",
            "gender_source_value",
            "race_source_value",
        )
        .withColumn(
            "person_id",
            F.expr("try_cast(trim(person_id) AS BIGINT)"),
        )
        .filter(F.col("person_id").isNotNull())
    )

    enriched = filtered.join(
        person,
        on="person_id",
        how="left",
    )

    # --------------------------------------------------------
    # AGREGACIÓN DEL JOIN
    # --------------------------------------------------------

    demographic_summary = (
        enriched
        .groupBy(
            "gender_source_value"
        )
        .agg(
            F.count(
                "visit_occurrence_id"
            ).alias(
                "visit_count"
            )
        )
        .orderBy(
            F.col(
                "visit_count"
            ).desc()
        )
    )

    print(
        "\nVisitas por género:"
    )

    demographic_summary.show(
        20,
        truncate=False,
    )

    # --------------------------------------------------------
    # FUNCIÓN DE VENTANA
    # --------------------------------------------------------

    print(
        "\nEjecutando función de ventana..."
    )

    rank_window = (
        Window
        .partitionBy(
            "visit_year"
        )
        .orderBy(
            F.col(
                "visit_count"
            ).desc()
        )
    )

    ranked_months = (
        monthly
        .withColumn(
            "month_rank",
            F.dense_rank().over(
                rank_window
            ),
        )
    )

    top_months = (
        ranked_months
        .filter(
            F.col(
                "month_rank"
            )
            <= 3
        )
    )

    print(
        "\nTop 3 meses por año:"
    )

    top_months.orderBy(
        "visit_year",
        "month_rank",
    ).show(
        30,
        truncate=False,
    )

    top_months.write.mode(
        "overwrite"
    ).parquet(
        str(
            RESULTS_DIR
            / "spark_top_months"
        )
    )

    # --------------------------------------------------------
    # COMPARACIÓN EQUIVALENTE
    #
    # MISMA operación que Dask:
    # groupBy visit_year + count
    # --------------------------------------------------------

    print(
        "\nEjecutando agregación equivalente..."
    )

    comparison_start = (
        time.perf_counter()
    )

    comparison = (
        filtered
        .groupBy(
            "visit_year"
        )
        .agg(
            F.count(
                "visit_occurrence_id"
            ).alias(
                "visit_count"
            )
        )
        .orderBy(
            "visit_year"
        )
    )

    comparison_rows = [
        row.asDict()
        for row in comparison.collect()
    ]

    comparison_time = (
        time.perf_counter()
        - comparison_start
    )

    # --------------------------------------------------------
    # MÉTRICAS
    # --------------------------------------------------------

    elapsed = (
        time.perf_counter()
        - start
    )

    memory_delta = max(
        0.0,
        memory_mb() - before_mb,
    )

    spark.stop()

    counts = {
        str(int(row["visit_year"])): int(
            row["visit_count"]
        )
        for row in comparison_rows
    }

    print(
        f"\nSpark pipeline completo: "
        f"{elapsed:.2f} segundos"
    )

    print(
        f"Spark agregación equivalente: "
        f"{comparison_time:.2f} segundos"
    )

    print(
        f"Delta de memoria: "
        f"{memory_delta:.2f} MB"
    )

    return (
        counts,
        elapsed,
        memory_delta,
        comparison_time,
    )


# ============================================================
# VISUALIZACIONES
# ============================================================

def generar_visualizaciones() -> list[str]:

    import matplotlib.pyplot as plt

    print("\n" + "=" * 70)
    print("ETAPA 3 - VISUALIZACIONES")
    print("=" * 70)

    # --------------------------------------------------------
    # Se leen únicamente los resultados agregados de Spark.
    # No se carga el dataset masivo completo a pandas.
    # --------------------------------------------------------

    aggregated = (
        dd.read_parquet(
            str(
                RESULTS_DIR
                / "spark_monthly"
            )
        )
        .compute()
        .reset_index(drop=True)
    )

    aggregated["period"] = (
        aggregated[
            "visit_year"
        ]
        .astype(int)
        .astype(str)
        + "-"
        + aggregated[
            "visit_month"
        ]
        .astype(int)
        .astype(str)
        .str.zfill(2)
    )

    aggregated = aggregated.sort_values(
        [
            "visit_year",
            "visit_month",
        ]
    )

    plots_dir = (
        RESULTS_DIR
        / "plots"
    )

    plots_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    paths = []

    # --------------------------------------------------------
    # 1. Visitas por año
    # --------------------------------------------------------

    yearly = (
        aggregated
        .groupby(
            "visit_year",
            as_index=False,
        )["visit_count"]
        .sum()
    )

    ax = yearly.plot(
        x="visit_year",
        y="visit_count",
        kind="line",
        marker="o",
        markersize=3,
        linewidth=1.5,
        figsize=(12, 5),
        legend=False,
    )

    years = yearly["visit_year"].astype(int).tolist()
    tick_years = years[::10]
    if (
        years
        and years[-1] not in tick_years
        and years[-1] - tick_years[-1] >= 5
    ):
        tick_years.append(years[-1])
    ax.set_xticks(tick_years)
    ax.set_xticklabels(
        [str(year) for year in tick_years],
        rotation=45,
        ha="right",
    )

    ax.set_title(
        "Visitas por año"
    )

    ax.set_xlabel(
        "Año"
    )

    ax.set_ylabel(
        "Cantidad de visitas"
    )

    path = (
        plots_dir
        / "visitas_por_anio.png"
    )

    ax.figure.tight_layout()
    ax.figure.savefig(
        path,
        dpi=150,
    )

    plt.close(
        ax.figure
    )

    paths.append(
        str(path)
    )

    # --------------------------------------------------------
    # 2. Promedio de visitas por mes
    # --------------------------------------------------------

    monthly = (
        aggregated
        .groupby(
            "visit_month",
            as_index=False,
        )["visit_count"]
        .mean()
    )

    ax = monthly.plot.bar(
        x="visit_month",
        y="visit_count",
        legend=False,
    )

    ax.set_title(
        "Promedio de visitas por mes"
    )

    ax.set_xlabel(
        "Mes"
    )

    ax.set_ylabel(
        "Promedio de visitas"
    )

    path = (
        plots_dir
        / "promedio_visitas_por_mes.png"
    )

    ax.figure.tight_layout()
    ax.figure.savefig(
        path,
        dpi=150,
    )

    plt.close(
        ax.figure
    )

    paths.append(
        str(path)
    )

    # --------------------------------------------------------
    # 3. Duración promedio
    # --------------------------------------------------------

    ax = aggregated.plot(
        x="period",
        y="avg_duration_days",
        figsize=(11, 4),
        legend=False,
    )

    ax.set_title(
        "Duración promedio de las visitas"
    )

    ax.set_xlabel(
        "Periodo"
    )

    ax.set_ylabel(
        "Días"
    )

    ax.tick_params(
        axis="x",
        rotation=90,
    )

    path = (
        plots_dir
        / "duracion_promedio_periodo.png"
    )

    ax.figure.tight_layout()
    ax.figure.savefig(
        path,
        dpi=150,
    )

    plt.close(
        ax.figure
    )

    paths.append(
        str(path)
    )

    print(
        "\n3 visualizaciones generadas:"
    )

    for path in paths:
        print(
            f"  - {path}"
        )

    return paths


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    parser = argparse.ArgumentParser(
        description=(
            "Pipeline integrado "
            "Dask + Spark"
        )
    )

    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        nargs="+",
        default=[8, 16],
        help=(
            "Configuraciones de "
            "spark.sql.shuffle.partitions. "
            "Ejemplo: 8 16"
        ),
    )

    args = parser.parse_args()

    # --------------------------------------------------------
    # Validaciones
    # --------------------------------------------------------

    if shutil.which("java") is None:

        raise RuntimeError(
            "Spark requiere Java 8+ "
            "en PATH o JAVA_HOME."
        )

    if not VISIT_FILES[0].exists():
        raise FileNotFoundError(
            f"No existe: {VISIT_FILES[0]}"
        )

    if not VISIT_FILES[1].exists():
        raise FileNotFoundError(
            f"No existe: {VISIT_FILES[1]}"
        )

    if not (
        DATA_DIR
        / "person.csv"
    ).exists():

        raise FileNotFoundError(
            "No existe person.csv"
        )

    RESULTS_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # --------------------------------------------------------
    # 1. DASK
    # --------------------------------------------------------

    dask_time, dask_memory = (
        ejecutar_dask()
    )

    # --------------------------------------------------------
    # 2. COMPARACIÓN DASK
    # --------------------------------------------------------

    (
        dask_counts,
        dask_comparison_time,
        dask_comparison_memory,
    ) = comparar_dask()

    # --------------------------------------------------------
    # 3. SPARK
    # --------------------------------------------------------

    spark_configurations = []

    for shuffle_partitions in (
        args.shuffle_partitions
    ):

        (
            spark_counts,
            spark_time,
            spark_memory,
            spark_comparison_time,
        ) = ejecutar_spark(
            shuffle_partitions
        )

        # ----------------------------------------------------
        # Validación de resultados
        # ----------------------------------------------------

        if spark_counts != dask_counts:

            raise RuntimeError(
                "Dask y Spark produjeron "
                "resultados distintos."
            )

        spark_configurations.append(
            {
                "shuffle_partitions":
                    shuffle_partitions,
                "pipeline_seconds":
                    round(
                        spark_time,
                        3,
                    ),
                "comparison_seconds":
                    round(
                        spark_comparison_time,
                        3,
                    ),
                "memory_delta_mb":
                    round(
                        spark_memory,
                        2,
                    ),
            }
        )

    # --------------------------------------------------------
    # 4. VISUALIZACIONES
    # --------------------------------------------------------

    plot_paths = (
        generar_visualizaciones()
    )

    # --------------------------------------------------------
    # 5. MÉTRICAS
    # --------------------------------------------------------

    metrics = {

        "pipeline": (
            "Dask CSV -> Parquet "
            "-> Spark"
        ),

        "data_exchange": (
            "Dask writes Parquet "
            "and Spark reads "
            "the same Parquet"
        ),

        "dask": {
            "pipeline_seconds":
                round(
                    dask_time,
                    3,
                ),
            "pipeline_memory_delta_mb":
                round(
                    dask_memory,
                    2,
                ),
            "comparison_seconds":
                round(
                    dask_comparison_time,
                    3,
                ),
            "comparison_memory_delta_mb":
                round(
                    dask_comparison_memory,
                    2,
                ),
        },

        "spark_configurations":
            spark_configurations,

        "equivalent_operation": (
            "groupBy visit_year "
            "and count visit_occurrence_id"
        ),

        "comparison_results":
            dask_counts,

        "advanced_spark": {
            "join":
                "visits JOIN person "
                "ON person_id",
            "window":
                "dense_rank by visit_year "
                "ordered by visit_count",
        },

        "visualizations":
            plot_paths,

        "shuffle_configurations":
            args.shuffle_partitions,
    }

    metrics_path = (
        RESULTS_DIR
        / "pipeline_metrics.json"
    )

    metrics_path.write_text(
        json.dumps(
            metrics,
            indent=2,
        ),
        encoding="utf-8",
    )

    # --------------------------------------------------------
    # RESUMEN FINAL
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("PIPELINE COMPLETADO")
    print("=" * 70)

    print(
        "\nFlujo ejecutado:"
    )

    print(
        "CSV grandes"
    )

    print(
        "  -> Dask: ingesta, limpieza "
        "y variables derivadas"
    )

    print(
        "  -> Parquet"
    )

    print(
        "  -> Spark: lectura, filtros "
        "y agregaciones"
    )

    print(
        "  -> Spark: JOIN + ventana"
    )

    print(
        "  -> Comparación Dask vs Spark"
    )

    print(
        "  -> 3 visualizaciones"
    )

    print(
        f"\nMétricas guardadas en:"
    )

    print(
        metrics_path
    )


if __name__ == "__main__":
    main()
