# Tabla comparativa Dask vs. Spark

Operación equivalente: contar `visit_occurrence_id` agrupado por `visit_year`.

| Herramienta | Operación | Datos | Configuración | Tiempo (s) | Memoria (MB) | Observaciones |
| --- | --- | --- | --- | --- | --- | --- |
| Dask | groupBy visit_year + count | 5,1 GB CSV -> 1,83 GB Parquet | 85 particiones de entrada (bloques de 64 MB) | 15,961 | 211,53 | Agregación equivalente sobre Parquet |
| Spark | groupBy visit_year + count | 5,1 GB CSV -> 1,83 GB Parquet | local[*], shuffle.partitions=8 | 7,121 | 1,88 | Mismo resultado; cambia el paralelismo |
| Spark | groupBy visit_year + count | 5,1 GB CSV -> 1,83 GB Parquet | local[*], shuffle.partitions=16 | 4,483 | 0,00 | Mismo resultado; cambia el paralelismo |

Los tiempos anteriores corresponden a la ejecución registrada en
`data/results/pipeline_metrics.json`, realizada con Docker Compose en un
contenedor con ~4 GB de RAM. La operación equivalente tardó 15,961 s con Dask,
7,121 s con Spark usando 8 particiones y 4,483 s con Spark usando 16
particiones. La ejecución de Spark con 16 particiones fue la más rápida en
este equipo. Los resultados de Dask y Spark fueron validados como iguales por
el pipeline.

La duración total del pipeline fue de 1218,663 s para la etapa Dask y de
124,628 s con Spark usando 8 particiones o 101,542 s usando 16 particiones.
Esos valores incluyen lectura, transformación, escritura, joins, ventana y
salidas; no deben confundirse con el tiempo de la operación equivalente de la
tabla.
