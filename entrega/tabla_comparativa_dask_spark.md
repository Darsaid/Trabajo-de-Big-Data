# Tabla comparativa Dask vs. Spark

Operación equivalente: contar `visit_occurrence_id` agrupado por `visit_year`.

| Herramienta | Operación | Datos | Configuración | Tiempo (s) | Memoria (MB) | Observaciones |
| --- | --- | --- | --- | --- | --- | --- |
| Dask | groupBy visit_year + count | 10,32 GB CSV -> 1,81 GB Parquet | 2 particiones de entrada | 0,932 | 0,00 | Agregación equivalente sobre Parquet |
| Spark | groupBy visit_year + count | 10,32 GB CSV -> 1,81 GB Parquet | local[*], shuffle.partitions=8 | 0,859 | 5,50 | Mismo resultado; cambia el paralelismo |
| Spark | groupBy visit_year + count | 10,32 GB CSV -> 1,81 GB Parquet | local[*], shuffle.partitions=16 | 0,786 | 0,21 | Mismo resultado; cambia el paralelismo |

Los tiempos anteriores corresponden a la ejecución registrada en
`data/results/pipeline_metrics.json`. La operación equivalente tardó 0,932 s
con Dask, 0,859 s con Spark usando 8 particiones y 0,786 s con Spark usando
16 particiones. La ejecución de Spark con 16 particiones fue la más rápida en
este equipo. Los resultados de Dask y Spark fueron validados como iguales por
el pipeline.

La duración total del pipeline fue de 141,957 s para la etapa Dask y de
24,939 s con Spark usando 8 particiones o 11,002 s usando 16 particiones. Esos
valores incluyen lectura, transformación, escritura, joins, ventana y salidas;
no deben confundirse con el tiempo de la operación equivalente de la tabla.