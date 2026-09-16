# Pipeline integrador Dask + Spark

Este proyecto utiliza el caso médico Synthea y procesa dos archivos
`visit_occurrence` de aproximadamente 10,32 GB en total.

## Publicar y compartir el proyecto

El repositorio debe contener el código, `Dockerfile`, `docker-compose.yml`,
`requirements.txt`, documentación, tabla comparativa y notebooks. No deben
subirse los CSV masivos, el entorno `.venv`, el Parquet generado ni los
resultados temporales. Estas rutas están excluidas en `.gitignore` porque
ocupan varios gigabytes y se regeneran durante la ejecución.

Después de crear un repositorio vacío en GitHub, ejecutar desde la raíz del
proyecto:

```powershell
git init
git add .
git status
git commit -m "Entrega pipeline Dask Spark"
git branch -M main
git remote add origin https://github.com/USUARIO/NOMBRE-REPOSITORIO.git
git push -u origin main
```

Cada integrante puede descargarlo así:

```powershell
git clone https://github.com/USUARIO/NOMBRE-REPOSITORIO.git
cd NOMBRE-REPOSITORIO
```

Luego debe copiar sus datos Synthea en la ruta indicada en
`data/README.md`, crear el entorno virtual e instalar las dependencias. El
pipeline local se ejecuta con Python del `.venv`; para Docker, los datos se
montan desde la carpeta local mediante `docker-compose.yml`.

## Flujo implementado

`src/04_pipeline_integrado.py` ejecuta un único flujo:

1. Dask lee los dos CSV UTF-16 y conserva cada archivo como una partición.
2. Dask limpia fechas y valores faltantes, selecciona columnas y crea
	`visit_year`, `visit_month` y `visit_duration_days`.
3. Dask escribe `data/processed/visits_parquet`.
4. Spark lee exactamente ese Parquet, aplica filtros, agregaciones y un `join`
	con `person.csv`.
5. Spark aplica una ventana para obtener los tres meses con más visitas por año.
6. La misma cuenta de visitas por año se ejecuta en Dask y Spark y se comparan
	sus resultados y tiempos.
7. Se generan tres gráficos a partir de la tabla mensual agregada, nunca desde
	los CSV masivos.

Dask se utiliza en la ingesta y limpieza porque permite trabajar de forma
perezosa sobre varios archivos y particiones sin cargar todo el CSV en memoria.
Spark se utiliza después porque ofrece un motor distribuido adecuado para
agregaciones, joins y funciones de ventana sobre el Parquet columnar.

## Arquitectura del pipeline

El flujo está organizado en cuatro etapas y usa Parquet como contrato de
intercambio entre motores:

1. **Ingesta Dask:** se leen los dos archivos `visit_occurrence` en UTF-16 y
	se concatenan como particiones Dask, sin cargar todo el dataset en pandas.
2. **Limpieza Dask:** se seleccionan las columnas necesarias, se convierten
	fechas, se eliminan registros sin identificadores o fecha de inicio, se
	imputan valores faltantes y se crean `visit_year`, `visit_month` y
	`visit_duration_days`.
3. **Intercambio y procesamiento Spark:** Dask escribe
	`data/processed/visits_parquet` con PyArrow/Snappy. Spark lee esa misma
	carpeta, filtra duraciones válidas, agrega por año y mes, y guarda los
	resultados en Parquet.
4. **Análisis avanzado y visualización:** Spark cruza las visitas con
	`person.csv`, calcula un resumen demográfico y aplica `dense_rank` para
	obtener los meses principales por año. Las gráficas se construyen solo a
	partir de la tabla mensual agregada.

La arquitectura demuestra el intercambio Dask -> Parquet -> Spark dentro del
mismo flujo ejecutable.

## Ejecución desde cero

### Requisitos

- Windows, Linux o macOS.
- Python 3.11 o superior. El contenedor usa Python 3.11.
- Java 8 o superior para Spark. Se recomienda JDK 17.
- AWS CLI para descargar los archivos originales desde el bucket público de
	Synthea. En Windows puede instalarse con `winget install --id Amazon.AWSCLI -e`.
- `lzop` para descomprimir. Windows ya incluye `tools/lzop/lzop103w/lzop.exe`.
- Los datos descomprimidos de Synthea en `data/raw/synthea23m/csv/`,
	incluyendo `person.csv`, `visit_occurrence_0.csv` y
	`visit_occurrence_1.csv`.

### Instalación local

Desde la carpeta raíz del proyecto, crear un entorno virtual e instalar las
dependencias:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Configurar `JAVA_HOME` con la carpeta del JDK y añadir `%JAVA_HOME%\bin` al
`PATH`. Comprobar la instalación:

```powershell
java -version
python --version
python -c "import dask, pyarrow, pyspark; print('Dependencias OK')"
```

En Windows, el proyecto incluye `tools/hadoop/bin/winutils.exe` y
`hadoop.dll`. El pipeline configura `HADOOP_HOME` automáticamente cuando
encuentra esos archivos.

### Descargar y preparar los datos

Los datos originales se obtienen del bucket público de AWS S3
`s3://synthea-omop/synthea23m/`. Ejecutar las instrucciones de
`data/README.md` para descargar los tres archivos `.lzo`, descomprimirlos y
organizarlos en la carpeta `csv`.

La verificación inicial es:

```powershell
.\.venv\Scripts\python.exe .\src\01_verificar_datos.py
```

Este comando verifica los archivos `.lzo` originales. El pipeline posterior
utiliza las versiones CSV descomprimidas.

### Ejecutar el pipeline completo

Verificar primero que existan los tres archivos de entrada:

```powershell
Get-ChildItem .\data\raw\synthea23m\csv\person.csv
Get-ChildItem .\data\raw\synthea23m\csv\visit_occurrence_0.csv
Get-ChildItem .\data\raw\synthea23m\csv\visit_occurrence_1.csv
```

Ejecutar todas las etapas, incluyendo las dos configuraciones de Spark:

```powershell
python .\src\04_pipeline_integrado.py --shuffle-partitions 8 16
```

Para una sola configuración de Spark:

```powershell
python .\src\04_pipeline_integrado.py --shuffle-partitions 16
```

El script vuelve a generar los resultados, mide tiempos y memoria, valida que
la agregación Dask y Spark produzca los mismos conteos, y crea las salidas
descritas abajo.

### Verificar resultados

```powershell
Get-Content .\data\results\pipeline_metrics.json
Get-ChildItem .\data\processed\visits_parquet
Get-ChildItem .\data\results\spark_monthly
Get-ChildItem .\data\results\spark_top_months
Get-ChildItem .\data\results\plots
```

## Resultados

El script genera:

- `data/processed/visits_parquet`: intercambio Dask -> Spark.
- `data/results/spark_monthly`: agregación mensual de Spark.
- `data/results/spark_top_months`: resultado de la ventana.
- `data/results/pipeline_metrics.json`: tiempos, memoria y comparación.
- `data/results/plots/visitas_por_anio.png`: visitas totales por año.
- `data/results/plots/promedio_visitas_por_mes.png`: promedio mensual de visitas.
- `data/results/plots/duracion_promedio_periodo.png`: duración promedio por periodo.

En la ejecución registrada en `data/results/pipeline_metrics.json`, Dask tardó
141,957 s en la etapa de ingesta, limpieza y escritura Parquet. Spark tardó
24,939 s con 8 particiones de shuffle y 11,002 s con 16 particiones para su
etapa completa. La operación equivalente de conteo por `visit_year` tardó
0,932 s en Dask, 0,859 s en Spark con 8 particiones y 0,786 s en Spark con 16
particiones. Los resultados fueron iguales.

## Comparación y conclusión técnica

La operación equivalente es contar visitas agrupadas por `visit_year`. Dask la
ejecuta sobre el Parquet generado y Spark la ejecuta sobre el DataFrame filtrado
de la misma salida. Ambos resultados, tiempos y memoria observada se guardan
en `pipeline_metrics.json`.

En un escenario real elegiríamos Dask para explorar, limpiar y preparar archivos
heterogéneos porque trabaja de forma perezosa con APIs cercanas a pandas y
permite controlar particiones. Elegiríamos Spark para joins, agregaciones y
ventanas cuando se requiere un motor distribuido con planificación SQL madura,
mejor tolerancia a fallos y crecimiento hacia un clúster. El Parquet funciona
como contrato columnar eficiente entre ambas etapas.

En conclusión, Dask resulta conveniente para la preparación inicial y la
interoperabilidad con archivos tabulares, mientras que Spark ofrece mejor
capacidad para el procesamiento analítico distribuido y las operaciones
avanzadas. La medición realizada muestra además que Spark fue más rápido en la
operación equivalente y que aumentar `shuffle.partitions` de 8 a 16 redujo el
tiempo total en este entorno. Esta conclusión depende del hardware, del tamaño
de los datos y de la configuración, por lo que en producción debe validarse
con una prueba representativa.

## Docker: prueba y reproducibilidad completa

El `Dockerfile` instala Python 3.11, Java 17, Dask y PySpark dentro de la
imagen. Los datos masivos no se copian durante la construcción; se montan como
volumen para conservar la imagen ligera y escribir los resultados directamente
en las carpetas locales.

### 1. Construir y probar la imagen

Construir la imagen:

```powershell
docker build -t trabajo-bigdata .
```

Ejecutar la prueba mínima:

```powershell
docker run --rm trabajo-bigdata
```

La salida esperada incluye:

```text
Dask disponible: 2 + 2 = 4
PySpark disponible: filas de prueba = 5
```

Guardar la salida de la prueba como evidencia:

```powershell
docker build -t trabajo-bigdata .
docker run --rm trabajo-bigdata 2>&1 | Tee-Object .\data\results\docker_smoke_test.txt
```

### 2. Ejecutar el pipeline completo con Docker Compose

`docker-compose.yml` monta `./data` en `/app/data` y ejecuta el pipeline
completo con Spark configurado con 16 particiones:

```powershell
docker compose build
docker compose run --rm pipeline
```

Para guardar la evidencia de la ejecución completa:

```powershell
docker compose build
docker compose run --rm pipeline 2>&1 | Tee-Object .\data\results\docker_pipeline_execution.txt
```

Los resultados quedan en `data/processed` y `data/results` porque `/app/data`
se monta como volumen. La composición tiene un único servicio porque el
pipeline utiliza Spark local; no requiere un clúster master/worker.

## Estructura principal

```text
data/          Datos de entrada, Parquet y resultados
notebooks/     Exploración y revisión de resultados del pipeline
src/           Scripts Dask, Spark, pipeline y prueba Docker
tools/         Soporte Hadoop para ejecución local en Windows
Dockerfile     Imagen reproducible con Java y PySpark
docker-compose.yml Ejecución reproducible del pipeline completo
requirements.txt
README.md
```
