# Pipeline integrador Dask + Spark

Este proyecto utiliza el caso médico Synthea y procesa dos archivos
`visit_occurrence` de aproximadamente 5,1 GB en total (UTF-8, tal como los
genera `lzop` al descomprimir).

## Local vs. Docker

Son dos formas de ejecutar el mismo pipeline, no dos pipelines distintos:

- **Local:** se usa `.venv`, Python y Java instalados en el equipo. El comando
	principal es `python .\src\04_pipeline_integrado.py`.
- **Docker:** la imagen instala Python, Java, Dask y PySpark. El contenedor
	ejecuta el mismo código de `src/04_pipeline_integrado.py`.

Los datos de Synthea no se suben a GitHub porque ocupan varios gigabytes. Se
descargan y preparan una sola vez en el equipo anfitrión siguiendo
`data/README.md`. Después pueden usarse de las dos maneras:

```text
data/raw/*.csv -> pipeline local
data/raw/*.csv -> volumen ./data -> contenedor Docker -> pipeline
```

La prueba mínima `src/05_prueba_docker.py` no necesita datos y solo confirma
que Dask y PySpark funcionan dentro de la imagen. Para ejecutar el pipeline
real con Docker sí deben existir los CSV en `data/raw/synthea23m/csv/`.

## Flujo paso a paso recomendado

### 1. Preparar los datos una sola vez

Descargar y descomprimir los datos siguiendo [data/README.md](data/README.md).
Al finalizar, deben existir estos archivos:

```text
data/raw/synthea23m/csv/person.csv
data/raw/synthea23m/csv/visit_occurrence_0.csv
data/raw/synthea23m/csv/visit_occurrence_1.csv
```

### 2. Construir la imagen Docker

Desde la raíz del proyecto:

```powershell
docker compose build
```

La imagen instala Python, Java, Dask y PySpark. Los datos grandes no se
copian a la imagen.

### 3. Ejecutar la prueba mínima

```powershell
docker compose run --rm pipeline python src/05_prueba_docker.py
```

Debe mostrar que Dask calcula `2 + 2 = 4` y que Spark cuenta cinco filas.
Esta prueba no usa los CSV.

### 4. Ejecutar el pipeline real

```powershell
docker compose run --rm pipeline
```

El volumen definido en `docker-compose.yml` conecta las mismas carpetas:

```text
equipo:      ./data  <-->  contenedor: /app/data
```

Así, el contenedor lee los CSV desde `/app/data/raw` y escribe los resultados
en `/app/data/processed` y `/app/data/results`, que aparecen directamente en
las carpetas locales `data/processed` y `data/results`.

### 5. Revisar los resultados

```text
data/results/pipeline_metrics.json
data/results/spark_monthly/
data/results/spark_top_months/
data/results/plots/
```

El mismo flujo puede ejecutarse localmente con `.venv`, pero no es necesario
activar `.venv` cuando se usa Docker.

## Reproducir el proyecto desde GitHub

El código completo está disponible en el repositorio:

```text
https://github.com/Darsaid/Trabajo-de-Big-Data
```

Cada integrante debe clonarlo así:

```powershell
git clone https://github.com/Darsaid/Trabajo-de-Big-Data.git
cd Trabajo-de-Big-Data
```

Después, debe obtener los datos Synthea desde el bucket público de AWS S3 y
seguir las instrucciones de `data/README.md` para descargarlos, descomprimirlos
y ubicarlos en `data/raw/synthea23m/csv/`. Los datos no están dentro de GitHub
porque ocupan varios gigabytes.

Crear el entorno e instalar las dependencias:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Ejecutar el pipeline completo:

```powershell
python .\src\04_pipeline_integrado.py --shuffle-partitions 8 16
```

También se puede reproducir mediante Docker Compose, montando los datos
locales en el contenedor:

```powershell
docker compose build
docker compose run --rm pipeline
```

## Flujo implementado

`src/04_pipeline_integrado.py` ejecuta un único flujo:

1. Dask lee los dos CSV en bloques de 64 MB (unas 85 particiones en total).
	El encoding se detecta automáticamente: los CSV en UTF-8 se dividen en
	bloques; si alguna copia está en UTF-16 (con BOM), cada archivo se lee como
	una sola partición porque ese formato no puede dividirse de forma segura.
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

1. **Ingesta Dask:** se leen los dos archivos `visit_occurrence` en bloques
	de 64 MB y se concatenan como particiones Dask, sin cargar todo el dataset
	en pandas.
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

En la ejecución registrada en `data/results/pipeline_metrics.json` (Docker
Compose con ~4 GB de RAM, CSV UTF-8 en 85 particiones de 64 MB), Dask tardó
1218,663 s en la etapa de ingesta, limpieza y escritura Parquet. Spark tardó
124,628 s con 8 particiones de shuffle y 101,542 s con 16 particiones para su
etapa completa. La operación equivalente de conteo por `visit_year` tardó
15,961 s en Dask, 7,121 s en Spark con 8 particiones y 4,483 s en Spark con 16
particiones. Los resultados fueron iguales: 42.404.379 visitas válidas.

Para no superar la memoria del contenedor, la etapa Dask se ejecuta en un
proceso aparte que libera su memoria antes de iniciar la JVM de Spark.

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
en las carpetas locales. La descarga desde S3 y la descompresión se realizan
antes de iniciar el contenedor, siguiendo `data/README.md`.

El orden completo en otra máquina es:

1. Clonar el repositorio.
2. Descargar y descomprimir los datos en `data/raw/synthea23m/csv/`.
3. Construir la imagen Docker.
4. Ejecutar la prueba mínima de Dask y Spark.
5. Ejecutar el pipeline con Docker Compose.

Comprobar antes de iniciar el contenedor que existan estos archivos en el
equipo anfitrión:

```powershell
Get-ChildItem .\data\raw\synthea23m\csv\person.csv
Get-ChildItem .\data\raw\synthea23m\csv\visit_occurrence_0.csv
Get-ChildItem .\data\raw\synthea23m\csv\visit_occurrence_1.csv
```

### 1. Construir la imagen

Desde la carpeta raíz del proyecto, construir la imagen una sola vez:

```powershell
docker compose build
```

Este comando instala Python, Java, Dask y PySpark dentro de la imagen. No
descarga los datos ni los copia dentro de la imagen.

### 2. Probar Dask y Spark sin datos

Esta prueba es opcional, pero sirve para comprobar que Docker funciona antes
de procesar los archivos grandes:

```powershell
docker compose run --rm pipeline python src/05_prueba_docker.py
```

La salida esperada incluye:

```text
Dask disponible: 2 + 2 = 4
PySpark disponible: filas de prueba = 5
```

Guardar la salida de la prueba como evidencia:

```powershell
docker compose run --rm pipeline python src/05_prueba_docker.py 2>&1 |
	Tee-Object .\data\results\docker_smoke_test.txt
```

### 3. Ejecutar el pipeline completo

El archivo `docker-compose.yml` monta la carpeta local `./data` dentro del
contenedor como `/app/data` y ejecuta el pipeline con Spark configurado con 8
y 16 particiones de shuffle:

```text
Equipo anfitrión                 Contenedor
./data  -----------------------> /app/data
./data/raw/*.csv --------------> /app/data/raw/*.csv
./data/results/ <--------------- /app/data/results/
```

Por tanto, no se debe copiar la carpeta de datos (varios GB) a la imagen. Docker la usa
como volumen y el contenedor lee y escribe directamente en la carpeta local.

Ejecutar:

```powershell
docker compose run --rm pipeline
```

Dentro del contenedor, `/app/data/raw/synthea23m/csv` contiene los datos que
estaban en `./data/raw/synthea23m/csv` del equipo anfitrión. El Parquet y los
resultados generados por Dask y Spark aparecen automáticamente en las carpetas
locales `data/processed` y `data/results`.

Para guardar la evidencia de la ejecución completa en un archivo:

```powershell
docker compose run --rm pipeline 2>&1 |
	Tee-Object .\data\results\docker_pipeline_execution.txt
```

Los resultados quedan en `data/processed` y `data/results` porque `/app/data`
se monta como volumen. La composición tiene un único servicio porque el
pipeline utiliza Spark local; no requiere un clúster master/worker.

### Memoria de Docker

Docker Desktop suele asignar unos 4 GB de RAM a los contenedores. Por eso
`docker-compose.yml` limita la memoria del driver de Spark con
`SPARK_DRIVER_MEMORY: 2g`; con el valor local por defecto (4 GB) la JVM supera
el límite del contenedor y Spark se detiene con
`Py4JNetworkError: Answer from Java side is empty`. Si Docker Desktop tiene
más memoria asignada (Settings -> Resources), puede subirse ese valor.

Durante el `join` Spark puede mostrar avisos
`WARN RowBasedKeyValueBatch: Calling spill()`. Solo indican que la memoria
está ajustada; Spark continúa y el resultado es correcto.

### Comandos completos

Si los datos ya están preparados, la secuencia mínima es:

```powershell
docker compose build
docker compose run --rm pipeline python src/05_prueba_docker.py
docker compose run --rm pipeline
```

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
