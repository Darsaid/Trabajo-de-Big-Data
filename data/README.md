# Datos locales

Los datos masivos provienen del bucket público de AWS S3
`s3://synthea-omop/synthea23m/` y no se publican en Git porque ocupan varios
gigabytes.

## Descargar los originales

Instalar AWS CLI si todavía no está disponible:

```powershell
winget install --id Amazon.AWSCLI -e
```

Cerrar y abrir PowerShell después de la instalación. Luego ejecutar desde la
raíz del proyecto:

```powershell
New-Item -ItemType Directory -Force .\data\raw\synthea23m | Out-Null
aws s3 cp s3://synthea-omop/synthea23m/person.csv.lzo .\data\raw\synthea23m\person.csv.lzo --no-sign-request
aws s3 cp s3://synthea-omop/synthea23m/visit_occurrence.csv.0.lzo .\data\raw\synthea23m\visit_occurrence.csv.0.lzo --no-sign-request
aws s3 cp s3://synthea-omop/synthea23m/visit_occurrence.csv.1.lzo .\data\raw\synthea23m\visit_occurrence.csv.1.lzo --no-sign-request
```

## Descomprimir y preparar el pipeline

Usar el `lzop.exe` incluido en el proyecto para descomprimir:

```powershell
& .\tools\lzop\lzop103w\lzop.exe -d -f .\data\raw\synthea23m\person.csv.lzo
& .\tools\lzop\lzop103w\lzop.exe -d -f .\data\raw\synthea23m\visit_occurrence.csv.0.lzo
& .\tools\lzop\lzop103w\lzop.exe -d -f .\data\raw\synthea23m\visit_occurrence.csv.1.lzo
New-Item -ItemType Directory -Force .\data\raw\synthea23m\csv | Out-Null
Move-Item -Force .\data\raw\synthea23m\person.csv .\data\raw\synthea23m\csv\person.csv
Move-Item -Force .\data\raw\synthea23m\visit_occurrence.csv.0 .\data\raw\synthea23m\csv\visit_occurrence_0.csv
Move-Item -Force .\data\raw\synthea23m\visit_occurrence.csv.1 .\data\raw\synthea23m\csv\visit_occurrence_1.csv
```

Verificar los archivos comprimidos originales con:

```powershell
.\.venv\Scripts\python.exe .\src\01_verificar_datos.py
```

El pipeline utiliza los CSV descomprimidos de esta ruta:

```text
data/raw/synthea23m/csv/
```

Debe contener `person.csv`, `visit_occurrence_0.csv` y
`visit_occurrence_1.csv`. `lzop` los genera en UTF-8 (unos 5,1 GB los
dos archivos de visitas). No es necesario convertirlos: el pipeline detecta
automáticamente si están en UTF-8 o en UTF-16.

Las carpetas `data/processed/` y `data/results/` se generan automáticamente al
ejecutar el pipeline.
