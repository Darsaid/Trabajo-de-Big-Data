from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "synthea23m"

files = [
    RAW_DIR / "person.csv.lzo",
    RAW_DIR / "visit_occurrence.csv.0.lzo",
    RAW_DIR / "visit_occurrence.csv.1.lzo",
]

print("=" * 70)
print("VERIFICACIÓN DEL DATASET SYNTHEA")
print("=" * 70)

print(f"\nDirectorio:")
print(RAW_DIR)
print("Origen: s3://synthea-omop/synthea23m/")

if not RAW_DIR.exists():
    raise FileNotFoundError(f"No existe: {RAW_DIR}")

total_bytes = 0
missing_files = []

print("\nArchivos:\n")

for file in files:
    if file.exists():
        size_bytes = file.stat().st_size
        size_gb = size_bytes / (1024 ** 3)
        total_bytes += size_bytes

        print(f"✓ {file.name}")
        print(f"  Tamaño: {size_gb:.2f} GB")
    else:
        print(f"✗ FALTA: {file.name}")
        missing_files.append(file.name)

total_gb = total_bytes / (1024 ** 3)

print("\n" + "=" * 70)
print(f"TAMAÑO TOTAL: {total_gb:.2f} GB")
print("=" * 70)

if missing_files:
    print("\nFaltan archivos del dataset original.")
    print("Descárguelos con las instrucciones de data/README.md.")
    sys.exit(1)