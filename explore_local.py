"""
Script de exploración y análisis offline para snapshots Parquet de WeatherNext usando Polars.
Permite analizar datos climáticos en memoria local con costo $0 y cero llamadas a GCP.
"""

import sys
from pathlib import Path
import polars as pl


def get_target_file(custom_path: str | None = None) -> Path:
    """Obtiene la ruta del archivo Parquet a explorar (el más reciente o el especificado)."""
    if custom_path:
        path = Path(custom_path)
        if not path.exists():
            raise FileNotFoundError(f"No se encontró el archivo especificado: {path.resolve()}")
        return path

    data_dir = Path("data")
    if not data_dir.exists():
        raise FileNotFoundError("El directorio 'data/' no existe. Ejecuta primero 'python main.py' para descargar un snapshot.")

    # Buscar archivos versionados
    parquets = sorted(list(data_dir.glob("weathernext_*.parquet")), key=lambda p: p.stat().st_mtime, reverse=True)
    if not parquets:
        raise FileNotFoundError("No se encontraron archivos Parquet en 'data/'.")

    return parquets[0]


def explore_parquet(file_path: Path):
    print(f"\n=======================================================")
    print(f" EXPLORADOR OFFLINE DE WEATHERNEXT (Polars Engine)")
    print(f"=======================================================")
    print(f" Archivo analizado: {file_path.resolve()}")
    print(f" Tamano en disco:   {file_path.stat().st_size / 1024:.2f} KB\n")

    # Carga ultrarrápida con Polars
    df = pl.read_parquet(file_path)

    print(f" Dimensiones: {df.shape[0]} filas x {df.shape[1]} columnas")
    print(f" Esquema de columnas:")
    for col_name, dtype in df.schema.items():
        print(f"   * {col_name:20} -> {dtype}")

    print("\n--- Resumen Estadistico de Variables Fisicas ---")
    numeric_cols = [
        c for c in [
            "temp_2m_celsius",
            "precip_6h_mm",
            "pressure_hpa",
            "wind_speed_kmh",
            "wind_speed_100m_kmh",
        ] if c in df.columns
    ]
    if numeric_cols:
        stats = df.select(
            [
                pl.col(c).mean().round(2).alias(f"{c}_media")
                for c in numeric_cols
            ] + [
                pl.col(c).min().round(2).alias(f"{c}_min")
                for c in numeric_cols
            ] + [
                pl.col(c).max().round(2).alias(f"{c}_max")
                for c in numeric_cols
            ]
        )
        print(stats)

    print("\n--- Primeras 5 filas del Dataset ---")
    print(df.head(5))

    if "forecast_hours" in df.columns:
        horizons = df.select(pl.col("forecast_hours").unique()).sort("forecast_hours").to_series().to_list()
        print(f"\n Horizontes de pronostico disponibles (horas): {horizons}")

    if "ensemble_member" in df.columns:
        members = df.select(pl.col("ensemble_member").unique()).sort("ensemble_member").to_series().to_list()
        print(f" Miembros del ensamble: {len(members)} miembros ({members[:5]}...)")

    print("\n[OK] Exploracion completada 100% offline (Costo $0 GCP).")


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else None
    try:
        path = get_target_file(target)
        explore_parquet(path)
    except Exception as e:
        print(f"[ERROR] Error en exploracion: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
