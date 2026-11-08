"""
WeatherNext 3 (0.1° / 10 km & 0.05° / 5 km, paso 1h, 250 miembros)
Pipeline de Extracción Nativo en BigQuery Analytics Hub (SQL Columnar + Apache Arrow).

Accede de forma optimizada a las vistas autorizadas weathernext_3_0_0_0p05deg y
weathernext_3_0_0_0p1deg, aplicando poda espacial de clusters, filtrado exacto
por presets y JOIN canónico entero (i10, j10).
Fallback disponible a GCS Zarr v3 vía zarr_wn3_fallback.py.
"""

import argparse
import configparser
from datetime import datetime, timezone
import os
from pathlib import Path
import time

from google.cloud import bigquery
import polars as pl
from rich.console import Console

from usage_tracker import get_bigquery_client, record_local_usage

console = Console()

# Coordenadas 5 km de presets oficiales para optimización de filas de salida
PRESETS_CHILE = [
    (-33.45, -70.60),  # Ñuñoa (Casa)
    (-33.40, -70.55),  # Las Condes (Trabajo)
    (-33.45, -70.50),  # Pie Andino
    (-33.35, -70.50),  # Las Condes Alto
    (-33.35, -70.30),  # Precordillera / Farellones
    (-33.65, -70.35),  # San José de Maipo
    (-33.45, -70.65),  # Santiago Poniente / Centro
    (-33.25, -70.70),  # Santiago Norte / Chicureo
    (-33.05, -71.60),  # Valparaíso / Viña del Mar
    (-34.15, -70.75),  # Rancagua
    (-29.95, -71.25),  # La Serena / Coquimbo
]

PRESETS_RIO = [
    (-22.90, -43.20),  # Copacabana
    (-23.10, -44.20),  # Ilha Grande
    (-23.00, -44.30),  # Angra dos Reis
    (-23.20, -44.70),  # Paraty
    (-22.70, -41.90),  # Búzios
]


def load_wn3_config(config_path: str = "config.ini") -> dict:
    """Carga configuración para WeatherNext 3.0."""
    cfg = configparser.ConfigParser()
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"No existe {config_path}")
    cfg.read(path, encoding="utf-8")

    project_id = os.environ.get("GCP_PROJECT_ID") or cfg.get("GCP", "project_id", fallback="").strip()
    creds_file = cfg.get("GCP", "credentials_file", fallback="").strip()

    wn3_dataset = cfg.get("WEATHERNEXT3", "dataset", fallback="weathernext_3").strip()
    wn3_0p1 = cfg.get("WEATHERNEXT3", "table_0p1", fallback="weathernext_3_0_0_0p1deg").strip()
    wn3_0p05 = cfg.get("WEATHERNEXT3", "table_0p05", fallback="weathernext_3_0_0_0p05deg").strip()
    default_source = cfg.get("WEATHERNEXT3", "default_source", fallback="bq").strip().lower()
    default_hours_chile = cfg.getint("WEATHERNEXT3", "default_hours_chile", fallback=120)
    default_hours_rio = cfg.getint("WEATHERNEXT3", "default_hours_rio", fallback=216)
    max_bytes_billed_mb = cfg.getfloat("WEATHERNEXT3", "max_bytes_billed_mb", fallback=10000000.0)

    return {
        "project_id": project_id,
        "credentials_file": creds_file,
        "dataset": wn3_dataset,
        "table_0p1": wn3_0p1,
        "table_0p05": wn3_0p05,
        "default_source": default_source,
        "default_hours_chile": default_hours_chile,
        "default_hours_rio": default_hours_rio,
        "max_bytes_billed_mb": max_bytes_billed_mb,
    }


def resolve_latest_wn3_init_time(client: bigquery.Client, cfg: dict) -> tuple[datetime, str]:
    """
    Detecta la última corrida común de WeatherNext 3.0 en ambas vistas (0.05° y 0.1°)
    mediante consulta podada espacialmente (costo 20 MB) y registra el consumo en el medidor
    local para conciliación exacta contra BigQuery.
    """
    q = f"""
    SELECT
      (SELECT MAX(init_time) FROM `{cfg['project_id']}.{cfg['dataset']}.{cfg['table_0p05']}` 
       WHERE init_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR) 
         AND ST_DWITHIN(geography, ST_GEOGPOINT(-70.60, -33.45), 15000)) as max_05,
      (SELECT MAX(init_time) FROM `{cfg['project_id']}.{cfg['dataset']}.{cfg['table_0p1']}` 
       WHERE init_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR) 
         AND ST_DWITHIN(geography, ST_GEOGPOINT(-70.60, -33.45), 15000)) as max_10
    """
    job = client.query(q, job_config=bigquery.QueryJobConfig(use_query_cache=True))
    row = list(job.result())[0]
    max_05 = row.max_05
    max_10 = row.max_10

    # Conciliar consulta de descubrimiento en el medidor local
    record_local_usage(
        job_id=job.job_id,
        bytes_billed=job.total_bytes_billed,
        bytes_processed=job.total_bytes_processed,
        query_type="discovery_wn3_init_bq",
        timestamp=datetime.now(timezone.utc),
    )

    if not max_05 or not max_10:
        raise ValueError(
            f"No se encontró una corrida reciente común en {cfg['dataset']} (0.05°: {max_05}, 0.1°: {max_10})"
        )

    chosen_init = min(max_05, max_10)
    init_str = chosen_init.strftime("%Y-%m-%d %H:%M:%S+00")
    return chosen_init, init_str


def build_wn3_bigquery_sql(
    cfg: dict,
    init_time_str: str,
    is_rio: bool = False,
    hours: int = 120,
) -> str:
    """
    Construye la consulta SQL optimizada con filtrado exacto por presets y
    unión espacial canónica (i10, j10) entre 5 km y 10 km.
    """
    if is_rio:
        presets = PRESETS_RIO
        filter_5km = """
            ST_DWITHIN(geography, ST_MAKEPOLYGON(ST_MAKELINE([
              ST_GEOGPOINT(-44.9, -23.4),
              ST_GEOGPOINT(-41.7, -23.4),
              ST_GEOGPOINT(-41.7, -22.5),
              ST_GEOGPOINT(-44.9, -22.5),
              ST_GEOGPOINT(-44.9, -23.4)
            ])), 0)
        """
        filter_10km = """
            ST_DWITHIN(geography, ST_MAKEPOLYGON(ST_MAKELINE([
              ST_GEOGPOINT(-45.0, -23.5),
              ST_GEOGPOINT(-41.6, -23.5),
              ST_GEOGPOINT(-41.6, -22.4),
              ST_GEOGPOINT(-45.0, -22.4),
              ST_GEOGPOINT(-45.0, -23.5)
            ])), 0)
        """
    else:
        presets = PRESETS_CHILE
        filter_5km = """
            (
              ST_DWITHIN(geography, ST_GEOGPOINT(-70.60, -33.45), 115000)
              OR ST_DWITHIN(geography, ST_GEOGPOINT(-71.25, -29.95), 25000)
            )
        """
        filter_10km = """
            (
              ST_DWITHIN(geography, ST_GEOGPOINT(-70.60, -33.45), 125000)
              OR ST_DWITHIN(geography, ST_GEOGPOINT(-71.25, -29.95), 35000)
            )
        """

    pts_5k_sql = ", ".join([f"STRUCT({lat} AS lat, {lon} AS lon)" for lat, lon in presets])

    return f"""
    WITH grid_5km AS (
      SELECT
        init_time,
        CAST(ROUND((ST_Y(geography) + 90.0) * 10) AS INT64) AS i10,
        CAST(ROUND((ST_X(geography) + 360.0) * 10) AS INT64) AS j10,
        ROUND(ST_Y(geography), 2) AS lat,
        ROUND(ST_X(geography), 2) AS lon,
        f.hours AS lead_time_hours,
        f.time AS end_time,
        ROUND(f.station_head_temperature_2m_p50 - 273.15, 2) AS temp_5km_p50_c,
        ROUND(f.station_head_dewpoint_temperature_2m_p50 - 273.15, 2) AS dewpoint_5km_p50_c
      FROM `{cfg['project_id']}.{cfg['dataset']}.{cfg['table_0p05']}`,
      UNNEST(forecast) AS f
      WHERE init_time = TIMESTAMP('{init_time_str}')
        AND {filter_5km}
        AND STRUCT(ROUND(ST_Y(geography), 2) AS lat, ROUND(ST_X(geography), 2) AS lon) IN ({pts_5k_sql})
        AND f.hours <= {hours}
    ),
    grid_10km AS (
      SELECT
        init_time,
        CAST(ROUND((ST_Y(geography) + 90.0) * 10) AS INT64) AS i10,
        CAST(ROUND((ST_X(geography) + 360.0) * 10) AS INT64) AS j10,
        ROUND(ST_Y(geography), 1) AS lat_10km,
        ROUND(ST_X(geography), 1) AS lon_10km,
        f.hours AS lead_time_hours,
        ROUND(GREATEST(0.0, f.imerg_tp_1hr_mean) * 1000.0, 2) AS imerg_1h_mean_mm,
        ROUND(GREATEST(0.0, f.imerg_tp_1hr_p50) * 1000.0, 2) AS imerg_1h_p50_mm,
        ROUND(GREATEST(0.0, f.imerg_tp_1hr_p75) * 1000.0, 2) AS imerg_1h_p75_mm,
        ROUND(GREATEST(0.0, f.imerg_tp_1hr_p90) * 1000.0, 2) AS imerg_1h_p90_mm,
        ROUND(GREATEST(0.0, f.total_precipitation_1hr_mean) * 1000.0, 2) AS tp_1h_mean_mm,
        ROUND(GREATEST(0.0, f.total_precipitation_1hr_p50) * 1000.0, 2) AS tp_1h_p50_mm,
        ROUND(GREATEST(0.0, f.total_precipitation_1hr_p75) * 1000.0, 2) AS tp_1h_p75_mm,
        ROUND(GREATEST(0.0, f.total_precipitation_1hr_p90) * 1000.0, 2) AS tp_1h_p90_mm,
        ROUND(f.temperature_2m_p50 - 273.15, 2) AS temp_10km_p50_c,
        ROUND(f.wind_speed_100m_p90 * 3.6, 2) AS wind_100m_p90_kmh,
        ROUND(f.wind_speed_10m_p50 * 3.6, 2) AS wind_10m_p50_kmh,
        ROUND(f.wind_speed_10m_p90 * 3.6, 2) AS wind_10m_p90_kmh,
        ROUND(f.mean_sea_level_pressure_p50 / 100.0, 2) AS pressure_p50_hpa,
        ROUND(LEAST(100.0, GREATEST(0.0, f.total_cloud_cover_p50 * 100.0)), 1) AS tcc_p50_pct,
        ROUND(LEAST(100.0, GREATEST(0.0, f.low_cloud_cover_p50 * 100.0)), 1) AS lcc_p50_pct,
        ROUND(GREATEST(0.0, f.surface_solar_radiation_downwards_1hr_p50 / 3600.0), 1) AS ssrd_1h_p50_wm2,
        ROUND(GREATEST(0.0, f.total_sky_direct_solar_radiation_at_surface_1hr_p50 / 3600.0), 1) AS fdir_1h_p50_wm2,
        ROUND(f.dewpoint_temperature_2m_p50 - 273.15, 2) AS dewpoint_10km_p50_c
      FROM `{cfg['project_id']}.{cfg['dataset']}.{cfg['table_0p1']}`,
      UNNEST(forecast) AS f
      WHERE init_time = TIMESTAMP('{init_time_str}')
        AND {filter_10km}
        AND f.hours <= {hours}
    )
    SELECT
      g5.init_time,
      g5.end_time,
      g5.lead_time_hours,
      g5.lat,
      g5.lon,
      g10.lat_10km,
      g10.lon_10km,
      g10.imerg_1h_mean_mm,
      g10.imerg_1h_p50_mm,
      g10.imerg_1h_p75_mm,
      g10.imerg_1h_p90_mm,
      g10.tp_1h_mean_mm,
      g10.tp_1h_p50_mm,
      g10.tp_1h_p75_mm,
      g10.tp_1h_p90_mm,
      g5.temp_5km_p50_c,
      g10.temp_10km_p50_c,
      g10.wind_100m_p90_kmh,
      g10.wind_10m_p50_kmh,
      g10.wind_10m_p90_kmh,
      g10.pressure_p50_hpa,
      g10.tcc_p50_pct,
      g10.lcc_p50_pct,
      g10.ssrd_1h_p50_wm2,
      g10.fdir_1h_p50_wm2,
      COALESCE(g5.dewpoint_5km_p50_c, g10.dewpoint_10km_p50_c) AS dewpoint_2m_p50_c
    FROM grid_5km g5
    INNER JOIN grid_10km g10
      ON g5.init_time = g10.init_time
      AND g5.i10 = g10.i10
      AND g5.j10 = g10.j10
      AND g5.lead_time_hours = g10.lead_time_hours
    ORDER BY g5.lead_time_hours ASC, g5.lat DESC, g5.lon ASC
    """


def extract_weathernext3_bq(
    hours: int | None = None,
    is_rio: bool = False,
    config_path: str = "config.ini",
) -> str:
    """Extrae corrida de WeatherNext 3.0 para Chile o Brasil mediante BigQuery SQL columnar."""
    t_start = time.time()
    cfg = load_wn3_config(config_path)
    default_h = cfg["default_hours_rio"] if is_rio else cfg["default_hours_chile"]
    h = hours or default_h
    client = get_bigquery_client(config_path)

    init_dt, init_str = resolve_latest_wn3_init_time(client, cfg)
    region_label = "Brasil (Río & Ilha Grande)" if is_rio else "Chile"
    color = "bright_magenta" if is_rio else "cyan"
    console.print(f"[bold {color}]🚀 WeatherNext 3.0 BigQuery Extractor ({region_label}):[/bold {color}] Corrida [bold yellow]{init_str}[/bold yellow] ({h} horas)")

    sql = build_wn3_bigquery_sql(cfg, init_str, is_rio=is_rio, hours=h)
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=int(cfg["max_bytes_billed_mb"] * 1024 * 1024),
        use_query_cache=True,
    )
    query_job = client.query(sql, job_config=job_config)
    arrow_tbl = query_job.to_arrow()
    df = pl.from_arrow(arrow_tbl)

    query_type = "extraction_wn3_rio_bq" if is_rio else "extraction_wn3_bq"
    record_local_usage(
        job_id=query_job.job_id,
        bytes_billed=query_job.total_bytes_billed,
        bytes_processed=query_job.total_bytes_processed,
        query_type=query_type,
        timestamp=datetime.now(timezone.utc),
    )

    out_dir = Path("data")
    out_dir.mkdir(parents=True, exist_ok=True)
    ts_str = datetime.now().strftime("%Y%m%d-%H%M")
    prefix = "weathernext3_rio" if is_rio else "weathernext3"
    out_ts = out_dir / f"{prefix}_{ts_str}.parquet"
    out_latest = out_dir / f"{prefix}_latest.parquet"

    df.write_parquet(out_ts)
    df.write_parquet(out_latest)

    elapsed = time.time() - t_start
    billed_mb = (query_job.total_bytes_billed or 0) / (1024 * 1024)
    console.print(
        f"[bold green]✅ WeatherNext 3 ({region_label}) guardado con éxito:[/bold green] {len(df):,} filas en {elapsed:.1f}s | Facturado: [bold yellow]{billed_mb:.1f} MB[/bold yellow]"
    )
    return str(out_latest)


def extract_weathernext3_entry(
    hours: int | None = None,
    is_rio: bool = False,
    source: str | None = None,
    config_path: str = "config.ini",
) -> str:
    """Enrutador de extracción unificado que selecciona entre BigQuery SQL y fallback GCS Zarr v3."""
    cfg = load_wn3_config(config_path)
    chosen_source = (source or cfg["default_source"]).strip().lower()

    if chosen_source == "zarr":
        from zarr_wn3_fallback import extract_weathernext3, extract_weathernext3_rio
        if is_rio:
            return extract_weathernext3_rio(hours=hours or cfg["default_hours_rio"])
        else:
            return extract_weathernext3(hours=hours or cfg["default_hours_chile"])
    else:
        # Por defecto: BigQuery nativo (rápido, filtrado y con telemetría completa)
        try:
            return extract_weathernext3_bq(hours=hours, is_rio=is_rio, config_path=config_path)
        except Exception as e:
            console.print(f"[bold yellow]⚠️ Aviso:[/bold yellow] Extracción BigQuery falló ({e}). Recurriendo a fallback Zarr v3...")
            from zarr_wn3_fallback import extract_weathernext3, extract_weathernext3_rio
            if is_rio:
                return extract_weathernext3_rio(hours=hours or cfg["default_hours_rio"])
            else:
                return extract_weathernext3(hours=hours or cfg["default_hours_chile"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extractor de WeatherNext 3.0 (BigQuery / Zarr v3)")
    parser.add_argument("--hours", type=int, default=None, help="Horizonte horario (defecto: según config.ini)")
    parser.add_argument("--rio", "--brasil", "--vacaciones", dest="rio", action="store_true", help="Extraer pronóstico de Río de Janeiro & Ilha Grande")
    parser.add_argument("--source", choices=["bq", "zarr"], default=None, help="Motor de extracción: bq (BigQuery SQL) o zarr (GCS Zarr v3)")
    args = parser.parse_args()
    extract_weathernext3_entry(hours=args.hours, is_rio=args.rio, source=args.source)
