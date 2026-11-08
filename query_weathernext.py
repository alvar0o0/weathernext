import configparser
import os
import socket
import sys
from datetime import datetime
from pathlib import Path

# Ensure IPv4 resolution to prevent IPv6 blackhole stalls on GCP endpoints
_orig_getaddrinfo = socket.getaddrinfo
def _getaddrinfo_ipv4(host, port, family=0, type=0, proto=0, flags=0):
    return _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
socket.getaddrinfo = _getaddrinfo_ipv4

import polars as pl
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPIError

from usage_tracker import get_bigquery_client


def load_config(config_path: str = "config.ini") -> dict:
    """Lee y valida el archivo de configuración .config / .ini con parámetros de seguridad y almacenamiento."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el archivo de configuración en: {path.resolve()}")

    config = configparser.ConfigParser()
    config.read(path, encoding="utf-8")

    if "GCP" not in config or "LOCATION" not in config:
        raise ValueError("El archivo config.ini debe contener al menos las secciones [GCP] y [LOCATION].")

    project_id = os.environ.get("GCP_PROJECT_ID") or config.get("GCP", "project_id", fallback="").strip()
    if not project_id or project_id in ("tu-proyecto-gcp-id", "TU_GCP_PROJECT_ID"):
        print(
            "⚠️ ADVERTENCIA: Configura tu ID de proyecto GCP en config.ini o mediante la variable de entorno GCP_PROJECT_ID.",
            file=sys.stderr,
        )

    credentials_file = config.get("GCP", "credentials_file", fallback="").strip()
    public_project = config.get("GCP", "public_project", fallback=project_id).strip()
    dataset = config.get("GCP", "dataset", fallback="weathernext_2").strip()
    table_name = config.get("GCP", "table_name", fallback="weathernext_2_0_0").strip()
    raw_full_table_id = config.get("GCP", "full_table_id", fallback="").strip()

    if raw_full_table_id:
        full_table_id = raw_full_table_id.format(
            public_project=public_project,
            dataset=dataset,
            table_name=table_name,
        )
    elif dataset and table_name:
        full_table_id = f"{public_project}.{dataset}.{table_name}"
    else:
        full_table_id = f"{public_project}.weathernext_2.weathernext_2_0_0"

    try:
        latitude = config.getfloat("LOCATION", "latitude")
        longitude = config.getfloat("LOCATION", "longitude")
        radius_meters = config.getfloat("LOCATION", "radius_meters", fallback=25000)
    except ValueError as e:
        raise ValueError(f"Error al leer parámetros de ubicación desde {config_path}: {e}")

    # Seguridad y Costos
    max_bytes_billed_mb = config.getfloat("SAFETY", "max_bytes_billed_mb", fallback=750000.0)
    dry_run_only = config.getboolean("SAFETY", "dry_run_only", fallback=False)

    # Almacenamiento Local (Parquet)
    output_dir = config.get("STORAGE", "output_dir", fallback="data").strip()
    file_prefix = config.get("STORAGE", "file_prefix", fallback="weathernext").strip()
    auto_save_parquet = config.getboolean("STORAGE", "auto_save_parquet", fallback=True)

    # Consulta
    init_time = config.get("QUERY", "init_time", fallback="").strip()
    unnest_forecast = config.getboolean("QUERY", "unnest_forecast", fallback=True)
    limit = config.getint("QUERY", "limit", fallback=100)

    return {
        "project_id": project_id,
        "credentials_file": credentials_file,
        "public_project": public_project,
        "dataset": dataset,
        "table_name": table_name,
        "full_table_id": full_table_id,
        "latitude": latitude,
        "longitude": longitude,
        "radius_meters": radius_meters,
        "max_bytes_billed_mb": max_bytes_billed_mb,
        "dry_run_only": dry_run_only,
        "output_dir": output_dir,
        "file_prefix": file_prefix,
        "auto_save_parquet": auto_save_parquet,
        "init_time": init_time,
        "unnest_forecast": unnest_forecast,
        "limit": limit,
    }


def save_dataframe_to_parquet(df: pl.DataFrame, cfg: dict) -> Path:
    """Guarda el DataFrame de Polars en un archivo Parquet con versión timestamp YYYYMMDD-HHMM."""
    out_dir = Path(cfg["output_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp_str = datetime.now().strftime("%Y%m%d-%H%M")
    filename = f"{cfg['file_prefix']}_{timestamp_str}.parquet"
    target_path = out_dir / filename

    # Guardar archivo versionado
    df.write_parquet(target_path)
    print(f"[STORAGE] Snapshot Parquet guardado con exito:")
    print(f"   * Archivo versionado: {target_path.resolve()} ({len(df)} filas, {target_path.stat().st_size / 1024:.1f} KB)")

    # Crear/actualizar copia de conveniencia 'latest'
    latest_path = out_dir / f"{cfg['file_prefix']}_latest.parquet"
    df.write_parquet(latest_path)
    print(f"   * Copia de trabajo:   {latest_path.resolve()}")

    return target_path


def build_sql_query(cfg: dict) -> str:
    """Construye la consulta SQL geoespacial optimizada con predicados nativos."""
    where_clauses = [
        f"ST_DWITHIN(geography, ST_GEOGPOINT({cfg['longitude']}, {cfg['latitude']}), {cfg['radius_meters']})"
    ]

    if cfg["init_time"]:
        where_clauses.append(f"init_time = TIMESTAMP('{cfg['init_time']}')")

    where_sql = " AND ".join(where_clauses)

    if cfg["unnest_forecast"]:
        return f"""
            SELECT 
                init_time,
                ST_Y(geography) AS latitude,
                ST_X(geography) AS longitude,
                ST_DISTANCE(geography, ST_GEOGPOINT({cfg['longitude']}, {cfg['latitude']})) AS distance_meters,
                f.time AS forecast_time,
                f.hours AS forecast_hours,
                ens.ensemble_member,
                ens.`2m_temperature` AS temp_2m_k,
                ROUND(ens.`2m_temperature` - 273.15, 2) AS temp_2m_celsius,
                ens.`10m_u_component_of_wind` AS wind_u_10m,
                ens.`10m_v_component_of_wind` AS wind_v_10m,
                ROUND(SQRT(POW(ens.`10m_u_component_of_wind`, 2) + POW(ens.`10m_v_component_of_wind`, 2)) * 3.6, 2) AS wind_speed_kmh,
                ens.`100m_u_component_of_wind` AS wind_u_100m,
                ens.`100m_v_component_of_wind` AS wind_v_100m,
                ROUND(SQRT(POW(ens.`100m_u_component_of_wind`, 2) + POW(ens.`100m_v_component_of_wind`, 2)) * 3.6, 2) AS wind_speed_100m_kmh,
                ens.mean_sea_level_pressure AS pressure_msl,
                ROUND(ens.mean_sea_level_pressure / 100.0, 2) AS pressure_hpa,
                ens.total_precipitation_6hr AS precip_6h,
                ROUND(GREATEST(0.0, ens.total_precipitation_6hr) * 1000.0, 2) AS precip_6h_mm
            FROM `{cfg['full_table_id']}`,
            UNNEST(forecast) AS f,
            UNNEST(f.ensemble) AS ens
            WHERE {where_sql}
            ORDER BY distance_meters ASC, forecast_hours ASC
            LIMIT {cfg['limit']}
        """
    else:
        return f"""
            SELECT 
                init_time,
                ST_Y(geography) AS latitude,
                ST_X(geography) AS longitude,
                ST_DISTANCE(geography, ST_GEOGPOINT({cfg['longitude']}, {cfg['latitude']})) AS distance_meters,
                forecast
            FROM `{cfg['full_table_id']}`
            WHERE {where_sql}
            ORDER BY distance_meters ASC
            LIMIT {cfg['limit']}
        """


def resolve_latest_init_time(client: bigquery.Client, cfg: dict) -> str:
    """Detecta automáticamente la corrida más reciente de WeatherNext en BigQuery."""
    print("   * [AUTO-RESOLVE] Detectando corrida mas reciente en BigQuery...")
    parts_query = f"""
        SELECT partition_id
        FROM `{cfg['public_project']}.{cfg['dataset']}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_name = '{cfg['table_name']}'
          AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
        ORDER BY partition_id DESC
        LIMIT 1
    """
    job = client.query(parts_query)
    rows = list(job)
    if not rows:
        raise ValueError("No se encontraron particiones activas en BigQuery.")
    latest_part = rows[0].partition_id
    date_str = f"{latest_part[:4]}-{latest_part[4:6]}-{latest_part[6:8]}"

    init_query = f"""
        SELECT MAX(init_time) as max_init
        FROM `{cfg['full_table_id']}`
        WHERE DATE(init_time) = '{date_str}'
    """
    job2 = client.query(init_query)
    rows2 = list(job2)
    max_init = rows2[0].max_init
    if not max_init:
        raise ValueError(f"No se encontró un init_time válido para la partición {date_str}.")

    formatted_init = max_init.strftime("%Y-%m-%d %H:%M:%S+00")
    print(f"   * [AUTO-RESOLVE] Corrida mas reciente encontrada: {formatted_init}")
    return formatted_init


def fetch_weather_data(config_path: str = "config.ini") -> pl.DataFrame:
    """
    Consulta BigQuery con protección de costos (dry_run + maximum_bytes_billed),
    transforma los datos vía Apache Arrow a Polars y persiste el snapshot en Parquet.
    """
    cfg = load_config(config_path)

    print(f"[CONFIG] Parametros de Consulta:")
    print(f"   * Proyecto Billing GCP: {cfg['project_id']}")
    print(f"   * Tabla BigQuery:      {cfg['full_table_id']}")
    print(f"   * Coordenadas:          Latitud {cfg['latitude']}, Longitud {cfg['longitude']} (Radio: {cfg['radius_meters']}m)")
    print(f"   * Limite de seguridad:  {cfg['max_bytes_billed_mb']:.0f} MB proyectados maximos")

    client = get_bigquery_client(config_path)
    print("   * Autenticacion:        BigQuery Client Inicializado (ADC / SA)")

    # Detección automática de la última corrida si init_time es 'latest' o está vacío
    if cfg["init_time"].lower() in ("", "latest", "auto"):
        cfg["init_time"] = resolve_latest_init_time(client, cfg)
    
    print(f"   * Particion init_time:  {cfg['init_time']}")

    query = build_sql_query(cfg)

    # --- PASO 1: SIMULACIÓN PREVENTIVA (DRY RUN a $0) ---
    print("\n[PASO 1] Simulacion de Costos (Dry Run):")
    dry_run_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    dry_run_job = client.query(query, job_config=dry_run_config)
    estimated_mb = dry_run_job.total_bytes_processed / (1024 * 1024)
    print(f"   * Proyeccion de particion (sin clustering): {estimated_mb:.2f} MB (~{estimated_mb/1024:.2f} GB)")
    print(f"   * Escaneo real esperado tras clustering espacial: ~300 - 400 MB")
    print(f"   * Costo estimado: $0.00 USD (dentro del tier gratuito de 1 TB/mes)")

    if estimated_mb > cfg["max_bytes_billed_mb"]:
        raise ValueError(
            f"[SEGURIDAD] La consulta proyecta {estimated_mb:.2f} MB, superando el limite configurado de {cfg['max_bytes_billed_mb']} MB. Operacion cancelada a $0 costo."
        )

    if cfg["dry_run_only"]:
        print("\n[INFO] Modo 'dry_run_only = true' activado. Finalizando simulacion sin descargar datos.")
        return pl.DataFrame()

    # --- PASO 2: EJECUCIÓN SEGURA CON INTERRUPTOR DE CORTE (maximum_bytes_billed) ---
    max_allowed_bytes = int(cfg["max_bytes_billed_mb"] * 1024 * 1024)
    exec_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=max_allowed_bytes,
        use_query_cache=True,
    )

    try:
        print("\n[PASO 2] Extraccion desde BigQuery:")
        query_job = client.query(query, job_config=exec_config)

        # Streaming de datos columnar vía Apache Arrow (Zero-Copy) -> Polars
        arrow_table = query_job.to_arrow()
        df = pl.from_arrow(arrow_table)

        actual_mb = (query_job.total_bytes_processed or 0) / (1024 * 1024)
        billed_mb = (query_job.total_bytes_billed or 0) / (1024 * 1024)
        print(f"[OK] Consulta completada exitosamente.")
        print(f"   * Escaneo real reportado: {actual_mb:.2f} MB (Facturado: {billed_mb:.2f} MB)")
        print(f"   * Filas recibidas:        {len(df)}")

        # --- REGISTRO EN MEDIDOR LOCAL (SQLite) ---
        try:
            from usage_tracker import record_local_usage
            record_local_usage(
                job_id=query_job.job_id,
                bytes_billed=query_job.total_bytes_billed,
                bytes_processed=query_job.total_bytes_processed,
                query_type="weathernext_extraction",
            )
            print(f"   * Medidor de consumo:     Job {query_job.job_id[:8]} registrado en SQLite (data/usage.db)")
        except Exception as tracker_err:
            print(f"   [AVISO] Medidor de consumo: {tracker_err}", file=sys.stderr)

        # --- PASO 3: PERSISTENCIA LOCAL EN PARQUET ---
        if cfg["auto_save_parquet"] and not df.is_empty():
            print("\n[PASO 3] Persistencia Local en Parquet:")
            save_dataframe_to_parquet(df, cfg)

        return df

    except GoogleAPIError as err:
        print(f"[ERROR] Error al consultar BigQuery: {err}", file=sys.stderr)
        raise
    except Exception as err:
        print(f"[ERROR] Error inesperado en el pipeline: {err}", file=sys.stderr)
        raise


def main():
    config_file = sys.argv[1] if len(sys.argv) > 1 else "config.ini"
    try:
        df = fetch_weather_data(config_file)
        if not df.is_empty():
            print("\n--- Vista Previa de los Datos (Polars DataFrame) ---")
            print(df.head(10))
            print(f"\nDimensiones: {df.shape[0]} filas x {df.shape[1]} columnas")
    except Exception as e:
        print(f"\nProceso finalizado con error: {e}")


if __name__ == "__main__":
    main()

