import configparser
import sys
from pathlib import Path

import polars as pl
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError


def load_config(config_path: str = "config.ini") -> dict:
    """Lee y valida el archivo de configuración .config / .ini"""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el archivo de configuración en: {path.resolve()}")

    config = configparser.ConfigParser()
    config.read(path, encoding="utf-8")

    if "GCP" not in config or "LOCATION" not in config:
        raise ValueError("El archivo .config debe contener las secciones [GCP] y [LOCATION].")

    project_id = config.get("GCP", "project_id", fallback="").strip()
    if not project_id or project_id == "tu-proyecto-gcp-id":
        print(
            "⚠️ ADVERTENCIA: Reemplaza 'tu-proyecto-gcp-id' en config.ini con tu ID de proyecto GCP real.",
            file=sys.stderr,
        )

    public_project = config.get("GCP", "public_project", fallback="gcp-public-data-weathernext").strip()
    dataset = config.get("GCP", "dataset", fallback="").strip()
    table_name = config.get("GCP", "table_name", fallback="").strip()
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
        full_table_id = f"{public_project}.dataset_name.table_name"

    try:
        latitude = config.getfloat("LOCATION", "latitude")
        longitude = config.getfloat("LOCATION", "longitude")
        radius_meters = config.getfloat("LOCATION", "radius_meters", fallback=25000)
    except ValueError as e:
        raise ValueError(f"Error al leer parámetros de ubicación desde {config_path}: {e}")

    init_time = config.get("QUERY", "init_time", fallback="").strip()
    unnest_forecast = config.getboolean("QUERY", "unnest_forecast", fallback=True)
    limit = config.getint("QUERY", "limit", fallback=100)

    return {
        "project_id": project_id,
        "full_table_id": full_table_id,
        "latitude": latitude,
        "longitude": longitude,
        "radius_meters": radius_meters,
        "init_time": init_time,
        "unnest_forecast": unnest_forecast,
        "limit": limit,
    }


def fetch_weather_data(config_path: str = "config.ini") -> pl.DataFrame:
    """
    Consulta la tabla WeatherNext en BigQuery filtrando espacialmente por coordenadas GEOGRAPHY,
    desanidando estructuras de pronóstico si se requiere, y retorna un DataFrame de Polars.
    """
    cfg = load_config(config_path)

    print(f"📌 Conectando a BigQuery...")
    print(f"   • Proyecto GCP Billing: {cfg['project_id']}")
    print(f"   • Tabla: {cfg['full_table_id']}")
    print(f"   • Ubicación objetivo: Latitud {cfg['latitude']}, Longitud {cfg['longitude']}")
    print(f"   • Radio de búsqueda: {cfg['radius_meters']} metros")
    if cfg["init_time"]:
        print(f"   • Filtro init_time: {cfg['init_time']}")

    client = bigquery.Client(project=cfg["project_id"])

    # Construir condiciones WHERE con literales para permitir la optimización de particionado y clustering espacial en BigQuery
    where_clauses = [
        f"ST_DWITHIN(geography, ST_GEOGPOINT({cfg['longitude']}, {cfg['latitude']}), {cfg['radius_meters']})"
    ]

    if cfg["init_time"]:
        where_clauses.append(f"init_time = TIMESTAMP('{cfg['init_time']}')")

    where_sql = " AND ".join(where_clauses)

    if cfg["unnest_forecast"]:
        query = f"""
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
                ens.mean_sea_level_pressure AS pressure_msl,
                ens.total_precipitation_6hr AS precip_6h
            FROM `{cfg['full_table_id']}`,
            UNNEST(forecast) AS f,
            UNNEST(f.ensemble) AS ens
            WHERE {where_sql}
            ORDER BY distance_meters ASC, forecast_hours ASC
            LIMIT {cfg['limit']}
        """
    else:
        query = f"""
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

    job_config = bigquery.QueryJobConfig()

    try:
        print("⏳ Ejecutando consulta SQL espacial en BigQuery...")
        query_job = client.query(query, job_config=job_config)

        arrow_table = query_job.to_arrow()
        df = pl.from_arrow(arrow_table)

        print(f"✅ Consulta completada. Filas obtenidas: {len(df)}")
        return df

    except GoogleAPIError as err:
        print(f"❌ Error al consultar BigQuery: {err}", file=sys.stderr)
        raise
    except Exception as err:
        print(f"❌ Error inesperado: {err}", file=sys.stderr)
        raise


def main():
    config_file = sys.argv[1] if len(sys.argv) > 1 else "config.ini"
    try:
        df = fetch_weather_data(config_file)
        print("\n--- Vista previa de los datos en Polars ---")
        print(df)
    except Exception as e:
        print(f"\nProceso finalizado con error: {e}")


if __name__ == "__main__":
    main()
