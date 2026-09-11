#!/usr/bin/env python3
"""
Medidor de Consumo y Facturación de Google Cloud BigQuery (Terminal CLI).
Muestra el consumo mensual actual con barra visual, historial de meses anteriores
y comparación paralela (Doble Entrada): Medidor Local vs. BigQuery Oficial en SQLite.
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from usage_tracker import (
    DB_DEFAULT_PATH,
    FREE_TIER_BYTES,
    get_monthly_stats,
    get_recent_jobs,
    sync_from_bigquery,
)


def render_progress_bar(percent: float, width: int = 32) -> str:
    """Genera una barra de progreso visual con caracteres de bloque."""
    filled_len = int(round(width * percent / 100))
    filled_len = max(0, min(width, filled_len))
    bar = "#" * filled_len + "-" * (width - filled_len)
    return f"[{bar}] {percent:5.2f}%"


def get_bigquery_client(config_path: str = "config.ini"):
    """Intenta inicializar el cliente BigQuery para sincronización."""
    import configparser
    from google.cloud import bigquery
    from google.oauth2 import service_account

    cfg = configparser.ConfigParser()
    cfg.read(config_path, encoding="utf-8")

    project_id = cfg.get("GCP", "project_id", fallback="").strip()
    creds_file = cfg.get("GCP", "credentials_file", fallback="").strip()

    if creds_file and Path(creds_file).exists():
        creds = service_account.Credentials.from_service_account_file(creds_file)
        return bigquery.Client(project=project_id, credentials=creds)
    elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return bigquery.Client(project=project_id)
    else:
        return bigquery.Client(project=project_id)


def print_dashboard(db_path: Path = DB_DEFAULT_PATH, sync_message: str = ""):
    now_utc = datetime.now(timezone.utc)
    current_period = now_utc.strftime("%Y-%m")

    stats = get_monthly_stats(db_path)
    current_stat = next((s for s in stats if s["period"] == current_period), None)
    history_stats = [s for s in stats if s["period"] != current_period]
    recent_jobs = get_recent_jobs(limit=6, db_path=db_path)

    term_width = 76
    border = "=" * term_width
    sub_border = "-" * term_width

    print(f"\n{border}")
    print(f" MEDIDOR DE GASTO Y CONSUMO GOOGLE CLOUD (BigQuery Engine)")
    print(f"{border}")

    if sync_message:
        print(f" [SYNC] {sync_message}")
        print(f"{sub_border}")

    print(f" Periodo Actual:   {current_period} (Fecha UTC: {now_utc.strftime('%Y-%m-%d %H:%M')})")
    print(f" Cuota Free Tier:  1,000.00 GB / mes (1 TB a costo $0.00 USD)")

    if current_stat:
        loc_gb = current_stat["local_gb"]
        bq_gb = current_stat["bq_gb"]
        pct = current_stat["free_tier_percent"]
        cost = current_stat["cost_usd"]
        rem_gb = current_stat["remaining_free_gb"]
        diff_mb = current_stat["diff_mb"]
        queries = current_stat["total_queries"]

        print(f"\n Progreso Mensual:")
        print(f"    {render_progress_bar(pct, width=36)}")
        print(f"    * Cuota libre restante: {rem_gb:.2f} GB disponibles este mes")
        print(f"    * Costo estimado:      ${cost:.2f} USD")

        print(f"\n Comparativa Paralela del Mes en Curso ({current_period}):")
        print(f"    +------------------------+----------------+----------------+")
        print(f"    | Metrica                | Medidor Local  | BigQuery Oficial|")
        print(f"    +------------------------+----------------+----------------+")
        print(f"    | Consultas Registradas  | {queries:14d} | {queries:14d} |")
        print(f"    | Volumen Facturado (GB) | {loc_gb:11.4f} GB | {bq_gb:11.4f} GB |")
        print(f"    | Volumen Facturado (MB) | {loc_gb*1024:11.1f} MB | {bq_gb*1024:11.1f} MB |")
        print(f"    +------------------------+----------------+----------------+")

        if abs(diff_mb) < 0.01:
            print(f"    [OK] Estado de Conciliacion: 100% Sincronizado y Coincidente (Delta: 0.00 MB)")
        else:
            status_text = "Local mayor que BQ" if diff_mb > 0 else "BQ mayor que Local"
            print(f"    [INFO] Estado de Conciliacion: Discrepancia de {abs(diff_mb):.2f} MB ({status_text})")
    else:
        print(f"\n Progreso Mensual:")
        print(f"    {render_progress_bar(0.0, width=36)}")
        print(f"    * Sin consultas registradas para el periodo {current_period}.")

    # --- HISTORIAL DE PERIODOS ANTERIORES ---
    print(f"\n{sub_border}")
    print(f" HISTORIAL DE PERIODOS ANTERIORES (Meses Pasados)")
    print(f"{sub_border}")

    if history_stats:
        print(f" +----------+-----------+--------------+--------------+----------+------------+")
        print(f" | Periodo  | Consultas | Local (GB)   | BigQuery(GB) | Delta MB | Costo USD  |")
        print(f" +----------+-----------+--------------+--------------+----------+------------+")
        for h in history_stats:
            p = h["period"]
            q = h["total_queries"]
            l_gb = h["local_gb"]
            b_gb = h["bq_gb"]
            d_mb = h["diff_mb"]
            c = h["cost_usd"]
            print(f" | {p:8} | {q:9d} | {l_gb:10.4f} GB | {b_gb:10.4f} GB | {d_mb:8.1f} | ${c:8.2f}  |")
        print(f" +----------+-----------+--------------+--------------+----------+------------+")
    else:
        print(f"  (Sin periodos anteriores cerrados en el historial)")

    # --- DETALLE DE ULTIMAS CONSULTAS ---
    if recent_jobs:
        print(f"\n{sub_border}")
        print(f" ULTIMAS CONSULTAS REGISTRADAS (Cotejo Uno a Uno)")
        print(f"{sub_border}")
        print(f" +--------------------+----------+--------------+--------------+-------------+")
        print(f" | Fecha / Hora (UTC) | Job ID   | Local (MB)   | BigQuery(MB) | Match       |")
        print(f" +--------------------+----------+--------------+--------------+-------------+")
        for j in recent_jobs:
            ts = j["timestamp"][:19].replace("T", " ")
            jid = j["job_id"][:8] + "..." if len(j["job_id"]) > 8 else j["job_id"]
            l_mb = f"{j['local_mb']:10.1f} MB" if j["local_mb"] is not None else "         - "
            b_mb = f"{j['bq_mb']:10.1f} MB" if j["bq_mb"] is not None else "         - "
            
            match_str = "Pendiente"
            if j["match_status"] == "MATCH":
                match_str = "Exacto"
            elif j["match_status"] == "DIFF":
                match_str = "Delta"
            elif j["match_status"] == "ONLY_BQ":
                match_str = "Solo BQ"
            elif j["match_status"] == "ONLY_LOCAL":
                match_str = "Solo Loc"

            print(f" | {ts:18} | {jid:8} | {l_mb} | {b_mb} | {match_str:11} |")
        print(f" +--------------------+----------+--------------+--------------+-------------+")

    print(f"\n Base de datos local: {db_path.resolve()}")
    print(f"{border}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Medidor de consumo y facturacion de Google Cloud BigQuery con doble entrada."
    )
    parser.add_argument(
        "--sync",
        "-s",
        action="store_true",
        default=True,
        help="Sincroniza con BigQuery INFORMATION_SCHEMA antes de mostrar (por defecto: True).",
    )
    parser.add_argument(
        "--no-sync",
        action="store_true",
        help="Modo offline estricto: lee unicamente la base de datos local SQLite sin conectar a Google Cloud.",
    )
    parser.add_argument(
        "--config",
        "-c",
        default="config.ini",
        help="Ruta al archivo config.ini (default: config.ini).",
    )
    parser.add_argument(
        "--db-path",
        default=str(DB_DEFAULT_PATH),
        help="Ruta a la base de datos SQLite (default: data/usage.db).",
    )

    args = parser.parse_args()
    db_path = Path(args.db_path)

    sync_msg = ""
    should_sync = args.sync and not args.no_sync

    if should_sync:
        try:
            client = get_bigquery_client(args.config)
            count = sync_from_bigquery(client, db_path=db_path)
            sync_msg = f"Sincronizacion con Google Cloud exitosa: {count} consultas conciliadas desde INFORMATION_SCHEMA."
        except Exception as e:
            sync_msg = f"Aviso de sincronizacion: No se pudo conectar a BigQuery ({e}). Mostrando datos locales offline."

    print_dashboard(db_path=db_path, sync_message=sync_msg)


if __name__ == "__main__":
    main()
