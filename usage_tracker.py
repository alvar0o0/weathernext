"""
Módulo de registro y conciliación de consumo de Google Cloud BigQuery.
Implementa contabilidad paralela (doble entrada) en SQLite:
  - Registro local inmediato tras cada query en Python
  - Registro oficial sincronizado desde INFORMATION_SCHEMA de BigQuery
"""

import configparser
from datetime import datetime, timezone
import os
from pathlib import Path
import socket
import sqlite3
from typing import Any, Dict, List, Optional

# Ensure IPv4 resolution to prevent IPv6 blackhole stalls on GCP endpoints
_orig_getaddrinfo = socket.getaddrinfo
def _getaddrinfo_ipv4(host, port, family=0, type=0, proto=0, flags=0):
    return _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
socket.getaddrinfo = _getaddrinfo_ipv4

import google.auth
from google.cloud import bigquery
from google.oauth2 import service_account
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

DB_DEFAULT_PATH = Path("data/usage.db")
FREE_TIER_BYTES = 1000 * 1024 * 1024 * 1024  # 1 TB = 1,000 GB
COST_PER_TB_USD = 6.25


def get_bigquery_client(config_path: str = "config.ini") -> bigquery.Client:
    """
    Inicializa el cliente de BigQuery con adaptador HTTP resiliente (retries TLS/SSL).
    Prioriza Application Default Credentials (ADC) si credentials_file está vacío o ausente.
    Si credentials_file está configurado y existe, lo intenta; si falla con error de permisos,
    hace fallback inmediato a ADC.
    """
    cfg = configparser.ConfigParser()
    path = Path(config_path)
    project_id = os.environ.get("GCP_PROJECT_ID", "")
    creds_file = ""

    if path.exists():
        cfg.read(path, encoding="utf-8")
        if not project_id:
            project_id = cfg.get("GCP", "project_id", fallback="").strip()
        creds_file = cfg.get("GCP", "credentials_file", fallback="").strip()

    client = None
    if creds_file and Path(creds_file).exists():
        try:
            creds = service_account.Credentials.from_service_account_file(creds_file)
            test_client = bigquery.Client(project=project_id, credentials=creds)
            # Test rápido de lectura mínima
            test_client.query("SELECT 1 FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT LIMIT 1").result()
            client = test_client
        except Exception:
            client = None

    if client is None:
        creds, _ = google.auth.default()
        client = bigquery.Client(project=project_id, credentials=creds)

    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    client._http.mount("https://", adapter)
    return client


def get_db_connection(db_path: Path = DB_DEFAULT_PATH) -> sqlite3.Connection:
    """Asegura la creación del directorio y conecta a la base de datos SQLite."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    _init_db(conn)
    return conn


def _init_db(conn: sqlite3.Connection):
    """Crea la tabla de consultas con columnas paralelas si no existe."""
    with conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS query_usage (
                job_id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                period TEXT NOT NULL,
                query_type TEXT DEFAULT 'query',
                local_bytes_billed INTEGER,
                local_bytes_processed INTEGER,
                bq_bytes_billed INTEGER,
                bq_bytes_processed INTEGER,
                bq_status TEXT,
                synced_at TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_query_usage_period ON query_usage(period)"
        )


def record_local_usage(
    job_id: str,
    bytes_billed: Optional[int],
    bytes_processed: Optional[int],
    query_type: str = "extraction",
    timestamp: Optional[datetime] = None,
    db_path: Path = DB_DEFAULT_PATH,
):
    """Registra la consulta en el medidor local inmediatamente al ejecutarse."""
    ts = timestamp or datetime.now(timezone.utc)
    ts_iso = ts.isoformat()
    period = ts.strftime("%Y-%m")

    billed = bytes_billed if bytes_billed is not None else 0
    processed = bytes_processed if bytes_processed is not None else 0

    conn = get_db_connection(db_path)
    with conn:
        conn.execute(
            """
            INSERT INTO query_usage (
                job_id, timestamp, period, query_type,
                local_bytes_billed, local_bytes_processed
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                local_bytes_billed = excluded.local_bytes_billed,
                local_bytes_processed = excluded.local_bytes_processed,
                query_type = excluded.query_type
            """,
            (job_id, ts_iso, period, query_type, billed, processed),
        )
    conn.close()


def sync_from_bigquery(client, db_path: Path = DB_DEFAULT_PATH) -> int:
    """
    Sincroniza el registro oficial de BigQuery consultando INFORMATION_SCHEMA.JOBS_BY_PROJECT
    (con fallback a JOBS_BY_USER si no hay permisos de proyecto) a costo 0 bytes y actualiza
    las columnas de BigQuery en la base de datos local.
    Retorna la cantidad de jobs sincronizados.
    """
    sql_project = """
        SELECT 
            job_id,
            creation_time,
            total_bytes_billed,
            total_bytes_processed,
            state
        FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE job_type = 'QUERY'
        ORDER BY creation_time DESC
        LIMIT 1000
    """
    sql_user = """
        SELECT 
            job_id,
            creation_time,
            total_bytes_billed,
            total_bytes_processed,
            state
        FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_USER
        WHERE job_type = 'QUERY'
        ORDER BY creation_time DESC
        LIMIT 1000
    """
    try:
        query_job = client.query(sql_project)
        rows = list(query_job.result())
    except Exception:
        query_job = client.query(sql_user)
        rows = list(query_job.result())

    conn = get_db_connection(db_path)
    now_iso = datetime.now(timezone.utc).isoformat()
    synced_count = 0

    with conn:
        for r in rows:
            jid = r.job_id
            created_at = r.creation_time
            if created_at is None:
                continue

            ts_iso = created_at.isoformat()
            period = created_at.strftime("%Y-%m")
            bq_billed = r.total_bytes_billed if r.total_bytes_billed is not None else 0
            bq_processed = r.total_bytes_processed if r.total_bytes_processed is not None else 0
            state = r.state or "UNKNOWN"

            conn.execute(
                """
                INSERT INTO query_usage (
                    job_id, timestamp, period, query_type,
                    bq_bytes_billed, bq_bytes_processed, bq_status, synced_at
                ) VALUES (?, ?, ?, 'cloud_job', ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    bq_bytes_billed = excluded.bq_bytes_billed,
                    bq_bytes_processed = excluded.bq_bytes_processed,
                    bq_status = excluded.bq_status,
                    synced_at = excluded.synced_at
                """,
                (jid, ts_iso, period, bq_billed, bq_processed, state, now_iso),
            )
            synced_count += 1

    conn.close()
    return synced_count


def get_monthly_stats(db_path: Path = DB_DEFAULT_PATH) -> List[Dict[str, Any]]:
    """Obtiene agregaciones mensuales comparando Local vs BigQuery."""
    conn = get_db_connection(db_path)
    with conn:
        rows = conn.execute(
            """
            SELECT 
                period,
                COUNT(*) AS total_queries,
                SUM(COALESCE(local_bytes_billed, 0)) AS sum_local_billed,
                SUM(COALESCE(bq_bytes_billed, 0)) AS sum_bq_billed,
                SUM(COALESCE(local_bytes_processed, 0)) AS sum_local_proc,
                SUM(COALESCE(bq_bytes_processed, 0)) AS sum_bq_proc,
                COUNT(CASE WHEN local_bytes_billed IS NOT NULL AND bq_bytes_billed IS NOT NULL THEN 1 END) AS verified_queries
            FROM query_usage
            GROUP BY period
            ORDER BY period DESC
            """
        ).fetchall()
    conn.close()

    results = []
    for r in rows:
        local_bytes = r["sum_local_billed"]
        bq_bytes = r["sum_bq_billed"]
        diff_bytes = local_bytes - bq_bytes

        # El consumo oficial prima para el cálculo de costo
        effective_bytes = bq_bytes if bq_bytes > 0 else local_bytes
        excess_bytes = max(0, effective_bytes - FREE_TIER_BYTES)
        cost_usd = (excess_bytes / (1024**4)) * COST_PER_TB_USD

        results.append(
            {
                "period": r["period"],
                "total_queries": r["total_queries"],
                "local_bytes": local_bytes,
                "local_gb": local_bytes / (1024**3),
                "bq_bytes": bq_bytes,
                "bq_gb": bq_bytes / (1024**3),
                "diff_bytes": diff_bytes,
                "diff_mb": diff_bytes / (1024**2),
                "cost_usd": cost_usd,
                "free_tier_percent": min(100.0, (effective_bytes / FREE_TIER_BYTES) * 100),
                "remaining_free_gb": max(0.0, (FREE_TIER_BYTES - effective_bytes) / (1024**3)),
            }
        )
    return results


def get_recent_jobs(limit: int = 10, db_path: Path = DB_DEFAULT_PATH) -> List[Dict[str, Any]]:
    """Retorna las consultas más recientes para cotejo visual uno a uno."""
    conn = get_db_connection(db_path)
    with conn:
        rows = conn.execute(
            """
            SELECT 
                job_id,
                timestamp,
                period,
                query_type,
                local_bytes_billed,
                bq_bytes_billed,
                bq_status,
                synced_at
            FROM query_usage
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    conn.close()

    results = []
    for r in rows:
        loc = r["local_bytes_billed"]
        bq = r["bq_bytes_billed"]
        match_status = "PENDING"
        if loc is not None and bq is not None:
            match_status = "MATCH" if loc == bq else "DIFF"
        elif loc is None and bq is not None:
            match_status = "ONLY_BQ"
        elif loc is not None and bq is None:
            match_status = "ONLY_LOCAL"

        results.append(
            {
                "job_id": r["job_id"],
                "timestamp": r["timestamp"],
                "period": r["period"],
                "query_type": r["query_type"],
                "local_mb": (loc / (1024**2)) if loc is not None else None,
                "bq_mb": (bq / (1024**2)) if bq is not None else None,
                "match_status": match_status,
                "bq_status": r["bq_status"],
            }
        )
    return results
