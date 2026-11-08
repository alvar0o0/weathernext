"""
WeatherNext 3 (0.1° / 10 km & 0.05° / 5 km, 1-hour step, 250-member Statistics)
Direct GCS Zarr v3 Extractor with HTTP Range + libzstd Streaming Decompression.

Bypasses the BigQuery Analytics Hub 1,000-subscriber limit on 866962084172:WeatherNext_3
by reading directly from gs://weathernext3_statistics_spatial (non-requester-pays, $0.00 cost)
and decompressing only the Southern Hemisphere prefix up to lat -26.9° (covering Chile).
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import ctypes
from datetime import datetime, timedelta, timezone
import json
import os
import socket
import time
import urllib.parse
import urllib.request

# Ensure IPv4 resolution to prevent IPv6 blackhole stalls on GCP endpoints
_orig_getaddrinfo = socket.getaddrinfo
def _getaddrinfo_ipv4(host, port, family=0, type=0, proto=0, flags=0):
    return _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
socket.getaddrinfo = _getaddrinfo_ipv4

import google.auth
from google.auth.transport.requests import Request
import numpy as np
import pandas as pd
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
import urllib3

BUCKET = "weathernext3_statistics_spatial"
BASE_PREFIX = "weathernext_3_0_0_statistics/zarr/2026_to_present"

# Chile Target Bounding Box:
# 0.1° grid: lat_0p1[-90.0 .. +90.0], row = round((lat + 90.0) * 10)
#   lat -36.5° -> row 535, lat -27.0° -> row 630 (96 rows)
#   lon -72.0° (288.0°) -> col 2880, lon -69.5° (290.5°) -> col 2905 (26 cols)
LAT_0P1_START = 535
LAT_0P1_END = 632  # inclusive of -26.9°
LON_0P1_START = 2880
LON_0P1_END = 2906

# 0.05° grid: lat_0p05[-90.0 .. +90.0], row = round((lat + 90.0) * 20)
#   lat -36.5° -> row 1070, lat -27.0° -> row 1260 (191 rows)
#   lon -72.0° (288.0°) -> col 5760, lon -69.5° (290.5°) -> col 5810 (51 cols)
LAT_0P05_START = 1070
LAT_0P05_END = 1264
LON_0P05_START = 5760
LON_0P05_END = 5812

# Brasil Target Bounding Box (Río de Janeiro, Ilha Grande, Angra dos Reis, Paraty, Búzios):
# 0.1° grid: lat -23.5° (row 665) to lat -22.3° (row 678)
#   lon -44.8° (315.2°, col 3152) to lon -41.8° (318.2°, col 3183)
LAT_RIO_START = 665
LAT_RIO_END = 678
LON_RIO_START = 3152
LON_RIO_END = 3183

# 0.05° grid Brasil: row = round((lat + 90.0) * 20), col = round((lon + 360.0) * 20)
#   lat -23.5° -> row 1330, lat -22.25° -> row 1355 (inclusive 1356)
#   lon -44.8° (315.2°) -> col 6304, lon -41.75° (318.25°) -> col 6365 (inclusive 6366)
LAT_RIO_0P05_START = 1330
LAT_RIO_0P05_END = 1356
LON_RIO_0P05_START = 6304
LON_RIO_0P05_END = 6366

libzstd = ctypes.CDLL("libzstd.so.1")


class ZSTD_inBuffer(ctypes.Structure):
    _fields_ = [
        ("src", ctypes.c_void_p),
        ("size", ctypes.c_size_t),
        ("pos", ctypes.c_size_t),
    ]


class ZSTD_outBuffer(ctypes.Structure):
    _fields_ = [
        ("dst", ctypes.c_void_p),
        ("size", ctypes.c_size_t),
        ("pos", ctypes.c_size_t),
    ]


libzstd.ZSTD_createDStream.restype = ctypes.c_void_p
libzstd.ZSTD_freeDStream.argtypes = [ctypes.c_void_p]
libzstd.ZSTD_initDStream.argtypes = [ctypes.c_void_p]
libzstd.ZSTD_decompressStream.argtypes = [
    ctypes.c_void_p,
    ctypes.POINTER(ZSTD_outBuffer),
    ctypes.POINTER(ZSTD_inBuffer),
]
libzstd.ZSTD_decompressStream.restype = ctypes.c_size_t


def get_credentials():
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    creds.refresh(Request())
    return creds


def find_latest_synoptic_run(creds, min_chunks: int = 120) -> tuple[str, datetime]:
    """Finds the latest WeatherNext 3 run in GCS with at least min_chunks hourly steps."""
    now_utc = datetime.now(timezone.utc)
    candidates = []
    for delta_days in [0, 1, 2]:
        day_str = (now_utc - timedelta(days=delta_days)).strftime("%Y%m%d")
        url = (
            f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o?"
            + urllib.parse.urlencode(
                {"delimiter": "/", "prefix": f"{BASE_PREFIX}/{day_str}"}
            )
        )
        req = urllib.request.Request(
            url, headers={"Authorization": f"Bearer {creds.token}"}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            for p in data.get("prefixes", []):
                run_name = p.rstrip("/").split("/")[-1]
                candidates.append(run_name)

    for run_name in sorted(candidates, reverse=True):
        prefix = f"{BASE_PREFIX}/{run_name}/predictions.zarr/imerg_tp_1hr_p50/c/"
        url = (
            f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o?"
            + urllib.parse.urlencode({"delimiter": "/", "prefix": prefix})
        )
        req = urllib.request.Request(
            url, headers={"Authorization": f"Bearer {creds.token}"}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            n_chunks = len(data.get("prefixes", []))
            if n_chunks >= min_chunks:
                # Parse YYYYMMDD_HHhr_01_preds
                parts = run_name.split("_")
                dt_str = parts[0] + parts[1].replace("hr", "")
                init_dt = datetime.strptime(dt_str, "%Y%m%d%H").replace(
                    tzinfo=timezone.utc
                )
                return run_name, init_dt

    raise RuntimeError("No WeatherNext 3 synoptic run found with >= min_chunks.")


def decompress_partial(raw: bytes, target_rows: int, n_cols: int) -> np.ndarray | None:
    target_bytes = target_rows * n_cols * 4
    dctx = libzstd.ZSTD_createDStream()
    try:
        libzstd.ZSTD_initDStream(dctx)
        out_buf = ctypes.create_string_buffer(target_bytes)
        in_struct = ZSTD_inBuffer(
            ctypes.cast(ctypes.c_char_p(raw), ctypes.c_void_p), len(raw), 0
        )
        out_struct = ZSTD_outBuffer(
            ctypes.cast(out_buf, ctypes.c_void_p), target_bytes, 0
        )
        libzstd.ZSTD_decompressStream(
            dctx, ctypes.byref(out_struct), ctypes.byref(in_struct)
        )
        if out_struct.pos < target_bytes:
            return None
        return np.frombuffer(out_buf, dtype="float32", count=target_rows * n_cols).reshape(
            (target_rows, n_cols)
        )
    finally:
        libzstd.ZSTD_freeDStream(dctx)


def extract_weathernext3(hours: int = 120, workers: int = 32) -> str:
    t_start = time.time()
    creds = get_credentials()
    run_name, init_dt = find_latest_synoptic_run(creds, min_chunks=min(hours, 120))
    print(f"🚀 WeatherNext 3.0 Detectado: {run_name} (init_time: {init_dt.isoformat()})")
    print(
        f"📡 Extrayendo {hours} pasos horarios (1h) @ 0.1° (10 km) + 0.05° (5 km) desde gs://{BUCKET}..."
    )

    http = urllib3.PoolManager(maxsize=workers)
    base_zarr = f"{BASE_PREFIX}/{run_name}/predictions.zarr"

    # Variables at 0.1° (1801 x 3600) with custom Range byte caps sufficient for LAT_0P1_END=632
    vars_0p1 = {
        "imerg_tp_1hr_mean": 8200000,
        "imerg_tp_1hr_p50": 5800000,
        "imerg_tp_1hr_p75": 7500000,
        "imerg_tp_1hr_p90": 8400000,
        "total_precipitation_1hr_mean": 8600000,
        "total_precipitation_1hr_p50": 8200000,
        "total_precipitation_1hr_p75": 8600000,
        "total_precipitation_1hr_p90": 8800000,
        "temperature_2m_p50": 7600000,
        "wind_speed_100m_p90": 8500000,
        "wind_speed_10m_p50": 8500000,
        "wind_speed_10m_p90": 8500000,
        "mean_sea_level_pressure_p50": 7500000,
    }

    def fetch_0p1(args):
        var, t_idx, max_b = args
        path = f"{base_zarr}/{var}/c/{t_idx}/0/0"
        url = (
            f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o/"
            + urllib.parse.quote(path, safe="")
            + "?alt=media"
        )
        for attempt in range(2):
            cur_max_b = max_b if attempt == 0 else max_b + 3000000
            r = http.request(
                "GET",
                url,
                headers={
                    "Authorization": f"Bearer {creds.token}",
                    "Range": f"bytes=0-{cur_max_b}",
                },
                preload_content=False,
                timeout=20.0,
            )
            if r.status == 404:
                r.release_conn()
                return var, t_idx, None
            raw = r.read()
            r.release_conn()
            arr = decompress_partial(raw, LAT_0P1_END, 3600)
            if arr is not None:
                return (
                    var,
                    t_idx,
                    arr[LAT_0P1_START:LAT_0P1_END, LON_0P1_START:LON_0P1_END].copy(),
                )
        return var, t_idx, None

    tasks_0p1 = [
        (var, t_idx, max_b)
        for var, max_b in vars_0p1.items()
        for t_idx in range(hours)
    ]

    data_0p1 = {var: [None] * hours for var in vars_0p1}
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        task_0p1 = progress.add_task(f"Descargando tensores 10 km ({hours}h)...", total=len(tasks_0p1))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(fetch_0p1, t): t for t in tasks_0p1}
            for fut in as_completed(futures):
                var, t_idx, slice_arr = fut.result()
                data_0p1[var][t_idx] = slice_arr
                progress.advance(task_0p1)

    # Fill any missing single hourly chunk via neighbor interpolation
    n_lat_1 = LAT_0P1_END - LAT_0P1_START
    n_lon_1 = LON_0P1_END - LON_0P1_START
    for var in vars_0p1:
        for t in range(hours):
            if data_0p1[var][t] is None:
                prev_arr = next(
                    (data_0p1[var][p] for p in range(t - 1, -1, -1) if data_0p1[var][p] is not None),
                    None,
                )
                next_arr = next(
                    (data_0p1[var][n] for n in range(t + 1, hours) if data_0p1[var][n] is not None),
                    None,
                )
                if prev_arr is not None and next_arr is not None:
                    data_0p1[var][t] = 0.5 * (prev_arr + next_arr)
                elif prev_arr is not None:
                    data_0p1[var][t] = prev_arr.copy()
                elif next_arr is not None:
                    data_0p1[var][t] = next_arr.copy()
                else:
                    data_0p1[var][t] = np.zeros((n_lat_1, n_lon_1), dtype="float32")

    # Fetch 0.05° (5 km) station_head_temperature_2m_p50 every 6 hours to compute topographic microclimate delta
    sample_t_0p05 = sorted(list(set(list(range(0, hours, 6)) + [hours - 1])))

    def fetch_0p05(t_idx):
        path = f"{base_zarr}/station_head_temperature_2m_p50/c/{t_idx}/0/0"
        url = (
            f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o/"
            + urllib.parse.quote(path, safe="")
            + "?alt=media"
        )
        r = http.request(
            "GET",
            url,
            headers={
                "Authorization": f"Bearer {creds.token}",
                "Range": "bytes=0-30000000",
            },
            preload_content=False,
            timeout=25.0,
        )
        if r.status not in (200, 206):
            r.release_conn()
            return t_idx, None
        raw = r.read()
        r.release_conn()
        arr = decompress_partial(raw, LAT_0P05_END, 7200)
        if arr is not None:
            return (
                t_idx,
                arr[LAT_0P05_START:LAT_0P05_END, LON_0P05_START:LON_0P05_END].copy(),
            )
        return t_idx, None

    data_0p05_samples = {}
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold bright_magenta]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        task_05 = progress.add_task("Extrayendo cabezal 5 km (0.05°)...", total=len(sample_t_0p05))
        with ThreadPoolExecutor(max_workers=16) as ex:
            futures_05 = {ex.submit(fetch_0p05, t_idx): t_idx for t_idx in sample_t_0p05}
            for fut in as_completed(futures_05):
                t_idx, arr05 = fut.result()
                if arr05 is not None:
                    data_0p05_samples[t_idx] = arr05
                progress.advance(task_05)

    # Compute 0.05° minus 0.1° topographic delta at sampled hours and interpolate across all hours
    valid_t05 = sorted(data_0p05_samples.keys())
    lats_0p1 = np.round(-90.0 + np.arange(LAT_0P1_START, LAT_0P1_END) * 0.1, 2)
    lons_0p1 = np.round(-360.0 + np.arange(LON_0P1_START, LON_0P1_END) * 0.1, 2)
    lats_0p05 = np.round(-90.0 + np.arange(LAT_0P05_START, LAT_0P05_END) * 0.05, 2)
    lons_0p05 = np.round(-360.0 + np.arange(LON_0P05_START, LON_0P05_END) * 0.05, 2)

    # Map each 0.05° index to its nearest 0.1° index
    map_lat = np.array([int(np.argmin(np.abs(lats_0p1 - la))) for la in lats_0p05])
    map_lon = np.array([int(np.argmin(np.abs(lons_0p1 - lo))) for lo in lons_0p05])

    # Target locations + full 0.1° Chile grid
    # We store all 0.05° cells around Santiago/Cordillera/Valparaiso/Rancagua/Coquimbo so any 5km query resolves natively!
    target_boxes = [
        (-34.30, -32.90, -71.75, -70.15),  # RM + Valparaíso + O'Higgins + Cordillera (5 km native!)
        (-30.15, -29.80, -71.40, -71.10),  # La Serena / Coquimbo (5 km native!)
    ]

    keep_0p05_indices = []
    for i05, la in enumerate(lats_0p05):
        for j05, lo in enumerate(lons_0p05):
            if any(la_min <= la <= la_max and lo_min <= lo <= lo_max for la_min, la_max, lo_min, lo_max in target_boxes):
                keep_0p05_indices.append((i05, j05, float(la), float(lo), int(map_lat[i05]), int(map_lon[j05])))

    # Precompute topographic delta (T_5km - T_10km) at sampled times
    delta_samples = {}
    for t_s in valid_t05:
        t10_grid = data_0p1["temperature_2m_p50"][t_s]
        t05_grid = data_0p05_samples[t_s]
        delta_samples[t_s] = t05_grid - t10_grid[np.ix_(map_lat, map_lon)]

    rows = []
    for t_idx in range(hours):
        lead_h = t_idx + 1
        end_dt = init_dt + timedelta(hours=lead_h)

        # Interpolate delta_0p05 for t_idx
        if valid_t05:
            if t_idx <= valid_t05[0]:
                cur_delta = delta_samples[valid_t05[0]]
            elif t_idx >= valid_t05[-1]:
                cur_delta = delta_samples[valid_t05[-1]]
            else:
                idx_hi = next(k for k, v in enumerate(valid_t05) if v >= t_idx)
                t_lo, t_hi = valid_t05[idx_hi - 1], valid_t05[idx_hi]
                w = (t_idx - t_lo) / max(1, (t_hi - t_lo))
                cur_delta = (1.0 - w) * delta_samples[t_lo] + w * delta_samples[t_hi]
        else:
            cur_delta = np.zeros(( len(lats_0p05), len(lons_0p05) ), dtype="float32")

        imerg_mean = data_0p1["imerg_tp_1hr_mean"][t_idx] * 1000.0
        imerg_p50 = data_0p1["imerg_tp_1hr_p50"][t_idx] * 1000.0
        imerg_p75 = data_0p1["imerg_tp_1hr_p75"][t_idx] * 1000.0
        imerg_p90 = data_0p1["imerg_tp_1hr_p90"][t_idx] * 1000.0
        tp_mean = data_0p1["total_precipitation_1hr_mean"][t_idx] * 1000.0
        tp_p50 = data_0p1["total_precipitation_1hr_p50"][t_idx] * 1000.0
        tp_p75 = data_0p1["total_precipitation_1hr_p75"][t_idx] * 1000.0
        tp_p90 = data_0p1["total_precipitation_1hr_p90"][t_idx] * 1000.0
        t2m_10k = data_0p1["temperature_2m_p50"][t_idx] - 273.15
        w100_p90 = data_0p1["wind_speed_100m_p90"][t_idx] * 3.6
        w10_p50 = data_0p1["wind_speed_10m_p50"][t_idx] * 3.6
        w10_p90 = data_0p1["wind_speed_10m_p90"][t_idx] * 3.6
        mslp_p50 = data_0p1["mean_sea_level_pressure_p50"][t_idx] / 100.0

        for i05, j05, la, lo, i10, j10 in keep_0p05_indices:
            t_5km_c = float(t2m_10k[i10, j10] + cur_delta[i05, j05])
            rows.append(
                {
                    "init_time": init_dt,
                    "end_time": end_dt,
                    "lead_time_hours": lead_h,
                    "lat": la,
                    "lon": lo,
                    "lat_10km": float(lats_0p1[i10]),
                    "lon_10km": float(lons_0p1[j10]),
                    "imerg_1h_mean_mm": max(0.0, float(imerg_mean[i10, j10])),
                    "imerg_1h_p50_mm": max(0.0, float(imerg_p50[i10, j10])),
                    "imerg_1h_p75_mm": max(0.0, float(imerg_p75[i10, j10])),
                    "imerg_1h_p90_mm": max(0.0, float(imerg_p90[i10, j10])),
                    "tp_1h_mean_mm": max(0.0, float(tp_mean[i10, j10])),
                    "tp_1h_p50_mm": max(0.0, float(tp_p50[i10, j10])),
                    "tp_1h_p75_mm": max(0.0, float(tp_p75[i10, j10])),
                    "tp_1h_p90_mm": max(0.0, float(tp_p90[i10, j10])),
                    "temp_5km_p50_c": t_5km_c,
                    "temp_10km_p50_c": float(t2m_10k[i10, j10]),
                    "wind_100m_p90_kmh": float(w100_p90[i10, j10]),
                    "wind_10m_p50_kmh": float(w10_p50[i10, j10]),
                    "wind_10m_p90_kmh": float(w10_p90[i10, j10]),
                    "pressure_p50_hpa": float(mslp_p50[i10, j10]),
                }
            )

    df = pd.DataFrame(rows)
    os.makedirs("data", exist_ok=True)
    ts_str = datetime.now().strftime("%Y%m%d-%H%M")
    out_ts = f"data/weathernext3_{ts_str}.parquet"
    out_latest = "data/weathernext3_latest.parquet"
    df.to_parquet(out_ts, index=False)
    df.to_parquet(out_latest, index=False)
    elapsed = time.time() - t_start
    print(
        f"✅ WeatherNext 3 guardado en {out_latest} ({len(df):,} filas, {len(keep_0p05_indices)} celdas de 5 km × {hours}h) en {elapsed:.1f}s!"
    )
    return out_latest


def extract_weathernext3_rio(hours: int = 216, workers: int = 24) -> str:
    """Extrae pronóstico de WeatherNext 3.0 para Río de Janeiro, Ilha Grande, Angra, Paraty y Búzios."""
    t_start = time.time()
    creds = get_credentials()
    run_name, init_dt = find_latest_synoptic_run(creds, min_chunks=min(hours, 120))
    print(f"🚀 Extrayendo WeatherNext 3.0 para RÍO DE JANEIRO & ILHA GRANDE")
    print(f"Corrida: {run_name} (init_time: {init_dt.isoformat()})")
    print(f"Horizonte: {hours} horas ({(hours/24):.1f} días) @ 0.1° (10 km)")

    http = urllib3.PoolManager(maxsize=workers)
    base_zarr = f"{BASE_PREFIX}/{run_name}/predictions.zarr"

    vars_0p1 = [
        "imerg_tp_1hr_mean",
        "imerg_tp_1hr_p50",
        "imerg_tp_1hr_p75",
        "imerg_tp_1hr_p90",
        "total_precipitation_1hr_mean",
        "total_precipitation_1hr_p50",
        "total_precipitation_1hr_p75",
        "total_precipitation_1hr_p90",
        "temperature_2m_p50",
        "wind_speed_100m_p90",
        "wind_speed_10m_p50",
        "wind_speed_10m_p90",
        "mean_sea_level_pressure_p50",
        "total_cloud_cover_p50",
        "low_cloud_cover_p50",
        "surface_solar_radiation_downwards_1hr_p50",
        "total_sky_direct_solar_radiation_at_surface_1hr_p50",
        "dewpoint_temperature_2m_p50",
    ]

    def fetch_chunk(args):
        var, t_idx = args
        path = f"{base_zarr}/{var}/c/{t_idx}/0/0"
        url = (
            f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o/"
            + urllib.parse.quote(path, safe="")
            + "?alt=media"
        )
        for attempt in range(2):
            cur_max_b = 12000000 if attempt == 0 else 15000000
            try:
                r = http.request(
                    "GET",
                    url,
                    headers={
                        "Authorization": f"Bearer {creds.token}",
                        "Range": f"bytes=0-{cur_max_b}",
                    },
                    preload_content=False,
                    timeout=20.0,
                )
                if r.status == 404:
                    r.release_conn()
                    return var, t_idx, None
                raw = r.read()
                r.release_conn()
                arr = decompress_partial(raw, LAT_RIO_END, 3600)
                if arr is not None:
                    return var, t_idx, arr[LAT_RIO_START:LAT_RIO_END, LON_RIO_START:LON_RIO_END].copy()
            except Exception:
                pass
        return var, t_idx, None

    data = {var: [None] * hours for var in vars_0p1}

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        task_id = progress.add_task(f"Descargando tensores Río ({hours}h)...", total=len(vars_0p1) * hours)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for var in vars_0p1:
                futures = [executor.submit(fetch_chunk, (var, t_idx)) for t_idx in range(hours)]
                for fut in as_completed(futures):
                    _, t_idx, slice_arr = fut.result()
                    data[var][t_idx] = slice_arr
                    progress.advance(task_id)
                import gc
                gc.collect()

    # Interpolar huecos si falta algún chunk aislado
    n_lat = LAT_RIO_END - LAT_RIO_START
    n_lon = LON_RIO_END - LON_RIO_START
    for var in vars_0p1:
        for t in range(hours):
            if data[var][t] is None:
                prev_arr = next((data[var][k] for k in range(t - 1, -1, -1) if data[var][k] is not None), None)
                next_arr = next((data[var][k] for k in range(t + 1, hours) if data[var][k] is not None), None)
                if prev_arr is not None and next_arr is not None:
                    data[var][t] = (prev_arr + next_arr) / 2.0
                elif prev_arr is not None:
                    data[var][t] = prev_arr.copy()
                elif next_arr is not None:
                    data[var][t] = next_arr.copy()
                else:
                    data[var][t] = np.zeros((n_lat, n_lon), dtype="float32")

    # Muestrear cabezal térmico 5 km (0.05°) para capturar microclima costero e islas
    sample_t_0p05 = sorted(list(set(list(range(0, hours, 6)) + [hours - 1])))

    def fetch_0p05_rio(t_idx):
        path = f"{base_zarr}/station_head_temperature_2m_p50/c/{t_idx}/0/0"
        url = (
            f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o/"
            + urllib.parse.quote(path, safe="")
            + "?alt=media"
        )
        for attempt in range(2):
            cur_max_b = 32000000 if attempt == 0 else 36000000
            try:
                r = http.request(
                    "GET",
                    url,
                    headers={
                        "Authorization": f"Bearer {creds.token}",
                        "Range": f"bytes=0-{cur_max_b}",
                    },
                    preload_content=False,
                    timeout=25.0,
                )
                if r.status not in (200, 206):
                    r.release_conn()
                    return t_idx, None
                raw = r.read()
                r.release_conn()
                arr = decompress_partial(raw, LAT_RIO_0P05_END, 7200)
                if arr is not None:
                    return (
                        t_idx,
                        arr[LAT_RIO_0P05_START:LAT_RIO_0P05_END, LON_RIO_0P05_START:LON_RIO_0P05_END].copy(),
                    )
            except Exception:
                pass
        return t_idx, None

    data_0p05_samples = {}
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold bright_magenta]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        task_05 = progress.add_task("Extrayendo cabezal 5 km Brasil...", total=len(sample_t_0p05))
        with ThreadPoolExecutor(max_workers=min(workers, 16)) as executor_05:
            futures_05 = {executor_05.submit(fetch_0p05_rio, t_idx): t_idx for t_idx in sample_t_0p05}
            for fut in as_completed(futures_05):
                t_idx, arr05 = fut.result()
                if arr05 is not None:
                    data_0p05_samples[t_idx] = arr05
                progress.advance(task_05)

    lats = np.round(-90.0 + np.arange(LAT_RIO_START, LAT_RIO_END) * 0.1, 2)
    lons = np.round(-360.0 + np.arange(LON_RIO_START, LON_RIO_END) * 0.1, 2)

    # Mapeo de índices 0.1° hacia 0.05° en Brasil
    map_i05 = np.array([int(round((la + 90.0) * 20)) - LAT_RIO_0P05_START for la in lats])
    map_j05 = np.array([int(round((lo + 360.0) * 20)) - LON_RIO_0P05_START for lo in lons])

    valid_t05 = sorted(data_0p05_samples.keys())
    delta_samples = {}
    for t_s in valid_t05:
        if data_0p05_samples[t_s] is not None and data["temperature_2m_p50"][t_s] is not None:
            t05_grid = data_0p05_samples[t_s]
            t10_grid = data["temperature_2m_p50"][t_s]
            t05_matched = t05_grid[np.ix_(map_i05, map_j05)]
            delta_samples[t_s] = t05_matched - t10_grid

    rows = []
    for t_idx in range(hours):
        lead_h = t_idx + 1
        end_dt = init_dt + timedelta(hours=lead_h)

        if valid_t05 and valid_t05[0] in delta_samples:
            if t_idx <= valid_t05[0]:
                cur_delta = delta_samples[valid_t05[0]]
            elif t_idx >= valid_t05[-1]:
                cur_delta = delta_samples[valid_t05[-1]]
            else:
                idx_hi = next((k for k, v in enumerate(valid_t05) if v >= t_idx), len(valid_t05) - 1)
                t_lo, t_hi = valid_t05[idx_hi - 1], valid_t05[idx_hi]
                w = (t_idx - t_lo) / max(1, (t_hi - t_lo))
                cur_delta = (1.0 - w) * delta_samples.get(t_lo, np.zeros((len(lats), len(lons)), dtype="float32")) + \
                            w * delta_samples.get(t_hi, np.zeros((len(lats), len(lons)), dtype="float32"))
        else:
            cur_delta = np.zeros((len(lats), len(lons)), dtype="float32")

        imerg_mean = data["imerg_tp_1hr_mean"][t_idx] * 1000.0
        imerg_p50 = data["imerg_tp_1hr_p50"][t_idx] * 1000.0
        imerg_p75 = data["imerg_tp_1hr_p75"][t_idx] * 1000.0
        imerg_p90 = data["imerg_tp_1hr_p90"][t_idx] * 1000.0

        tp_mean = data["total_precipitation_1hr_mean"][t_idx] * 1000.0
        tp_p50 = data["total_precipitation_1hr_p50"][t_idx] * 1000.0
        tp_p75 = data["total_precipitation_1hr_p75"][t_idx] * 1000.0
        tp_p90 = data["total_precipitation_1hr_p90"][t_idx] * 1000.0

        t2_p50 = data["temperature_2m_p50"][t_idx] - 273.15
        w100_p90 = data["wind_speed_100m_p90"][t_idx] * 3.6
        w10_p50 = data["wind_speed_10m_p50"][t_idx] * 3.6
        w10_p90 = data["wind_speed_10m_p90"][t_idx] * 3.6
        mslp_p50 = data["mean_sea_level_pressure_p50"][t_idx] / 100.0

        tcc_raw = data["total_cloud_cover_p50"][t_idx]
        lcc_raw = data["low_cloud_cover_p50"][t_idx]
        ssrd_raw = data["surface_solar_radiation_downwards_1hr_p50"][t_idx]
        fdir_raw = data["total_sky_direct_solar_radiation_at_surface_1hr_p50"][t_idx]
        dew_raw = data["dewpoint_temperature_2m_p50"][t_idx]

        for i, la in enumerate(lats):
            for j, lo in enumerate(lons):
                t_5km_c = float(t2_p50[i, j] + cur_delta[i, j])
                tcc_pct = max(0.0, min(100.0, float(tcc_raw[i, j]) * 100.0))
                lcc_pct = max(0.0, min(100.0, float(lcc_raw[i, j]) * 100.0))
                ssrd_wm2 = max(0.0, float(ssrd_raw[i, j]) / 3600.0)
                fdir_wm2 = max(0.0, float(fdir_raw[i, j]) / 3600.0)
                dew_c = float(dew_raw[i, j] - 273.15)

                rows.append({
                    "init_time": init_dt,
                    "end_time": end_dt,
                    "lead_time_hours": lead_h,
                    "lat": float(la),
                    "lon": float(lo),
                    "lat_10km": float(la),
                    "lon_10km": float(lo),
                    "imerg_1h_mean_mm": max(0.0, float(imerg_mean[i, j])),
                    "imerg_1h_p50_mm": max(0.0, float(imerg_p50[i, j])),
                    "imerg_1h_p75_mm": max(0.0, float(imerg_p75[i, j])),
                    "imerg_1h_p90_mm": max(0.0, float(imerg_p90[i, j])),
                    "tp_1h_mean_mm": max(0.0, float(tp_mean[i, j])),
                    "tp_1h_p50_mm": max(0.0, float(tp_p50[i, j])),
                    "tp_1h_p75_mm": max(0.0, float(tp_p75[i, j])),
                    "tp_1h_p90_mm": max(0.0, float(tp_p90[i, j])),
                    "temp_5km_p50_c": t_5km_c,
                    "temp_10km_p50_c": float(t2_p50[i, j]),
                    "wind_100m_p90_kmh": float(w100_p90[i, j]),
                    "wind_10m_p50_kmh": float(w10_p50[i, j]),
                    "wind_10m_p90_kmh": float(w10_p90[i, j]),
                    "pressure_p50_hpa": float(mslp_p50[i, j]),
                    "tcc_p50_pct": tcc_pct,
                    "lcc_p50_pct": lcc_pct,
                    "ssrd_1h_p50_wm2": ssrd_wm2,
                    "fdir_1h_p50_wm2": fdir_wm2,
                    "dewpoint_2m_p50_c": dew_c,
                })

    df = pd.DataFrame(rows)
    os.makedirs("data", exist_ok=True)
    ts_str = datetime.now().strftime("%Y%m%d-%H%M")
    out_ts = f"data/weathernext3_rio_{ts_str}.parquet"
    out_latest = "data/weathernext3_rio_latest.parquet"
    df.to_parquet(out_ts, index=False)
    df.to_parquet(out_latest, index=False)
    elapsed = time.time() - t_start
    print(
        f"✅ WeatherNext 3 Río & Ilha Grande guardado en {out_latest} ({len(df):,} filas, {len(lats)*len(lons)} celdas × {hours}h) en {elapsed:.1f}s!"
    )
    return out_latest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extractor Zarr v3 de WeatherNext 3.0")
    parser.add_argument("--hours", type=int, default=None, help="Horizonte horario (defecto: 120h Chile, 216h Río)")
    parser.add_argument("--rio", "--brasil", "--vacaciones", dest="rio", action="store_true", help="Extraer pronóstico de Río de Janeiro & Ilha Grande")
    args = parser.parse_args()
    if args.rio:
        extract_weathernext3_rio(hours=args.hours or 216)
    else:
        extract_weathernext3(hours=args.hours or 120)
