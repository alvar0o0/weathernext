#!/usr/bin/env python3
"""
WEATHERNEXT 2.0 — CLI TUI & ANALIZADOR DE TEMPORALES (Google DeepMind Ensemble)
Visualizador offline en terminal para snapshots Parquet de WeatherNext en Polars y Rich.
Incluye soporte para presets de ubicación (Ñuñoa, Las Condes) y modo de traslado (Commute).
"""

import argparse
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import polars as pl
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box

console = Console()

# Zona horaria de Santiago de Chile (UTC-3 estándar / horario actual)
SANTIAGO_TZ_OFFSET = timedelta(hours=-3)

SPANISH_WEEKDAYS = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
SPANISH_DAYS_FULL = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
SPANISH_MONTHS_FULL = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]

# Presets de ubicaciones mapeados a WeatherNext 2 (0.25° / 28km) y WeatherNext 3 (0.05° / 5km & 0.1° / 10km)
PRESETS = {
    "casa": {
        "alias": "casa",
        "name": "Ñuñoa (Casa)",
        "desc": "Santiago Oriente • Plaza Ñuñoa",
        "target_cell": (-33.50, -70.50),
        "target_5km": (-33.45, -70.60),
        "base_elev_m": 600.0,
    },
    "nunoa": {
        "alias": "nunoa",
        "name": "Ñuñoa",
        "desc": "Santiago Oriente • Plaza Ñuñoa",
        "target_cell": (-33.50, -70.50),
        "target_5km": (-33.45, -70.60),
        "base_elev_m": 600.0,
    },
    "trabajo": {
        "alias": "trabajo",
        "name": "Las Condes (Trabajo)",
        "desc": "Santiago Oriente • Eje Apoquindo / Manquehue",
        "target_cell": (-33.50, -70.50),
        "target_5km": (-33.40, -70.55),
        "base_elev_m": 720.0,
    },
    "las_condes": {
        "alias": "las_condes",
        "name": "Las Condes",
        "desc": "Santiago Oriente • Eje Apoquindo / Manquehue",
        "target_cell": (-33.50, -70.50),
        "target_5km": (-33.40, -70.55),
        "base_elev_m": 720.0,
    },
    "pie_andino": {
        "alias": "pie_andino",
        "name": "Pie Andino (La Reina / Peñalolén)",
        "desc": "Falla de Ramón • Quebrada de Macul / Mahuida",
        "target_cell": (-33.50, -70.50),
        "target_5km": (-33.45, -70.50),
        "base_elev_m": 950.0,
    },
    "las_condes_alto": {
        "alias": "las_condes_alto",
        "name": "Las Condes Alto / Cantagallo",
        "desc": "Precordillera Norte • Cantagallo / San Carlos",
        "target_cell": (-33.25, -70.50),
        "target_5km": (-33.35, -70.50),
        "base_elev_m": 1050.0,
    },
    "cordillera": {
        "alias": "cordillera",
        "name": "Precordillera / Farellones",
        "desc": "Cordillera Andina • Farellones / Valle Nevado",
        "target_cell": (-33.25, -70.50),
        "target_5km": (-33.35, -70.30),
        "base_elev_m": 2400.0,
    },
    "maipo": {
        "alias": "maipo",
        "name": "San José de Maipo (Cajón del Maipo)",
        "desc": "Precordillera Sur • Cuenca Río Maipo",
        "target_cell": (-33.50, -70.25),
        "target_5km": (-33.65, -70.35),
        "base_elev_m": 1050.0,
    },
    "poniente": {
        "alias": "poniente",
        "name": "Santiago Centro / Poniente",
        "desc": "Valle Central • Santiago / Pudahuel / Maipú",
        "target_cell": (-33.50, -70.75),
        "target_5km": (-33.45, -70.65),
        "base_elev_m": 520.0,
    },
    "norte": {
        "alias": "norte",
        "name": "Santiago Norte (Chicureo)",
        "desc": "Chicureo • Colina • Lampa",
        "target_cell": (-33.25, -70.75),
        "target_5km": (-33.25, -70.70),
        "base_elev_m": 580.0,
    },
    "valpo": {
        "alias": "valpo",
        "name": "Valparaíso / Viña del Mar",
        "desc": "Litoral Central • Bahía de Valparaíso",
        "target_cell": (-33.00, -71.50),
        "target_5km": (-33.05, -71.60),
        "base_elev_m": 50.0,
    },
    "rancagua": {
        "alias": "rancagua",
        "name": "Rancagua (O'Higgins)",
        "desc": "Valle Cachapoal • Región de O'Higgins",
        "target_cell": (-34.25, -70.75),
        "target_5km": (-34.15, -70.75),
        "base_elev_m": 500.0,
    },
    "serena": {
        "alias": "serena",
        "name": "La Serena / Coquimbo",
        "desc": "Norte Chico • Bahía de Coquimbo",
        "target_cell": (-30.00, -71.25),
        "target_5km": (-29.95, -71.25),
        "base_elev_m": 40.0,
    },
}


def load_latest_snapshot(parquet_path: str | None = None) -> tuple[pl.DataFrame, Path]:
    """Carga el snapshot Parquet local más reciente o el especificado."""
    if parquet_path:
        p = Path(parquet_path)
        if not p.exists():
            console.print(f"[bold red]Error:[/bold red] No se encontró el archivo: {p}")
            sys.exit(1)
        return pl.read_parquet(p), p

    data_dir = Path("data")
    latest_file = data_dir / "weathernext_latest.parquet"
    if latest_file.exists():
        return pl.read_parquet(latest_file), latest_file

    parquets = sorted(list(data_dir.glob("weathernext_*.parquet")), key=lambda f: f.stat().st_mtime, reverse=True)
    if not parquets:
        console.print("[bold red]Error:[/bold red] No hay snapshots en data/. Ejecuta primero './storm --fetch'.")
        sys.exit(1)

    return pl.read_parquet(parquets[0]), parquets[0]


def filter_by_location(df: pl.DataFrame, preset_key: str = "casa") -> tuple[pl.DataFrame, dict]:
    """Filtra el DataFrame por la celda objetivo del preset."""
    clean_key = preset_key.lower().replace("-", "_")
    preset = PRESETS.get(clean_key, PRESETS["casa"])
    target_lat, target_lon = preset["target_cell"]

    if "latitude" in df.columns and "longitude" in df.columns:
        filtered = df.filter(
            (pl.col("latitude").round(2) == target_lat) & 
            (pl.col("longitude").round(2) == target_lon)
        )
        if len(filtered) > 0:
            return filtered, preset

    # Si no hay columnas de coordenadas exactas, retornar todo
    return df, preset


def render_rain_bar(mm: float, max_scale: float = 15.0, width: int = 8) -> Text:
    """Genera una barra visual en caracteres Unicode con gradiente de color según mm."""
    if mm <= 0.05:
        return Text("·" * width, style="dim")

    ratio = min(1.0, mm / max_scale)
    filled = int(round(ratio * width))
    filled = max(1, min(width, filled))
    empty = width - filled

    bar_chars = "█" * filled + "░" * empty

    if mm < 2.0:
        color = "green"
    elif mm < 5.0:
        color = "cyan"
    elif mm < 10.0:
        color = "bright_blue"
    elif mm < 16.0:
        color = "bold magenta"
    else:
        color = "bold red on #330000"

    return Text(bar_chars, style=color)


def analyze_storm(df: pl.DataFrame, max_hours: int = 168, base_elev_m: float = 600.0) -> tuple[pl.DataFrame, dict]:
    """
    Agrega los 64 miembros del ensamble por paso de 6 horas y por trayectoria completa:
    - Calcula estadísticas por ventana (Media, P10, P50, P90, Máximo, Probabilidad de lluvia).
    - Calcula acumulados reales por trayectoria de miembro (true_cum_p10, true_cum_p50, true_cum_p90, true_cum_max).
    - Modela ráfagas turbulentas en superficie integrando el viento a 100m (100m_wind).
    - Calcula Isoterma 0°C activa (gradiente pseudoadiabático húmedo 6.5°C/km) y Riesgo Aluvional.
    - Detecta Inestabilidad Convectiva / Baja Segregada (choque térmico ΔT y dispersión extrema).
    """
    filtered = df.filter(pl.col("forecast_hours") <= max_hours)

    p_col = pl.col("precip_6h_mm") if "precip_6h_mm" in filtered.columns else (pl.col("precip_6h").clip(lower_bound=0.0) * 1000.0)
    pres_col = pl.col("pressure_hpa") if "pressure_hpa" in filtered.columns else (pl.col("pressure_msl") / 100.0)
    wind_col = pl.col("wind_speed_kmh") if "wind_speed_kmh" in filtered.columns else ((pl.col("wind_u_10m").pow(2) + pl.col("wind_v_10m").pow(2)).sqrt() * 3.6)

    if "wind_speed_100m_kmh" in filtered.columns:
        wind_100m_col = pl.col("wind_speed_100m_kmh")
    elif "wind_u_100m" in filtered.columns and "wind_v_100m" in filtered.columns:
        wind_100m_col = (pl.col("wind_u_100m").pow(2) + pl.col("wind_v_100m").pow(2)).sqrt() * 3.6
    else:
        wind_100m_col = None

    if wind_100m_col is not None:
        # En convección activa (p_col >= 1.0 mm), las corrientes descendentes (downdrafts)
        # transfieren el momentum de 100m a superficie con factor de ráfaga turbulenta.
        gust_expr = (
            pl.when(p_col >= 1.0)
            .then(pl.max_horizontal(wind_100m_col * 1.25, wind_col * 1.55))
            .otherwise(pl.max_horizontal(wind_100m_col * 1.10, wind_col * 1.35))
        )
    else:
        gust_expr = (
            pl.when(p_col >= 1.0)
            .then(wind_col * 1.65)
            .otherwise(wind_col * 1.35)
        )

    summary = filtered.group_by(["forecast_hours", "forecast_time"]).agg([
        p_col.mean().round(1).alias("precip_mean"),
        p_col.quantile(0.10).round(1).alias("p10"),
        p_col.quantile(0.50).round(1).alias("p50"),
        p_col.quantile(0.90).round(1).alias("p90"),
        p_col.max().round(1).alias("max"),
        (p_col > 0.5).mean().mul(100).round(0).alias("prob_pct"),
        pl.col("temp_2m_celsius").quantile(0.50).round(1).alias("temp_p50"),
        wind_col.quantile(0.90).round(1).alias("wind_p90"),
        gust_expr.quantile(0.90).round(1).alias("gust_p90"),
        pres_col.quantile(0.50).round(1).alias("pres_p50"),
    ]).with_columns(
        (pl.lit(base_elev_m) + (pl.col("temp_p50") / 0.0065)).round(0).cast(pl.Int64).alias("iso0_m")
    ).sort("forecast_hours")

    # 1. Acumulados reales por trayectoria de miembro (elimina sesgo del -35% por desfase de fase)
    member_totals = filtered.group_by("ensemble_member").agg(
        p_col.sum().alias("member_total_mm")
    )
    true_cum_p10 = round(float(member_totals["member_total_mm"].quantile(0.10) or 0.0), 1)
    true_cum_p50 = round(float(member_totals["member_total_mm"].quantile(0.50) or 0.0), 1)
    true_cum_p90 = round(float(member_totals["member_total_mm"].quantile(0.90) or 0.0), 1)
    true_cum_max = round(float(member_totals["member_total_mm"].max() or 0.0), 1)
    cum_mean = round(float(member_totals["member_total_mm"].mean() or 0.0), 1)

    window_sum_p50 = round(float(summary["p50"].sum()), 1)
    window_sum_p90 = round(float(summary["p90"].sum()), 1)

    peak_row = summary.sort(["p90", "p50"], descending=True).row(0, named=True)
    min_pressure = round(float(summary["pres_p50"].min()), 1)
    max_pressure = round(float(summary["pres_p50"].max()), 1)
    max_wind = round(float(summary["wind_p90"].max()), 1)
    max_gust = round(float(summary["gust_p90"].max()), 1)

    # 2. Isoterma 0°C en ventanas con precipitación activa y Riesgo Aluvional
    rain_windows = summary.filter((pl.col("prob_pct") >= 40) | (pl.col("p50") >= 0.5) | (pl.col("p90") >= 2.0))
    if len(rain_windows) > 0:
        iso0_rain_min = int(rain_windows["iso0_m"].min())
        iso0_rain_max = int(rain_windows["iso0_m"].max())
    else:
        iso0_rain_min = int(summary["iso0_m"].min())
        iso0_rain_max = int(summary["iso0_m"].max())
    iso0_peak = int(peak_row["iso0_m"])

    # Ventanas lluviosas intensas con isoterma alta (>2800m)
    high_iso_rain = summary.filter(
        ((pl.col("p50") >= 1.5) | (pl.col("p90") >= 4.0)) & (pl.col("iso0_m") >= 2800)
    )
    if len(high_iso_rain) > 0 or (iso0_rain_max >= 2800 and true_cum_p50 >= 15.0):
        alluvial_risk = "ALTO (Isoterma >2800m en lluvia)"
    elif iso0_rain_max >= 2400 and true_cum_p50 >= 10.0:
        alluvial_risk = "MODERADO (Vigilar quebradas)"
    else:
        alluvial_risk = "BAJO (Nieve en cordillera)"

    # 3. Diagnóstico de Inestabilidad Convectiva / Baja Segregada (Choque Térmico ΔT + Dispersión)
    temp_max_pre = float(summary["temp_p50"].max())
    temp_min_event = float(summary["temp_p50"].min())
    thermal_drop = round(temp_max_pre - temp_min_event, 1)
    pres_drop = round(max_pressure - min_pressure, 1)

    if thermal_drop >= 8.0 and (peak_row["max"] >= 12.0 or peak_row["p90"] >= 8.0):
        convective_alert = f"ALTA (Baja Segregada • ΔT -{thermal_drop:.1f}°C • Tormentas Eléctricas)"
    elif thermal_drop >= 6.0 or peak_row["max"] >= 10.0:
        convective_alert = f"MODERADA (Convección Activa • ΔT -{thermal_drop:.1f}°C)"
    else:
        convective_alert = f"BAJA (Frente Estratiforme • ΔT -{thermal_drop:.1f}°C)"

    metrics = {
        "cum_mean": cum_mean,
        "cum_p10": true_cum_p10,
        "cum_p50": true_cum_p50,
        "cum_p90": true_cum_p90,
        "cum_max": true_cum_max,
        "window_sum_p50": window_sum_p50,
        "window_sum_p90": window_sum_p90,
        "peak_time": peak_row["forecast_time"],
        "peak_hours": peak_row["forecast_hours"],
        "peak_p50": peak_row["p50"],
        "peak_p90": peak_row["p90"],
        "peak_max": peak_row["max"],
        "min_pressure": min_pressure,
        "pres_drop": pres_drop,
        "max_wind": max_wind,
        "max_gust": max_gust,
        "iso0_rain_min": iso0_rain_min,
        "iso0_rain_max": iso0_rain_max,
        "iso0_peak": iso0_peak,
        "alluvial_risk": alluvial_risk,
        "temp_max_pre": temp_max_pre,
        "temp_min_event": temp_min_event,
        "thermal_drop": thermal_drop,
        "convective_alert": convective_alert,
        "has_100m_wind": wind_100m_col is not None,
        "init_time": df["init_time"][0],
        "total_horizons": len(summary),
    }

    return summary, metrics


def determine_alert_level(metrics: dict) -> tuple[str, str, str]:
    """Determina el nivel de alerta sinóptica en base a acumulados por trayectoria, ráfagas, isoterma y convección."""
    p90 = metrics["cum_p90"]
    p50 = metrics["cum_p50"]
    win_p90 = metrics.get("window_sum_p90", p90)
    gust = metrics.get("max_gust", metrics["max_wind"])
    alluvial = metrics.get("alluvial_risk", "")
    convective = metrics.get("convective_alert", "")

    if p90 >= 80 or win_p90 >= 90 or p50 >= 50 or gust >= 60 or ("ALTO" in alluvial and p90 >= 45):
        return "ROJA", "ALERTA ROJA (Temporal Severo / Río Atmosférico + Baja Segregada)", "bold white on red"
    elif p90 >= 45 or p50 >= 25 or gust >= 40 or "ALTA" in convective:
        return "NARANJA", "ALERTA NARANJA (Temporal Significativo / Convección Severa)", "bold black on yellow"
    elif p90 >= 15 or p50 >= 10:
        return "AMARILLA", "ALERTA AMARILLA (Chubascos / Precipitación Activa)", "bold black on bright_yellow"
    else:
        return "VERDE", "ALERTA VERDE (Condiciones Normales / Sin Riesgo Mayor)", "bold white on green"


def generate_html_spaghetti_plot(df: pl.DataFrame, location_name: str = "Santiago Oriente", output_path: str = "data/temporal_santiago.html"):
    """Genera un gráfico HTML interactivo tipo spaghetti con las 64 trayectorias del ensamble."""
    try:
        import plotly.graph_objects as go
    except ImportError:
        return None

    df_sorted = df.sort(["ensemble_member", "forecast_hours"])
    members = df_sorted["ensemble_member"].unique().to_list()

    fig = go.Figure()

    for m in members:
        m_df = df_sorted.filter(pl.col("ensemble_member") == m)
        p_vals = m_df["precip_6h_mm"].to_list() if "precip_6h_mm" in m_df.columns else (m_df["precip_6h"] * 1000).clip(lower=0).to_list()
        cum_vals = []
        running = 0.0
        for v in p_vals:
            running += v
            cum_vals.append(round(running, 1))

        times = [t.strftime("%Y-%m-%d %H:%M") for t in m_df["forecast_time"].to_list()]

        fig.add_trace(go.Scatter(
            x=times,
            y=cum_vals,
            mode='lines',
            line=dict(color='rgba(100, 180, 240, 0.25)', width=1),
            hoverinfo='skip',
            showlegend=False,
            name=f"Miembro {m}"
        ))

    summary = df.group_by(["forecast_hours", "forecast_time"]).agg([
        pl.col("precip_6h_mm").quantile(0.50).alias("p50"),
        pl.col("precip_6h_mm").quantile(0.90).alias("p90"),
        pl.col("pressure_hpa").quantile(0.50).alias("pres"),
    ]).sort("forecast_hours")

    p50_cum = []
    p90_cum = []
    r50 = 0.0
    r90 = 0.0
    for row in summary.iter_rows(named=True):
        r50 += row["p50"]
        r90 += row["p90"]
        p50_cum.append(round(r50, 1))
        p90_cum.append(round(r90, 1))

    times_sum = [t.strftime("%Y-%m-%d %H:%M") for t in summary["forecast_time"].to_list()]

    fig.add_trace(go.Scatter(
        x=times_sum,
        y=p90_cum,
        mode='lines',
        line=dict(color='rgba(255, 120, 0, 0.9)', width=2.5, dash='dot'),
        name='Peor Escenario (Percentil 90)'
    ))

    fig.add_trace(go.Scatter(
        x=times_sum,
        y=p50_cum,
        mode='lines+markers',
        line=dict(color='#00e5ff', width=3.5),
        marker=dict(size=5),
        name='Escenario Central (Mediana P50)'
    ))

    fig.add_trace(go.Scatter(
        x=times_sum,
        y=summary["pres"].to_list(),
        mode='lines',
        line=dict(color='rgba(255, 80, 120, 0.8)', width=2, dash='dash'),
        name='Presión Barométrica (hPa)',
        yaxis='y2'
    ))

    fig.update_layout(
        title=f'🌧️ DeepMind WeatherNext 2.0 — Ensamble Spaghetti Plot ({location_name})',
        template='plotly_dark',
        xaxis=dict(title='Fecha y Hora (UTC)', gridcolor='#263238'),
        yaxis=dict(title='Lluvia Acumulada (mm)', gridcolor='#263238'),
        yaxis2=dict(
            title='Presión (hPa)',
            overlaying='y',
            side='right',
            showgrid=False
        ),
        legend=dict(x=0.02, y=0.98, bgcolor='rgba(0,0,0,0.6)'),
        hovermode='x unified',
        height=620,
    )

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(out))
    return out


def _build_commute_recommendations(commute_rows: list[dict], is_mobile: bool = False) -> str:
    """Genera recomendaciones dinámicas de traslado según las ventanas críticas reales del pronóstico."""
    if not commute_rows:
        return "• [bold green]Sin precipitaciones relevantes en ventanas de traslado.[/bold green]"

    sorted_rows = sorted(commute_rows, key=lambda r: (r["p90"], r["p50"]), reverse=True)
    top = sorted_rows[0]
    second = sorted_rows[1] if len(sorted_rows) > 1 else None

    lines = []
    if top["p50"] >= 5.0 or top["p90"] >= 10.0:
        if is_mobile:
            lines.append(
                f"• [bold red]{top['label']}:[/bold red] Pico crítico ({top['p50']:.1f}/{top['p90']:.1f} mm/6h).\n"
                f"  Riesgo de anegamiento en ejes Tobalaba/Manquehue.\n"
                f"  [italic]👉 Teletrabajo altamente recomendado.[/italic]"
            )
        else:
            lines.append(
                f"• [bold red]{top['label']} ({top['sentido']}):[/bold red] Ventana más compleja con [bold]{top['p50']:.1f} mm[/bold] esperados "
                f"(P90: [bold]{top['p90']:.1f} mm/6h[/bold], ráfagas ~{top['gust']:.0f} km/h).\n"
                f"  [italic]Alto riesgo de anegamiento en Manquehue, Tobalaba y Vespucio. Se recomienda priorizar teletrabajo.[/italic]"
            )
    elif top["p50"] >= 1.5 or top["p90"] >= 4.0:
        lines.append(
            f"• [bold yellow]{top['label']}:[/bold yellow] Precipitación activa ({top['p50']:.1f} mm P50 / {top['p90']:.1f} mm P90).\n"
            f"  Congestión alta y calzada resbaladiza; considera salir con +25 min de margen."
        )
    else:
        lines.append(
            f"• [bold green]Panorama General:[/bold green] Ventanas de hora punta con baja acumulación (máx {top['p50']:.1f}/{top['p90']:.1f} mm en {top['label']})."
        )

    if second and (second["p50"] >= 1.0 or second["p90"] >= 3.5):
        lines.append(
            f"• [bold yellow]{second['label']}:[/bold yellow] Segunda ventana de atención ({second['p50']:.1f} mm P50 / {second['p90']:.1f} mm P90). "
            f"Traslado posible con precaución."
        )

    return "\n".join(lines)


def display_commute_view(df: pl.DataFrame, is_mobile: bool = False):
    """
    Vista especializada de Traslado Diario (Commute):
    Analiza ventanas de viaje entre Ñuñoa (Casa) y Las Condes (Trabajo)
    para los próximos 5 días hábiles/calendario de forma 100% dinámica.
    """
    df_oriente, preset = filter_by_location(df, "casa")
    summary, _ = analyze_storm(df_oriente, max_hours=120, base_elev_m=preset.get("base_elev_m", 600.0))

    commute_rows = []
    for row in summary.iter_rows(named=True):
        ft_local = row["forecast_time"] + SANTIAGO_TZ_OFFSET
        hour = ft_local.hour
        day_str = f"{SPANISH_WEEKDAYS[ft_local.weekday()]} {ft_local.day:02d}/{ft_local.month:02d}"
        if hour in (9, 21):
            tramo = "Mañana (08-10h)" if hour == 9 else "Tarde (18-20h)"
            sentido = "Ñuñoa ➔ Las Condes" if hour == 9 else "Las Condes ➔ Ñuñoa"
            commute_rows.append({
                "day_str": day_str,
                "hour": hour,
                "label": f"{day_str} {tramo}",
                "sentido": sentido,
                "p50": row["p50"],
                "p90": row["p90"],
                "gust": row["gust_p90"],
                "iso0": row["iso0_m"],
            })

    rec_text = _build_commute_recommendations(commute_rows, is_mobile=is_mobile)

    if is_mobile:
        console.print(Panel(
            "[bold cyan]🚗 TRASLADO DIARIO: ÑUÑOA ⇄ LAS CONDES[/bold cyan]\n"
            "[dim]Evaluación para celular (Horas Punta)[/dim]",
            border_style="magenta",
            box=box.ROUNDED
        ))

        blocks_text = Text()
        first = True
        div_len = max(20, min(46, console.width - 4))
        for item in commute_rows:
            p50 = item["p50"]
            p90 = item["p90"]
            if not first:
                blocks_text.append("─" * div_len + "\n", style="dim")
            first = False
            blocks_text.append(f"📅 {item['label']} CLT\n", style="bold white")
            blocks_text.append(f"➔ Sentido: {item['sentido']}\n", style="cyan")
            blocks_text.append(f"💧 Lluvia: {p50:4.1f} mm (P90: {p90:4.1f} mm) • Ráf: {item['gust']:.0f}k\n", style="bold")
            if p50 >= 6.0 or p90 >= 12.0:
                blocks_text.append("🔴 Impacto: CRÍTICO (Anegamientos)\n", style="bold red")
            elif p50 >= 2.5 or p90 >= 6.0:
                blocks_text.append("🟠 Impacto: ALTO (Congestión Severa)\n", style="bold yellow")
            elif p50 >= 0.5 or p90 >= 2.0:
                blocks_text.append("🟡 Impacto: MODERADO (Resbaladizo)\n", style="cyan")
            else:
                blocks_text.append("🟢 Impacto: Despejado / Sin riesgo\n", style="green")

        console.print(Panel(blocks_text, title="🚦 Ventanas de Viaje", border_style="cyan", box=box.ROUNDED))
        console.print(Panel(rec_text, title="💡 Recomendación Dinámica", border_style="yellow", box=box.ROUNDED))
        return

    # Vista escritorio estándar (ancho)
    console.print(Panel(
        "[bold cyan]🚗 ANÁLISIS DE TRASLADO DIARIO: ÑUÑOA ⇄ LAS CONDES[/bold cyan]\n"
        "[dim]Evaluación dinámica de ventanas de hora punta (Mañana 08:00-10:00 CLT / Tarde 18:00-20:00 CLT)[/dim]",
        border_style="magenta",
        box=box.ROUNDED
    ))

    commute_table = Table(box=box.ROUNDED, header_style="bold cyan", padding=(0, 1))
    commute_table.add_column("Ventana CLT", style="bold white", no_wrap=True)
    commute_table.add_column("Sentido", justify="center", no_wrap=True)
    commute_table.add_column("P50", justify="right", style="bold cyan", no_wrap=True)
    commute_table.add_column("P90", justify="right", style="bold orange3", no_wrap=True)
    commute_table.add_column("Ráf", justify="right", no_wrap=True)
    commute_table.add_column("Impacto Vial", justify="left", no_wrap=True)

    for item in commute_rows:
        p50 = item["p50"]
        p90 = item["p90"]
        if p50 >= 6.0 or p90 >= 12.0:
            impacto = Text("🔴 CRÍTICO: Anegado", style="bold red")
        elif p50 >= 2.5 or p90 >= 6.0:
            impacto = Text("🟠 ALTO: Congestión", style="bold yellow")
        elif p50 >= 0.5 or p90 >= 2.0:
            impacto = Text("🟡 MOD: Resbaladizo", style="cyan")
        else:
            impacto = Text("🟢 Normal / Despejado", style="green")

        short_day = item["day_str"][:6]  # "Lun 19"
        short_label = f"{short_day} {'08-10h' if item['hour'] == 9 else '18-20h'}"
        short_sentido = "Ñuñoa➔Condes" if item["hour"] == 9 else "Condes➔Ñuñoa"
        commute_table.add_row(
            short_label,
            short_sentido,
            f"{p50:4.1f}mm",
            f"{p90:4.1f}mm",
            f"{item['gust']:2.0f}k",
            impacto,
        )

    console.print(commute_table)
    console.print(Panel(
        f"💡 [bold yellow]Recomendación Estratégica Dinámica:[/bold yellow]\n{rec_text}",
        border_style="yellow",
        box=box.ROUNDED
    ))


def display_clusters_view(
    parquet_path: str | None = None,
    location_key: str = "casa",
    is_mobile: bool = False,
    max_hours: int = 72,
):
    """Analiza y agrupa los miembros del ensamble en clusters según el momento de su descarga máxima."""
    raw_df, filepath = load_latest_snapshot(parquet_path)
    df, preset = filter_by_location(raw_df, preset_key=location_key)

    if "ensemble_member" not in df.columns:
        console.print("[bold red]Error:[/bold red] El archivo no contiene trayectorias individuales de miembros ('ensemble_member').")
        return

    # Filtrar por horizonte de pulso (por defecto 72h / 3 días)
    sub = df.filter(pl.col("forecast_hours") <= max_hours)
    n_members = df["ensemble_member"].n_unique()

    # Calcular para cada miembro su ventana pico (6h), intensidad máxima y acumulado del pulso
    member_peaks = sub.group_by("ensemble_member").agg([
        pl.col("precip_6h_mm").max().alias("peak_mm_6h"),
        pl.col("forecast_time").sort_by("precip_6h_mm").last().alias("peak_time"),
        pl.col("forecast_hours").sort_by("precip_6h_mm").last().alias("peak_hours"),
        pl.col("precip_6h_mm").sum().alias("total_pulse_mm"),
    ]).with_columns([
        (pl.col("peak_time") + SANTIAGO_TZ_OFFSET).alias("peak_local")
    ])

    # Agrupar en clusters por ventana horaria de ocurrencia del pico
    clusters = member_peaks.group_by("peak_local").agg([
        pl.len().alias("n_miembros"),
        pl.col("peak_mm_6h").mean().round(1).alias("peak_prom"),
        pl.col("peak_mm_6h").max().round(1).alias("peak_max"),
        pl.col("total_pulse_mm").mean().round(1).alias("total_prom"),
        pl.col("total_pulse_mm").max().round(1).alias("total_max"),
        pl.col("ensemble_member").alias("members_list"),
    ]).sort("n_miembros", descending=True)

    # Encabezado
    init_dt = df["init_time"][0]
    init_str = init_dt.strftime("%Y-%m-%d %H:%M UTC") if isinstance(init_dt, datetime) else str(init_dt)[:16]
    elev_m = int(preset.get("base_elev_m", 600))

    header = Text()
    header.append("🎯 ANÁLISIS DE CLUSTERS DE DESCARGA PICO (ENSEMBLE TIMING)\n", style="bold cyan")
    header.append(f"Ubicación: {preset['name']} (Cota ~{elev_m}m) | Miembros: {n_members} | Corrida: {init_str}\n", style="bold yellow")
    header.append(f"Horizonte evaluado: {max_hours}h (Pulso principal • Snapshot: {filepath.name})", style="dim")
    console.print(Panel(header, border_style="cyan", box=box.ROUNDED))

    # Identificar cluster dominante
    top_cluster = clusters.row(0, named=True)
    top_dt = top_cluster["peak_local"]
    top_day = SPANISH_WEEKDAYS[top_dt.weekday()]
    top_pct = (top_cluster["n_miembros"] / n_members) * 100

    # Tarjeta de síntesis de clusters
    summary_card = Panel(
        f"[bold magenta]Cluster Dominante ({top_pct:.1f}% de Consenso):[/bold magenta] "
        f"[bold white]{top_day} {top_dt.strftime('%d/%m %H:%M CLT')}[/bold white] "
        f"([cyan]{top_cluster['n_miembros']} de {n_members} miembros[/cyan])\n"
        f"• Descarga pico media en 6h: [bold]{top_cluster['peak_prom']:.1f} mm[/bold] "
        f"(Peor escenario del cluster: [bold red]{top_cluster['peak_max']:.1f} mm/6h[/bold red])\n"
        f"• Acumulado medio del pulso: [bold]{top_cluster['total_prom']:.1f} mm[/bold]\n\n"
        f"⚠️  [bold yellow]Riesgo Hidrológico de Escurrimiento:[/bold yellow]\n"
        f"Al producirse el pico en la precordillera (cota 950–2.400m con isoterma alta >2.800m), "
        f"la masa de agua líquida escurre hacia {preset['name']} en una ventana de [bold cyan]60 a 90 minutos[/bold cyan] "
        f"a través de las quebradas de drenaje y avenidas orientadas este-oeste.",
        title="⚡ Síntesis de Consenso del Ensamble",
        border_style="magenta",
        box=box.ROUNDED,
    )
    console.print(summary_card)

    # Tabla de clusters
    cluster_table = Table(
        title=f"📊 Agrupación de Miembros por Ventana Horaria del Chaparrón Más Fuerte ({preset['name']})",
        box=box.SIMPLE_HEAVY if not is_mobile else box.ROUNDED,
        header_style="bold cyan",
        padding=(0, 1),
        pad_edge=False,
    )
    cluster_table.add_column("Ventana Pico", style="bold white", no_wrap=True)
    cluster_table.add_column("Consenso", justify="right", style="cyan", no_wrap=True)
    cluster_table.add_column("Bar", no_wrap=True)
    cluster_table.add_column("Pico 6h(M/Mx)", justify="right", style="bold cyan", no_wrap=True)
    cluster_table.add_column("Total", justify="right", no_wrap=True)
    cluster_table.add_column("Ej. Miembros", style="dim", no_wrap=True)

    for c in clusters.iter_rows(named=True):
        dt = c["peak_local"]
        day_str = SPANISH_WEEKDAYS[dt.weekday()]
        label = f"{day_str} {dt.strftime('%d %H:%M')}"
        cnt = c["n_miembros"]
        pct = (cnt / n_members) * 100
        
        # Barra de consenso de 6 caracteres
        bar_len = int(round((cnt / n_members) * 6))
        bar_str = "█" * bar_len + "░" * (6 - bar_len)
        bar_txt = Text(bar_str, style="bold magenta" if pct >= 40 else ("yellow" if pct >= 15 else "dim"))

        pct_style = "bold red" if pct >= 50 else ("bold yellow" if pct >= 20 else "cyan")
        consenso_txt = Text(f"{cnt:2d}m({pct:4.1f}%)", style=pct_style)

        pico_txt = f"{c['peak_prom']:4.1f}/{c['peak_max']:4.1f}m"
        tot_txt = f"{c['total_prom']:4.1f}m"
        
        sample_m = c["members_list"][:4]
        members_str = "#" + ",".join(str(m) for m in sample_m)
        if len(c["members_list"]) > 4:
            members_str += "…"

        cluster_table.add_row(
            label,
            consenso_txt,
            bar_txt,
            pico_txt,
            tot_txt,
            members_str,
        )

    console.print(cluster_table)


def _display_tui_mobile(
    summary: pl.DataFrame,
    metrics: dict,
    preset: dict,
    init_str: str,
    level_code: str,
    level_label: str,
    level_style: str,
    days: int,
    hours: int | None = None,
):
    """Renderizado optimizado para pantallas estrechas / smartphones vía SSH."""
    # 1. Header Compacto
    header_text = Text()
    header_text.append("🌦️ WEATHERNEXT 2.0 (Mobile)\n", style="bold cyan")
    header_text.append(f"📍 {preset['name']}\n", style="bold yellow")
    header_text.append(f"Corrida: {init_str} • 64 miembros", style="dim")
    console.print(Panel(header_text, border_style="cyan", box=box.ROUNDED))

    # 2. Resumen Síntesis (Tarjeta Vertical Unificada)
    alert_border = "red" if "ROJA" in level_code else ("yellow" if "NARANJA" in level_code or "AMARILLA" in level_code else "green")
    peak_dt = metrics["peak_time"]
    peak_dt_local = peak_dt + SANTIAGO_TZ_OFFSET
    peak_day_name = SPANISH_WEEKDAYS[peak_dt_local.weekday()]

    summary_body = Text()
    short_labels = {
        "ROJA": "ALERTA ROJA • Temporal Severo",
        "NARANJA": "ALERTA NARANJA • Temporal Significativo",
        "AMARILLA": "ALERTA AMARILLA • Chubascos Activos",
        "VERDE": "ALERTA VERDE • Sin Riesgo Mayor",
    }
    alert_tag = short_labels.get(level_code, level_label)
    summary_body.append(f"⚠️  {alert_tag}\n\n", style=level_style)

    summary_body.append(f"🌧️ Acumulado Real ({days} Días • 64 tray.):\n", style="bold cyan")
    summary_body.append(f"  • P50 (Mediana):  {metrics['cum_p50']:>6.1f} mm\n", style="bold white")
    summary_body.append(f"  • P90 (Crítico):  {metrics['cum_p90']:>6.1f} mm\n", style="bold orange3")
    summary_body.append(f"  • Máx Miembro:    {metrics['cum_max']:>6.1f} mm\n\n", style="dim")

    summary_body.append("⚡ Pico & Convección:\n", style="bold magenta")
    summary_body.append(f"  • {peak_day_name} {peak_dt_local.strftime('%d/%m %H:%M CLT')} (+{metrics['peak_hours']}h)\n", style="bold white")
    summary_body.append(f"  • P50: {metrics['peak_p50']:.1f} mm (P90: {metrics['peak_p90']:.1f} mm/6h)\n", style="bold white")
    summary_body.append(f"  • {metrics['convective_alert']}\n\n", style="yellow")

    summary_body.append("🏔️ Termodinámica & Viento:\n", style="bold bright_blue")
    summary_body.append(f"  • Isoterma 0°C:   {metrics['iso0_rain_min']}–{metrics['iso0_rain_max']} m\n", style="bold white")
    summary_body.append(f"  • Aluvional:      {metrics['alluvial_risk']}\n", style="bold red" if "ALTO" in metrics["alluvial_risk"] else "bold white")
    summary_body.append(f"  • Ráfagas Est.:   {metrics['max_gust']:.1f} km/h (10m: {metrics['max_wind']:.1f}k)\n", style="bold white")
    summary_body.append(f"  • Presión Mínima: {metrics['min_pressure']:.1f} hPa (-{metrics['pres_drop']:.1f})", style="bold white")

    console.print(Panel(
        summary_body,
        title="[bold]SÍNTESIS DEL TEMPORAL[/bold]",
        border_style=alert_border,
        box=box.ROUNDED
    ))

    # 3. Tabla Diaria Compacta (3 columnas, encaja en <45 caracteres)
    daily = summary.with_columns(
        (pl.col("forecast_time") + SANTIAGO_TZ_OFFSET).dt.date().alias("fecha_local")
    ).group_by("fecha_local").agg([
        pl.col("p50").sum().round(1).alias("lluvia_p50"),
        pl.col("p90").sum().round(1).alias("lluvia_p90"),
    ]).sort("fecha_local")

    daily_table = Table(
        title="📅 Lluvia Acumulada por Día",
        box=box.ROUNDED,
        header_style="bold magenta",
        padding=(0, 1),
        expand=True
    )
    daily_table.add_column("Día", style="bold white", width=11, no_wrap=True)
    daily_table.add_column("P50 / P90", justify="right", style="bold cyan", width=14, no_wrap=True)
    daily_table.add_column("Gráf", justify="center", width=10, no_wrap=True)

    for d in daily.iter_rows(named=True):
        f = d["fecha_local"]
        dia_str = f"{SPANISH_WEEKDAYS[f.weekday()]} {f.day:02d}/{f.month:02d}"
        p50_val = d["lluvia_p50"]
        p90_val = d["lluvia_p90"]
        p_str = f"{p50_val:4.1f}/{p90_val:4.1f}mm"
        bar = render_rain_bar(p50_val, max_scale=35.0, width=8)
        daily_table.add_row(dia_str, p_str, bar)

    daily_table.add_section()
    daily_table.add_row(
        "[bold white]TRAY. 64m[/bold white]",
        f"[bold cyan]{metrics['cum_p50']:4.1f}[/bold cyan]/[bold orange3]{metrics['cum_p90']:4.1f}mm[/bold orange3]",
        ""
    )
    console.print(daily_table)

    # 4. Cronograma Horario Compacto (4 columnas, encaja en <45 caracteres)
    if hours is not None:
        horizon_limit = max(1, hours // 6)
    else:
        horizon_limit = 8  # 48 horas por defecto en celular

    horizon_limit = min(len(summary), horizon_limit)
    hours_shown = horizon_limit * 6

    hourly_table = Table(
        title=f"📊 Próximas {hours_shown} Horas (Pasos 6h)",
        box=box.SIMPLE_HEAVY,
        header_style="bold cyan",
        padding=(0, 1),
        expand=True
    )
    hourly_table.add_column("Hora (CLT)", style="bold white", width=12, no_wrap=True)
    hourly_table.add_column("Prob", justify="right", width=5, no_wrap=True)
    hourly_table.add_column("P50 / P90", justify="right", style="bold cyan", width=13, no_wrap=True)
    hourly_table.add_column("Gráf", justify="center", width=8, no_wrap=True)

    for row in summary.head(horizon_limit).iter_rows(named=True):
        ft_local = row["forecast_time"] + SANTIAGO_TZ_OFFSET
        day_name = SPANISH_WEEKDAYS[ft_local.weekday()]
        h_str = f"{day_name} {ft_local.strftime('%d %H:%M')}"
        prob = row["prob_pct"]
        if prob >= 90:
            prob_txt = Text(f"{prob:3.0f}%", style="bold red")
        elif prob >= 60:
            prob_txt = Text(f"{prob:3.0f}%", style="bold yellow")
        elif prob >= 30:
            prob_txt = Text(f"{prob:3.0f}%", style="cyan")
        else:
            prob_txt = Text(f"{prob:3.0f}%", style="dim")

        p50 = row["p50"]
        p90 = row["p90"]
        rain_txt = f"{p50:4.1f}/{p90:4.1f}mm" if (p50 > 0 or p90 > 0) else "   -   "
        bar = render_rain_bar(p50, max_scale=15.0, width=6)
        hourly_table.add_row(h_str, prob_txt, rain_txt, bar)

    console.print(hourly_table)
    if len(summary) > horizon_limit:
        console.print(f"[dim]ℹ️ {hours_shown}h críticas. Ver todo: '--hours {len(summary)*6}'[/dim]\n")


def _display_tui_desktop(
    summary: pl.DataFrame,
    metrics: dict,
    preset: dict,
    filepath: Path,
    init_str: str,
    level_code: str,
    level_label: str,
    level_style: str,
    days: int,
):
    """Renderizado completo para terminales estándar de escritorio (>=80 columnas)."""
    target_coord = preset["target_cell"]
    elev_m = int(preset.get("base_elev_m", 600))

    # 1. ENCABEZADO
    header_text = Text()
    header_text.append("🌦️  WEATHERNEXT 2.0 ", style="bold cyan")
    header_text.append("• GOOGLE DEEPMIND NEURAL ENSEMBLE\n", style="bold white")
    header_text.append(f"Ubicación: {preset['name']} ({preset['desc']} • Cota ~{elev_m}m)\n", style="bold yellow")
    header_text.append(f"Celda Modelo: ({target_coord[0]:.2f}°, {target_coord[1]:.2f}°) | Corrida: {init_str} | Snapshot: {filepath.name}", style="dim")

    console.print(Panel(header_text, border_style="cyan", box=box.ROUNDED))

    # 2. TARJETAS DE ALERTA Y MÉTRICAS CLAVE
    p50_val = f"{metrics['cum_p50']:.1f} mm"
    p90_val = f"{metrics['cum_p90']:.1f} mm"
    peak_dt = metrics['peak_time']
    peak_dt_local = peak_dt + SANTIAGO_TZ_OFFSET
    peak_day_name = SPANISH_WEEKDAYS[peak_dt_local.weekday()]
    peak_local = f"{peak_day_name} {peak_dt_local.strftime('%d/%m %H:%M CLT')}"
    peak_info = (
        f"{peak_local} (+{metrics['peak_hours']}h) • [bold]{metrics['peak_p50']:.1f} mm[/bold] (P90: {metrics['peak_p90']:.1f} mm/6h, Máx: {metrics['peak_max']:.1f} mm)\n"
        f"[bold yellow]Convección:[/bold yellow] {metrics['convective_alert']}"
    )

    wind_src_tag = "100m+10m" if metrics.get("has_100m_wind") else "proxy 10m"
    alluvial_style = "bold red" if "ALTO" in metrics["alluvial_risk"] else ("bold yellow" if "MODERADO" in metrics["alluvial_risk"] else "bold green")
    synoptic_info = (
        f"Isoterma 0°C (Lluvia): [bold cyan]{metrics['iso0_rain_min']} – {metrics['iso0_rain_max']} m[/bold cyan] (Peak: {metrics['iso0_peak']} m)\n"
        f"Riesgo Aluvional:      [{alluvial_style}]{metrics['alluvial_risk']}[/{alluvial_style}]\n"
        f"Ráfagas P90 ({wind_src_tag}): [bold]{metrics['max_gust']:.1f} km/h[/bold] (Vto 10m: {metrics['max_wind']:.1f} km/h) | Presión Mín: [bold]{metrics['min_pressure']:.1f} hPa[/bold]"
    )

    card_alert = Panel(
        Text(f"\n{level_code}: {level_label}\n", justify="center", style=level_style),
        title="⚠️ Nivel de Alerta Sinóptica",
        border_style="yellow" if "NARANJA" in level_code or "AMARILLA" in level_code else ("red" if "ROJA" in level_code else "green"),
        box=box.ROUNDED
    )

    card_rain = Panel(
        f"[bold cyan]P50 (Mediana 64 tray.):[/bold cyan] [bold white]{p50_val}[/bold white] [dim](P10: {metrics['cum_p10']:.1f} mm)[/dim]\n"
        f"[bold orange3]P90 (Escenario Crítico):[/bold orange3] [bold white]{p90_val}[/bold white] [dim](Máx: {metrics['cum_max']:.1f} mm)[/dim]\n"
        f"[dim]Media Ensamble: {metrics['cum_mean']:.1f} mm | Suma ventanas P50: {metrics['window_sum_p50']:.1f} mm[/dim]",
        title=f"🌧️ Acumulado Real del Evento ({days} Días)",
        border_style="blue",
        box=box.ROUNDED
    )

    card_peak = Panel(
        peak_info,
        title="⚡ Pico del Temporal & Inestabilidad Convectiva",
        border_style="magenta",
        box=box.ROUNDED
    )

    card_syn = Panel(
        synoptic_info,
        title="🏔️ Isoterma 0°C, Ráfagas & Dinámica Atmosférica",
        border_style="bright_blue",
        box=box.ROUNDED
    )

    cards_grid = Table.grid(expand=True, padding=(0, 1))
    cards_grid.add_column(ratio=1)
    cards_grid.add_column(ratio=1)
    cards_grid.add_row(card_alert, card_rain)
    cards_grid.add_row(card_peak, card_syn)
    console.print(cards_grid)

    # 3. CRONOGRAMA DETALLADO (TABLA CON HIDROGRAMA DE BARRAS, ISOTERMA Y RÁFAGAS)
    table = Table(
        title=f"📊 Cronograma 6h en {preset['name']} ({days} Días)",
        box=box.SIMPLE_HEAVY,
        header_style="bold cyan",
        show_lines=False,
        padding=(0, 1),
        pad_edge=False,
    )

    table.add_column("Hora CLT", style="bold white", no_wrap=True)
    table.add_column("H+", justify="right", style="cyan", no_wrap=True)
    table.add_column("Prob", justify="right", no_wrap=True)
    table.add_column("Bar", no_wrap=True)
    table.add_column("P50/P90(Máx)", justify="right", style="bold cyan", no_wrap=True)
    table.add_column("T°/Iso0°", justify="right", no_wrap=True)
    table.add_column("Ráf", justify="right", no_wrap=True)
    table.add_column("Pres", justify="right", no_wrap=True)

    prev_pres = None
    for row in summary.iter_rows(named=True):
        ft_utc = row["forecast_time"]
        ft_local = ft_utc + SANTIAGO_TZ_OFFSET
        day_name = SPANISH_WEEKDAYS[ft_local.weekday()]
        local_str = f"{day_name} {ft_local.day:02d} {ft_local.hour:02d}h"
        h = f"+{row['forecast_hours']:02d}h"
        
        prob = row["prob_pct"]
        if prob >= 90:
            prob_txt = Text(f"{prob:3.0f}%", style="bold red")
        elif prob >= 60:
            prob_txt = Text(f"{prob:3.0f}%", style="bold yellow")
        elif prob >= 30:
            prob_txt = Text(f"{prob:3.0f}%", style="cyan")
        else:
            prob_txt = Text(f"{prob:3.0f}%", style="dim")

        bar = render_rain_bar(row["p50"], max_scale=15.0, width=4)

        p50 = row['p50']
        p90 = row['p90']
        pmax = row['max']

        temp_c = row["temp_p50"]
        iso0 = row["iso0_m"]
        is_rainy = prob >= 35 or p50 >= 0.3 or p90 >= 1.5
        if is_rainy:
            iso_style = "bold red" if iso0 >= 2800 else ("yellow" if iso0 >= 2400 else "cyan")
            temp_iso_txt = Text(f"{temp_c:2.0f}°/{iso0:4d}m", style=iso_style)
        else:
            temp_iso_txt = Text(f"{temp_c:2.0f}°/  -  ", style="dim")

        gust_val = row["gust_p90"]
        gust_style = "bold red" if gust_val >= 40 else ("yellow" if gust_val >= 25 else "white")
        wind_s = Text(f"{gust_val:4.1f}k", style=gust_style)

        pres = row["pres_p50"]
        if prev_pres is not None:
            diff = pres - prev_pres
            if diff < -1.5:
                pres_s = Text(f"{pres:4.0f}↓↓", style="bold red")
            elif diff < -0.3:
                pres_s = Text(f"{pres:5.1f}↓", style="yellow")
            elif diff > 0.5:
                pres_s = Text(f"{pres:5.1f}↑", style="green")
            else:
                pres_s = Text(f"{pres:5.1f}=", style="dim")
        else:
            pres_s = Text(f"{pres:5.1f}", style="dim")
        prev_pres = pres

        rain_txt = f"{p50:4.1f}/{p90:4.1f}({pmax:2.0f})" if (p50 > 0 or p90 > 0) else "      -      "
        table.add_row(
            local_str,
            h,
            prob_txt,
            bar,
            rain_txt,
            temp_iso_txt,
            wind_s,
            pres_s
        )

    console.print(table)

    # 4. RESUMEN DÍA POR DÍA (CRONOLÓGICO)
    console.print(f"\n[bold cyan]📅 Desglose Acumulado por Día Calendario en {preset['name']}:[/bold cyan]")
    daily = summary.with_columns(
        (pl.col("forecast_time") + SANTIAGO_TZ_OFFSET).dt.date().alias("fecha_local")
    ).group_by("fecha_local").agg([
        pl.col("p50").sum().round(1).alias("lluvia_p50"),
        pl.col("p90").sum().round(1).alias("lluvia_p90"),
        pl.col("iso0_m").filter((pl.col("prob_pct") >= 35) | (pl.col("p50") >= 0.3)).max().alias("iso0_max_rain"),
        pl.col("iso0_m").filter((pl.col("prob_pct") >= 35) | (pl.col("p50") >= 0.3)).min().alias("iso0_min_rain"),
        pl.col("gust_p90").max().round(1).alias("rafaga_max"),
        pl.col("pres_p50").min().round(1).alias("pres_min"),
    ]).sort("fecha_local")

    daily_table = Table(box=box.ROUNDED, header_style="bold magenta", padding=(0, 1))
    daily_table.add_column("Día", style="bold white", no_wrap=True)
    daily_table.add_column("P50 Día", justify="right", style="bold cyan", no_wrap=True)
    daily_table.add_column("P90 Día", justify="right", style="bold orange3", no_wrap=True)
    daily_table.add_column("Iso 0°", justify="right", no_wrap=True)
    daily_table.add_column("Ráf Máx", justify="right", no_wrap=True)
    daily_table.add_column("Pres Mín", justify="right", no_wrap=True)

    for d in daily.iter_rows(named=True):
        f = d["fecha_local"]
        dia_str = f"{SPANISH_WEEKDAYS[f.weekday()]} {f.day:02d}/{f.month:02d}"
        p50_val = d['lluvia_p50']
        p90_str = f"{d['lluvia_p90']:5.1f}mm"
        
        bar_len = int(min(5, p50_val / 4.0))
        bar_daily = " " + ("█" * bar_len) if bar_len > 0 else ""
        p50_str = f"{p50_val:4.1f}mm{bar_daily}"

        iso_min = d["iso0_min_rain"]
        iso_max = d["iso0_max_rain"]
        if iso_min is not None and iso_max is not None:
            iso_str = f"{int(iso_min)}–{int(iso_max)}m" if iso_min != iso_max else f"{int(iso_max)}m"
        else:
            iso_str = "Sin lluvia"

        daily_table.add_row(
            dia_str,
            p50_str,
            p90_str,
            iso_str,
            f"{d['rafaga_max']:4.1f} km/h",
            f"{d['pres_min']:6.1f}",
        )

    daily_table.add_section()
    daily_table.add_row(
        "[dim]Suma 6h[/dim]",
        f"[dim]{metrics['window_sum_p50']:4.1f}mm[/dim]",
        f"[dim]{metrics['window_sum_p90']:5.1f}mm[/dim]",
        f"[dim]{metrics['iso0_rain_min']}–{metrics['iso0_rain_max']}m[/dim]",
        f"[dim]{metrics['max_gust']:4.1f} km/h[/dim]",
        f"[dim]{metrics['min_pressure']:6.1f}[/dim]",
    )
    daily_table.add_row(
        "[bold white]TRAY. 64m[/bold white]",
        f"[bold cyan]{metrics['cum_p50']:4.1f}mm P50[/bold cyan]",
        f"[bold orange3]{metrics['cum_p90']:5.1f}mm[/bold orange3]",
        f"[bold]Máx:{metrics['cum_max']:5.1f}[/bold]",
        f"[bold]{metrics['max_gust']:4.1f} km/h[/bold]",
        f"[bold]{metrics['min_pressure']:6.1f}[/bold]",
    )
    console.print(daily_table)


def display_wn3_tui(
    location_key: str = "casa",
    is_mobile: bool = False,
    hours: int | None = None,
    hourly: bool = False,
):
    """Renderiza el dashboard de alta resolución WeatherNext 3.0 (0.05° / 5km & 0.1° / 10km, paso 1h, 250 miembros)."""
    wn3_path = Path("data/weathernext3_latest.parquet")
    if not wn3_path.exists():
        console.print("[bold red]Error:[/bold red] No existe data/weathernext3_latest.parquet. Ejecuta './storm --wn3 --fetch-wn3'.")
        sys.exit(1)

    raw_df = pl.read_parquet(wn3_path)
    clean_key = location_key.lower().replace("-", "_")
    preset = PRESETS.get(clean_key, PRESETS["casa"])
    target_5km = preset.get("target_5km", (-33.45, -70.60))
    elev_m = int(preset.get("base_elev_m", 600))

    sub = raw_df.filter(
        (pl.col("lat").round(2) == round(target_5km[0], 2))
        & (pl.col("lon").round(2) == round(target_5km[1], 2))
    ).sort("lead_time_hours")

    if len(sub) == 0:
        console.print(f"[bold red]Error:[/bold red] Coordenada 5 km {target_5km} no encontrada en {wn3_path.name}.")
        sys.exit(1)

    init_dt = sub["init_time"][0]
    init_str = init_dt.strftime("%Y-%m-%d %H:%M UTC") if isinstance(init_dt, datetime) else str(init_dt)[:16]
    lat_10k = float(sub["lat_10km"][0])
    lon_10k = float(sub["lon_10km"][0])

    # Cálculo de acumulados reales de 250 miembros (Esperanza matemática lineal = suma de medias horarias)
    tp_cum_mean = float(sub["tp_1h_mean_mm"].sum()) if "tp_1h_mean_mm" in sub.columns else float(sub["tp_1h_p50_mm"].sum())
    tp_cum_p75 = float(sub["tp_1h_p75_mm"].sum()) if "tp_1h_p75_mm" in sub.columns else float(sub["tp_1h_p90_mm"].sum() * 0.75)
    tp_cum_p90 = float(sub["tp_1h_p90_mm"].sum())
    imerg_cum_mean = float(sub["imerg_1h_mean_mm"].sum()) if "imerg_1h_mean_mm" in sub.columns else float(sub["imerg_1h_p50_mm"].sum())
    imerg_cum_p90 = float(sub["imerg_1h_p90_mm"].sum())

    # Enriquecer con Isoterma 0°C activa usando la temperatura nativa de 5 km (station_head_temperature_2m)
    sub = sub.with_columns([
        (pl.lit(float(elev_m)) + pl.col("temp_5km_p50_c") / 0.0065).clip(lower_bound=0.0).round(0).cast(pl.Int64).alias("iso0_5km_m"),
        pl.max_horizontal(
            pl.col("wind_100m_p90_kmh") * 1.05,
            pl.col("wind_10m_p90_kmh") * 1.35,
            pl.col("wind_10m_p50_kmh") * 1.30 + pl.col("imerg_1h_p90_mm") * 1.2,
        ).round(1).alias("gust_kmh"),
    ])

    peak_row = sub.sort("imerg_1h_p90_mm", descending=True).row(0, named=True)
    peak_dt_local = peak_row["end_time"] + SANTIAGO_TZ_OFFSET
    peak_day_name = SPANISH_WEEKDAYS[peak_dt_local.weekday()]

    rainy_sub = sub.filter((pl.col("tp_1h_mean_mm") >= 0.25) | (pl.col("imerg_1h_p90_mm") >= 1.0))
    if len(rainy_sub) > 0:
        iso_min = int(rainy_sub["iso0_5km_m"].min())
        iso_max = int(rainy_sub["iso0_5km_m"].max())
    else:
        iso_min = int(sub["iso0_5km_m"].min())
        iso_max = int(sub["iso0_5km_m"].max())

    max_gust = float(sub["gust_kmh"].max())
    min_pres = float(sub["pressure_p50_hpa"].min())
    t5_min = float(sub["temp_5km_p50_c"].min())
    t5_max = float(sub["temp_5km_p50_c"].max())

    # Encabezado WeatherNext 3.0
    header_text = Text()
    header_text.append("⚡ WEATHERNEXT 3.0 (HIGH-RES 5km / 10km • 1h • 250 MIEMBROS)\n", style="bold bright_magenta")
    header_text.append(f"Ubicación: {preset['name']} ({preset['desc']} • Cota ~{elev_m}m)\n", style="bold yellow")
    header_text.append(
        f"Celda 5km (T°): ({target_5km[0]:.2f}°, {target_5km[1]:.2f}°) | Celda 10km (Lluvia): ({lat_10k:.2f}°, {lon_10k:.2f}°) | Corrida: {init_str}",
        style="dim",
    )
    console.print(Panel(header_text, border_style="bright_magenta", box=box.ROUNDED))

    # Tarjetas Resumen WN3
    card_accum = Panel(
        f"[bold cyan]ERA5-Head (10km Media 250m):[/bold cyan] [bold white]{tp_cum_mean:.1f} mm[/bold white] [dim](P75: {tp_cum_p75:.1f} mm)[/dim]\n"
        f"[bold bright_magenta]IMERG-Satélite (10km Media):[/bold bright_magenta] [bold white]{imerg_cum_mean:.1f} mm[/bold white] [dim](Suma P90: {imerg_cum_p90:.1f} mm)[/dim]\n"
        f"[dim]Horizonte: 120 horas (5 días a paso de 1h sin suavizado de 6h)[/dim]",
        title="🌧️ Acumulado 5 Días (250 Trayectorias WN3)",
        border_style="cyan",
        box=box.ROUNDED,
    )

    iso_risk_tag = "ALTO (>2800m)" if iso_max >= 2800 else ("MODERADO" if iso_max >= 2400 else "NORMAL (Nieve Andina)")
    iso_risk_style = "bold red" if iso_max >= 2800 else ("bold yellow" if iso_max >= 2400 else "bold green")
    card_meso = Panel(
        f"[bold magenta]Hora Pico (1h):[/bold magenta] {peak_day_name} {peak_dt_local.strftime('%d/%m %H:%M CLT')} (+{peak_row['lead_time_hours']}h) • "
        f"[bold]P50: {peak_row['imerg_1h_p50_mm']:.1f} mm/h | P90: {peak_row['imerg_1h_p90_mm']:.1f} mm/h[/bold]\n"
        f"[bold cyan]Temp 5km (Station-Head):[/bold cyan] {t5_min:.1f}°C a {t5_max:.1f}°C | "
        f"[bold]Iso 0°C:[/bold] {iso_min}–{iso_max} m ([{iso_risk_style}]{iso_risk_tag}[/{iso_risk_style}])\n"
        f"[bold]Ráfagas P90 (100m):[/bold] {max_gust:.1f} km/h | [bold]Presión Mín:[/bold] {min_pres:.1f} hPa",
        title="🏔️ Mesoescala 5 km & Intensidad Horaria (1h)",
        border_style="bright_magenta",
        box=box.ROUNDED,
    )

    if is_mobile:
        console.print(card_accum)
        console.print(card_meso)
    else:
        grid = Table.grid(expand=True, padding=(0, 1))
        grid.add_column(ratio=1)
        grid.add_column(ratio=1)
        grid.add_row(card_accum, card_meso)
        console.print(grid)

    # Tabla 1: Matriz Microclimática de la Cuenca (5 km / 10 km)
    basin_keys = ["poniente", "casa", "trabajo", "pie_andino", "maipo", "cordillera", "rancagua", "valpo", "serena"]
    basin_table = Table(
        title="🗺️  Gradiente Topográfico WeatherNext 3.0 (Celdas 5 km & 10 km • 5 Días)",
        box=box.SIMPLE_HEAVY,
        header_style="bold bright_magenta",
        padding=(0, 1),
        pad_edge=False,
    )
    basin_table.add_column("Zona", style="bold white", no_wrap=True)
    basin_table.add_column("5km", style="dim", no_wrap=True)
    basin_table.add_column("TP(mm)", justify="right", style="bold cyan", no_wrap=True)
    basin_table.add_column("IM_M/P90", justify="right", style="bright_magenta", no_wrap=True)
    basin_table.add_column("Pk1h", justify="right", style="bold yellow", no_wrap=True)
    basin_table.add_column("T°5km", justify="right", no_wrap=True)
    basin_table.add_column("Vto", justify="right", no_wrap=True)

    short_names = {
        "poniente": "Stgo Centro/Pon.",
        "casa": "Ñuñoa (Casa)",
        "trabajo": "Las Condes",
        "pie_andino": "Pie Andino(Reina)",
        "maipo": "San José de Maipo",
        "cordillera": "Farellones(2400m)",
        "rancagua": "Rancagua",
        "valpo": "Valparaíso/Viña",
        "serena": "La Serena/Coquim.",
    }

    for bk in basin_keys:
        bp = PRESETS[bk]
        c5 = bp["target_5km"]
        bsub = raw_df.filter(
            (pl.col("lat").round(2) == round(c5[0], 2))
            & (pl.col("lon").round(2) == round(c5[1], 2))
        )
        if len(bsub) == 0:
            continue
        b_tp = float(bsub["tp_1h_mean_mm"].sum())
        b_im_m = float(bsub["imerg_1h_mean_mm"].sum())
        b_im_90 = float(bsub["imerg_1h_p90_mm"].sum())
        b_pk1h = float(bsub["imerg_1h_p90_mm"].max())
        b_tmin = float(bsub["temp_5km_p50_c"].min())
        b_tmax = float(bsub["temp_5km_p50_c"].max())
        b_w100 = float(bsub["wind_100m_p90_kmh"].max())
        marker = "▶" if bk == clean_key else " "
        basin_table.add_row(
            f"{marker}{short_names.get(bk, bp['name'][:17])}",
            f"{c5[0]:.2f},{c5[1]:.2f}",
            f"{b_tp:5.1f}",
            f"{b_im_m:4.1f}/{b_im_90:3.0f}",
            f"{b_pk1h:4.1f}",
            f"{b_tmin:2.0f}..{b_tmax:2.0f}°",
            f"{b_w100:2.0f}k",
        )
    console.print(basin_table)

    # Tabla 2: Cronograma en bloques de 3h (o 1h si se especifica --hourly o --hours <= 48)
    step_h = 1 if (hourly or (hours is not None and hours <= 48)) else 3
    max_h = hours if hours is not None else 120
    sub_h = sub.filter(pl.col("lead_time_hours") <= max_h).with_columns(
        ((pl.col("lead_time_hours") - 1) // step_h).alias("block_id")
    )
    blocks = sub_h.group_by("block_id").agg([
        pl.col("end_time").last().alias("end_time"),
        pl.col("lead_time_hours").last().alias("lead_h"),
        pl.col("tp_1h_mean_mm").sum().round(1).alias("tp_mean"),
        pl.col("imerg_1h_p50_mm").sum().round(1).alias("im_p50"),
        pl.col("imerg_1h_p90_mm").sum().round(1).alias("im_p90"),
        pl.col("imerg_1h_p90_mm").max().round(1).alias("im_peak1h"),
        pl.col("temp_5km_p50_c").mean().round(1).alias("t5_c"),
        pl.col("iso0_5km_m").mean().round(0).cast(pl.Int64).alias("iso0_m"),
        pl.col("gust_kmh").max().round(1).alias("gust"),
        pl.col("pressure_p50_hpa").min().round(1).alias("pres"),
    ]).sort("lead_h")

    sched_table = Table(
        title=f"⏱️  Cronograma Mesoescala WeatherNext 3.0 en {preset['name']} (Pasos de {step_h}h)",
        box=box.SIMPLE_HEAVY,
        header_style="bold cyan",
        padding=(0, 1),
        pad_edge=False,
    )
    sched_table.add_column("Hora CLT", style="bold white", no_wrap=True)
    sched_table.add_column("H+", justify="right", style="cyan", no_wrap=True)
    sched_table.add_column("Bar", no_wrap=True)
    sched_table.add_column(f"TP/IM90({step_h}h)", justify="right", style="bold cyan", no_wrap=True)
    sched_table.add_column("Pico1h", justify="right", style="bold magenta", no_wrap=True)
    sched_table.add_column("T°5k/Iso0°", justify="right", no_wrap=True)
    sched_table.add_column("Ráf", justify="right", no_wrap=True)
    sched_table.add_column("Pres", justify="right", no_wrap=True)

    for r in blocks.iter_rows(named=True):
        ft_local = r["end_time"] + SANTIAGO_TZ_OFFSET
        day_name = SPANISH_WEEKDAYS[ft_local.weekday()]
        local_str = f"{day_name} {ft_local.day:02d} {ft_local.hour:02d}h"
        h_str = f"+{r['lead_h']:02d}h"
        tp_m = r["tp_mean"]
        im_90 = r["im_p90"]
        pk1 = r["im_peak1h"]
        bar = render_rain_bar(tp_m, max_scale=4.0 * step_h, width=4)
        rain_str = f"{tp_m:4.1f}/{im_90:4.1f}mm" if (tp_m >= 0.1 or im_90 >= 0.2) else "     -     "
        pk_str = f"{pk1:4.1f}mm" if pk1 >= 0.2 else "  -   "
        is_rainy = tp_m >= 0.25 or im_90 >= 1.0
        iso0 = int(r["iso0_m"])
        if is_rainy:
            iso_style = "bold red" if iso0 >= 2800 else ("yellow" if iso0 >= 2400 else "cyan")
            t_iso = Text(f"{r['t5_c']:3.0f}°/{iso0:4d}m", style=iso_style)
        else:
            t_iso = Text(f"{r['t5_c']:3.0f}°/  -  ", style="dim")
        sched_table.add_row(
            local_str,
            h_str,
            bar,
            rain_str,
            pk_str,
            t_iso,
            f"{r['gust']:4.1f}k",
            f"{r['pres']:6.1f}",
        )
    console.print(sched_table)


def display_tui(
    parquet_path: str | None = None,
    location_key: str = "casa",
    days: int = 7,
    make_html: bool = True,
    is_mobile: bool = False,
    hours: int | None = None,
):
    """Renderiza el dashboard interactivo en la terminal con Rich."""
    raw_df, filepath = load_latest_snapshot(parquet_path)
    df, preset = filter_by_location(raw_df, preset_key=location_key)

    max_hours = days * 24
    base_elev_m = preset.get("base_elev_m", 600.0)
    summary, metrics = analyze_storm(df, max_hours=max_hours, base_elev_m=base_elev_m)
    level_code, level_label, level_style = determine_alert_level(metrics)

    init_dt = metrics["init_time"]
    init_str = init_dt.strftime("%Y-%m-%d %H:%M UTC") if isinstance(init_dt, datetime) else str(init_dt)[:16]

    if is_mobile:
        _display_tui_mobile(
            summary=summary,
            metrics=metrics,
            preset=preset,
            init_str=init_str,
            level_code=level_code,
            level_label=level_label,
            level_style=level_style,
            days=days,
            hours=hours,
        )
    else:
        _display_tui_desktop(
            summary=summary,
            metrics=metrics,
            preset=preset,
            filepath=filepath,
            init_str=init_str,
            level_code=level_code,
            level_label=level_label,
            level_style=level_style,
            days=days,
        )

    # 5. GENERACIÓN HTML
    if make_html:
        html_file = generate_html_spaghetti_plot(df, location_name=preset['name'])
        if html_file:
            if is_mobile:
                console.print(f"[bold green]✓ Gráfico HTML:[/bold green] [underline cyan]{html_file.name}[/underline cyan]")
            else:
                console.print(f"\n[bold green]✓ Gráfico HTML Interactivo (Spaghetti Plot):[/bold green] [underline cyan]{html_file.resolve()}[/underline cyan]")


def main():
    parser = argparse.ArgumentParser(description="WeatherNext Storm TUI — Analizador de Temporales")
    parser.add_argument("--file", "-f", help="Ruta a un archivo Parquet específico", default=None)
    parser.add_argument("--days", "-d", type=int, default=7, help="Días de pronóstico a visualizar (defecto: 7)")
    parser.add_argument("--hours", type=int, default=None, help="Número de horas a mostrar en el cronograma (ej: 36, 48)")
    parser.add_argument("--hourly", "-1", action="store_true", help="Mostrar cronograma hora a hora (paso de 1h) en WeatherNext 3.0")
    parser.add_argument("--mobile", "-m", action="store_true", help="Formato optimizado para celular / pantallas estrechas (<65 columnas)")
    parser.add_argument("--no-html", action="store_true", help="No generar el archivo HTML")
    parser.add_argument("--fetch", action="store_true", help="Extraer primero el snapshot más reciente de WeatherNext 2 desde BigQuery")
    parser.add_argument("--wn3", "-3", action="store_true", help="Usar WeatherNext 3.0 (0.05°/5km & 0.1°/10km, paso 1h, 250 miembros)")
    parser.add_argument("--fetch-wn3", action="store_true", help="Extraer primero la última corrida de WeatherNext 3.0 desde GCS Zarr v3")

    # Modos de ubicación
    parser.add_argument("--casa", action="store_true", help="Pronóstico para Ñuñoa (Casa)")
    parser.add_argument("--trabajo", action="store_true", help="Pronóstico para Las Condes (Trabajo)")
    parser.add_argument("--poniente", action="store_true", help="Pronóstico para Santiago Centro / Poniente")
    parser.add_argument("--cordillera", action="store_true", help="Pronóstico para Precordillera / Farellones / Valle Nevado")
    parser.add_argument("--pie-andino", action="store_true", help="Pronóstico para Pie Andino (La Reina / Peñalolén)")
    parser.add_argument("--maipo", action="store_true", help="Pronóstico para San José de Maipo (Cajón del Maipo)")
    parser.add_argument("--norte", action="store_true", help="Pronóstico para Santiago Norte (Chicureo / Colina)")
    parser.add_argument("--lugar", "-l", help="Alias de ubicación (casa, trabajo, poniente, pie_andino, maipo, cordillera, valpo, rancagua, serena)", default=None)
    parser.add_argument("--commute", "-c", action="store_true", help="Modo especial de Traslado Diario (Ñuñoa ⇄ Las Condes)")
    parser.add_argument("--clusters", "-k", action="store_true", help="Analizar clusters temporales de miembros para el pico del temporal")

    args = parser.parse_args()

    # Detección automática o explícita de terminal móvil
    global console
    is_mobile = args.mobile or (console.is_terminal and console.width < 68)
    if is_mobile:
        target_width = 50 if (args.mobile and console.width >= 68) else console.width
        console = Console(width=target_width, height=console.size.height or 40)
    elif console.width < 96:
        console = Console(width=96)

    if args.fetch:
        if args.wn3:
            console.print("[bold bright_magenta]Extrayendo última corrida de WeatherNext 3.0 (5km/10km) desde GCS Zarr v3...[/bold bright_magenta]")
            from query_weathernext3 import extract_weathernext3
            extract_weathernext3(hours=args.hours or 120)
        else:
            console.print("[bold yellow]Extrayendo última corrida de WeatherNext 2 desde BigQuery...[/bold yellow]")
            from query_weathernext import fetch_weather_data
            fetch_weather_data()

    if args.fetch_wn3 and not (args.fetch and args.wn3):
        console.print("[bold bright_magenta]Extrayendo última corrida de WeatherNext 3.0 (5km/10km) desde GCS Zarr v3...[/bold bright_magenta]")
        from query_weathernext3 import extract_weathernext3
        extract_weathernext3(hours=args.hours or 120)

    # Si se pide el modo commute
    if args.commute:
        raw_df, _ = load_latest_snapshot(args.file)
        display_commute_view(raw_df, is_mobile=is_mobile)
        return

    # Determinar lugar
    if args.trabajo:
        loc = "trabajo"
    elif args.poniente:
        loc = "poniente"
    elif args.cordillera:
        loc = "cordillera"
    elif args.pie_andino:
        loc = "pie_andino"
    elif args.maipo:
        loc = "maipo"
    elif args.norte:
        loc = "norte"
    elif args.lugar:
        loc = args.lugar
    else:
        loc = "casa" # Ñuñoa por defecto

    # Si se pide el modo clusters de miembros
    if args.clusters:
        display_clusters_view(
            parquet_path=args.file,
            location_key=loc,
            is_mobile=is_mobile,
            max_hours=args.hours or 72,
        )
        return

    if args.wn3 or args.fetch_wn3:
        display_wn3_tui(location_key=loc, is_mobile=is_mobile, hours=args.hours, hourly=args.hourly)
        return

    display_tui(
        parquet_path=args.file,
        location_key=loc,
        days=args.days,
        make_html=not args.no_html,
        is_mobile=is_mobile,
        hours=args.hours,
    )


if __name__ == "__main__":
    main()
