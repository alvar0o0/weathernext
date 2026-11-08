#!/usr/bin/env python3
"""
WEATHER APP TUI — Telemetría Física Pura & Caipirinha Beach Index.
Interfaz visual optimizada para terminales móviles y SSH (<50 columnas),
con telemetría in situ, carrusel horario (score activo solo en horas de sol),
tendencia semanal con barras térmicas y alertas cuantitativas con cero texto de relleno.
"""

import argparse
from datetime import datetime, timedelta, date
from pathlib import Path
import sys

import numpy as np
import polars as pl
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box

console = Console()

SANTIAGO_TZ_OFFSET = timedelta(hours=-3)
SPANISH_WEEKDAYS = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
SPANISH_MONTHS = ["", "Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

PRESETS = {
    "rio": {
        "alias": "rio",
        "name": "Río de Janeiro (Copacabana / Ipanema)",
        "city": "Río de Janeiro",
        "region": "Zona Sul • Brasil",
        "coord": (-22.90, -43.20),
        "is_brasil": True,
    },
    "copacabana": {
        "alias": "copacabana",
        "name": "Copacabana / Ipanema (Zona Sul)",
        "city": "Copacabana",
        "region": "Zona Sul • Río de Janeiro",
        "coord": (-22.90, -43.20),
        "is_brasil": True,
    },
    "ipanema": {
        "alias": "ipanema",
        "name": "Copacabana / Ipanema (Zona Sul)",
        "city": "Ipanema",
        "region": "Zona Sul • Río de Janeiro",
        "coord": (-22.90, -43.20),
        "is_brasil": True,
    },
    "arpoador": {
        "alias": "arpoador",
        "name": "Copacabana / Ipanema (Zona Sul)",
        "city": "Arpoador",
        "region": "Zona Sul • Río de Janeiro",
        "coord": (-22.90, -43.20),
        "is_brasil": True,
    },
    "ilha_grande": {
        "alias": "ilha_grande",
        "name": "Ilha Grande (Vila do Abraão)",
        "city": "Ilha Grande",
        "region": "Costa Verde • Angra dos Reis",
        "coord": (-23.10, -44.20),
        "is_brasil": True,
    },
    "angra": {
        "alias": "angra",
        "name": "Angra dos Reis (Puerto)",
        "city": "Angra dos Reis",
        "region": "Costa Verde Fluminense",
        "coord": (-23.00, -44.30),
        "is_brasil": True,
    },
    "paraty": {
        "alias": "paraty",
        "name": "Paraty Histórico",
        "city": "Paraty",
        "region": "Costa Verde Colonial",
        "coord": (-23.20, -44.70),
        "is_brasil": True,
    },
    "buzios": {
        "alias": "buzios",
        "name": "Armação dos Búzios",
        "city": "Búzios",
        "region": "Região dos Lagos • Brasil",
        "coord": (-22.70, -41.90),
        "is_brasil": True,
    },
    "casa": {
        "alias": "casa",
        "name": "Ñuñoa (Casa)",
        "city": "Santiago",
        "region": "Santiago Oriente • Chile",
        "coord": (-33.45, -70.60),
        "is_brasil": False,
    },
    "trabajo": {
        "alias": "trabajo",
        "name": "Las Condes (Trabajo)",
        "city": "Santiago",
        "region": "Sector Oriente • Chile",
        "coord": (-33.40, -70.55),
        "is_brasil": False,
    },
    "valpo": {
        "alias": "valpo",
        "name": "Valparaíso / Viña del Mar",
        "city": "Viña del Mar",
        "region": "Litoral Central • Chile",
        "coord": (-33.05, -71.60),
        "is_brasil": False,
    },
}


def get_weather_condition(
    tp_mm: float,
    im_p90: float,
    mslp_hpa: float,
    gust_kmh: float,
    hour: int,
    tcc_pct: float | None = None,
    lcc_pct: float | None = None,
    ssrd_wm2: float | None = None,
) -> tuple[str, str, str]:
    """Determina icono, etiqueta técnica y estilo según lluvia, radiación solar, nubes y viento."""
    is_night = hour < 6 or hour >= 18

    if im_p90 >= 5.0 or tp_mm >= 3.5:
        return ("⛈️", "Tormenta / Lluvia Intensa", "bold red")
    elif im_p90 >= 1.5 or tp_mm >= 1.0:
        return ("🌧️", "Lluvia Activa", "bold blue")
    elif im_p90 >= 0.3 or tp_mm >= 0.2:
        return ("🌦️", "Chubascos Aislados", "bold yellow")

    # Si hay telemetría real de nubosidad y radiación solar:
    if tcc_pct is not None:
        if is_night:
            if tcc_pct < 40.0:
                return ("🌙", "Noche Despejada", "bright_blue")
            elif tcc_pct < 80.0:
                return ("☁️", "Noche Nublada", "white")
            else:
                return ("☁️", "Noche Cubierta", "dim white")
        else:
            # Horario diurno: evaluar nubes bajas, cobertura total y radiación solar
            if (lcc_pct is not None and lcc_pct >= 70.0) or tcc_pct >= 85.0 or (ssrd_wm2 is not None and ssrd_wm2 < 220.0 and 10 <= hour <= 15):
                return ("☁️", "Nublado / Cubierto", "white")
            elif tcc_pct >= 65.0 or (lcc_pct is not None and lcc_pct >= 45.0):
                return ("⛅", "Parcialmente Nublado", "cyan")
            elif tcc_pct >= 35.0 or (lcc_pct is not None and lcc_pct >= 20.0):
                return ("⛅", "Mayormente Soleado", "cyan")
            else:
                return ("☀️", "Soleado & Despejado", "bold yellow")

    # Fallback si no hay tensores de nubes (snapshots antiguos de Chile)
    if mslp_hpa < 1012.0 and gust_kmh >= 28.0:
        return ("☁️", "Nublado / Ventoso", "white")
    elif mslp_hpa < 1014.0:
        return ("⛅", "Parcialmente Nublado", "cyan")
    else:
        if is_night:
            return ("🌙", "Noche Despejada", "bright_blue")
        else:
            return ("☀️", "Soleado & Despejado", "bold yellow")


def calculate_caipirinha_index(
    temp_max: float,
    temp_beach_avg: float,
    rain_beach: float,
    rain_day: float,
    max_gust: float,
    mslp_hpa: float,
    wind_10m: float | None = None,
    imerg_p90: float | None = None,
    tcc_pct: float | None = None,
    lcc_pct: float | None = None,
    ssrd_wm2: float | None = None,
) -> tuple[float, str, str, dict, str]:
    """
    Calcula el 'Índice de Caipirinha & Playa' de 1.0 a 10.0 calibrado con telemetría física in situ.
    Penaliza: lluvia en playa, lluvia residual, ráfagas/viento costero, falta de radiación solar (nubes)
    y el Wind Chill Playero (sensación de enfriamiento en traje de baño con viento/falta de sol bajo 27°C).
    Retorna: (score, tag, badge_style, penalties_dict, bar_str)
    """
    score = 10.0

    # 1. Penalización Lluvia en horario solar de playa (o acumulado horario)
    pen_rain_beach = 0.0
    if rain_beach >= 5.0 or (imerg_p90 is not None and imerg_p90 >= 4.0):
        pen_rain_beach = 6.5
    elif rain_beach >= 1.5 or (imerg_p90 is not None and imerg_p90 >= 1.5):
        pen_rain_beach = 4.5
    elif rain_beach >= 0.3 or (imerg_p90 is not None and imerg_p90 >= 0.3):
        pen_rain_beach = 2.5
    elif rain_beach > 0.05:
        pen_rain_beach = 1.0
    score -= pen_rain_beach

    # 2. Penalización Lluvia residual en el resto del día (solo si rain_beach es baja)
    pen_rain_day = 0.0
    if rain_beach < 1.0:
        if rain_day >= 15.0:
            pen_rain_day = 1.5
        elif rain_day >= 5.0:
            pen_rain_day = 0.8
    score -= pen_rain_day

    # 3. Penalización Viento Costero (sensibilidad de playa: sombrillas, arena volando y oleaje)
    pen_wind = 0.0
    eff_vto = wind_10m if wind_10m is not None else (max_gust * 0.55)
    if max_gust > 36.0 or eff_vto > 24.0:
        pen_wind = 3.0
    elif max_gust > 28.0 or eff_vto > 18.0:
        pen_wind = 2.0
    elif max_gust > 22.0 or eff_vto > 14.0:
        pen_wind = 1.0
    elif max_gust > 16.0 or eff_vto > 12.0:
        pen_wind = 0.3
    score -= pen_wind

    # 4. Penalización Nubosidad / Pérdida de Radiación Solar Directa
    pen_cloud = 0.0
    if tcc_pct is not None:
        if (lcc_pct is not None and lcc_pct >= 70.0) or tcc_pct >= 85.0 or (ssrd_wm2 is not None and ssrd_wm2 < 220.0):
            pen_cloud = 2.5  # Nublado cerrado / cubierto
        elif tcc_pct >= 65.0 or (lcc_pct is not None and lcc_pct >= 45.0):
            pen_cloud = 1.5  # Parcialmente nublado / resolana
        elif tcc_pct >= 35.0 or (lcc_pct is not None and lcc_pct >= 20.0):
            pen_cloud = 0.5  # Nubes dispersas / mayormente soleado
        else:
            pen_cloud = 0.0  # Cielo despejado / sol pleno
    else:
        # Fallback al proxy barométrico únicamente si no hay tensores de nubes
        if mslp_hpa < 1012.0 and max_gust >= 28.0:
            pen_cloud = 2.5
        elif mslp_hpa < 1014.5:
            pen_cloud = 1.5
        elif mslp_hpa < 1016.0:
            pen_cloud = 0.5
    score -= pen_cloud

    # 5. Factor Wind Chill Playero (Confort térmico en traje de baño con viento/nubes bajo 27°C)
    t_eval = temp_beach_avg
    pen_chill = 0.0
    if t_eval < 21.0:
        pen_chill = (21.0 - t_eval) * 1.5 + (1.5 if pen_wind >= 1.0 else 0.5)
    elif t_eval < 24.0:
        if pen_wind >= 1.0 or pen_cloud >= 1.0:
            pen_chill = (24.0 - t_eval) * 1.2 + 0.8
        else:
            pen_chill = (24.0 - t_eval) * 0.5
    elif t_eval < 27.0:
        chill_factor = (27.0 - t_eval) / 3.0
        if pen_wind >= 1.0 or pen_cloud >= 1.0:
            pen_chill = chill_factor * 1.5
    elif t_eval > 34.0:
        pen_chill = 0.8
    pen_chill = round(pen_chill, 1)
    score -= pen_chill

    score = round(max(1.0, min(10.0, score)), 1)
    filled = int(round(score))
    bar_str = "█" * filled + "░" * (10 - filled)

    if score >= 9.0:
        tag = "EXCELENTE SOL / PLAYA ÓPTIMA"
        badge_style = "bold green"
    elif score >= 7.5:
        tag = "BUENA CONDICIÓN PLAYERA"
        badge_style = "bold cyan"
    elif score >= 5.5:
        tag = "CONDICIÓN REGULAR / VENTOSO O NUBES"
        badge_style = "bold yellow"
    elif score >= 4.0:
        tag = "INESTABLE / FRÍO O CHUBASCOS"
        badge_style = "bold bright_yellow"
    else:
        tag = "NO APTO PLAYA / INESTABLE"
        badge_style = "bold red"

    penalties = {
        "base": 10.0,
        "rain_beach": pen_rain_beach,
        "rain_day": pen_rain_day,
        "wind": pen_wind,
        "cloud": pen_cloud,
        "chill": pen_chill,
    }
    return score, tag, badge_style, penalties, bar_str


def render_apple_temp_bar(t_min: float, t_max: float, global_min: float, global_max: float, width: int = 12) -> Text:
    """Genera la barra térmica horizontal en degradé al estilo Apple Weather."""
    span = max(1.0, global_max - global_min)
    p_start = max(0.0, min(1.0, (t_min - global_min) / span))
    p_end = max(0.0, min(1.0, (t_max - global_min) / span))

    idx_start = int(round(p_start * (width - 1)))
    idx_end = max(idx_start, int(round(p_end * (width - 1))))

    bar = Text()
    for i in range(width):
        if i < idx_start or i > idx_end:
            bar.append("─", style="dim white")
        else:
            temp_ratio = (global_min + (i / max(1, width - 1)) * span)
            if temp_ratio < 22.0:
                col = "cyan"
            elif temp_ratio < 26.0:
                col = "bright_yellow"
            else:
                col = "bright_red"
            bar.append("█", style=f"bold {col}")
    return bar


def load_weather_data(preset: dict) -> tuple[pl.DataFrame, str]:
    """Carga los datos meteorológicos disponibles (Río o Chile)."""
    if preset["is_brasil"]:
        path = Path("data/weathernext3_rio_latest.parquet")
        if not path.exists():
            console.print("[bold red]Error:[/bold red] No existe data/weathernext3_rio_latest.parquet. Ejecuta './storm --fetch-rio'.")
            sys.exit(1)
    else:
        path = Path("data/weathernext3_latest.parquet")
        if not path.exists():
            console.print("[bold red]Error:[/bold red] No existe data/weathernext3_latest.parquet. Ejecuta './storm -3 --fetch-wn3'.")
            sys.exit(1)

    raw_df = pl.read_parquet(path)
    init_dt = raw_df["init_time"][0]
    init_str = init_dt.strftime("%Y-%m-%d %H:%M UTC") if isinstance(init_dt, datetime) else str(init_dt)[:16]

    coord = preset["coord"]
    df_loc = raw_df.filter(
        (pl.col("lat").round(2) == round(coord[0], 2))
        & (pl.col("lon").round(2) == round(coord[1], 2))
    ).sort("lead_time_hours")

    if len(df_loc) == 0:
        df_loc = raw_df.sort("lead_time_hours")

    # Datetime local en Brasil / Chile (UTC-3)
    df_loc = df_loc.with_columns([
        (pl.col("end_time") + SANTIAGO_TZ_OFFSET).alias("dt_local"),
        (pl.col("end_time") + SANTIAGO_TZ_OFFSET).dt.date().alias("fecha_local"),
        (pl.col("end_time") + SANTIAGO_TZ_OFFSET).dt.hour().alias("hora_local"),
    ])

    return df_loc, init_str


def display_weather_app(location_key: str = "rio", is_mobile: bool = True, target_date_str: str | None = None):
    global console
    target_width = 48 if is_mobile else (max(96, console.width) if console.width else 96)
    console = Console(width=target_width, height=console.size.height or 45)

    clean_key = location_key.lower().replace("-", "_")
    preset = PRESETS.get(clean_key, PRESETS["rio"])
    df, init_str = load_weather_data(preset)

    all_dates = sorted(df["fecha_local"].unique().to_list())
    if not all_dates:
        console.print("[bold red]Error:[/bold red] No hay datos disponibles para la ubicación.")
        sys.exit(1)

    # Seleccionar la fecha
    now_local = (datetime.now() + SANTIAGO_TZ_OFFSET).date()
    if target_date_str:
        try:
            target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
        except ValueError:
            target_date = now_local if now_local in all_dates else all_dates[0]
    else:
        target_date = now_local if now_local in all_dates else all_dates[0]

    sub_today = df.filter(pl.col("fecha_local") == target_date).sort("hora_local")
    if len(sub_today) == 0:
        sub_today = df.slice(0, 24)
        target_date = sub_today["fecha_local"][0]

    # Datos físicos agregados del día
    beach_sub = sub_today.filter((pl.col("hora_local") >= 10) & (pl.col("hora_local") <= 17))
    beach_solar = sub_today.filter((pl.col("hora_local") >= 10) & (pl.col("hora_local") <= 16))
    t_min = float(sub_today["temp_5km_p50_c"].min())
    t_max = float(sub_today["temp_5km_p50_c"].max())
    t_beach_avg = float(beach_sub["temp_5km_p50_c"].mean()) if len(beach_sub) > 0 else t_max
    rain_day = float(sub_today["tp_1h_mean_mm"].sum())
    rain_day_p90 = float(sub_today["imerg_1h_p90_mm"].sum())
    rain_beach = float(beach_sub["tp_1h_mean_mm"].sum()) if len(beach_sub) > 0 else 0.0
    rain_beach_p90 = float(beach_sub["imerg_1h_p90_mm"].sum()) if len(beach_sub) > 0 else 0.0

    # Viento y ráfagas a 10m (arena) vs 100m (altura/morros)
    max_gust_100m = float(sub_today["wind_100m_p90_kmh"].max())
    max_gust_10m = float(beach_sub["wind_10m_p90_kmh"].max()) if len(beach_sub) > 0 else float(sub_today["wind_10m_p90_kmh"].max())
    max_wind_10m = float(beach_sub["wind_10m_p50_kmh"].max()) if len(beach_sub) > 0 else float(sub_today["wind_10m_p50_kmh"].max())
    min_mslp = float(sub_today["pressure_p50_hpa"].min())
    max_mslp = float(sub_today["pressure_p50_hpa"].max())
    delta_p = min_mslp - max_mslp

    # Nubosidad y radiación solar en horario diurno de playa
    has_clouds = "tcc_p50_pct" in sub_today.columns
    if has_clouds and len(beach_solar) > 0:
        day_tcc = float(beach_solar["tcc_p50_pct"].mean())
        day_lcc = float(beach_solar["lcc_p50_pct"].mean())
        day_ssrd = float(beach_solar["ssrd_1h_p50_wm2"].mean())
    elif has_clouds:
        day_tcc = float(sub_today["tcc_p50_pct"].mean())
        day_lcc = float(sub_today["lcc_p50_pct"].mean())
        day_ssrd = float(sub_today["ssrd_1h_p50_wm2"].mean())
    else:
        day_tcc, day_lcc, day_ssrd = None, None, None

    # Humedad relativa y rocío exacto (Magnus-Tetens)
    has_dew = "dewpoint_2m_p50_c" in sub_today.columns
    if has_dew:
        t_dew = float(beach_sub["dewpoint_2m_p50_c"].mean()) if len(beach_sub) > 0 else float(sub_today["dewpoint_2m_p50_c"].mean())
        t_dew = round(t_dew, 1)
        hr_approx = int(round(max(10.0, min(100.0, 100.0 * np.exp((17.625 * t_dew)/(243.04 + t_dew) - (17.625 * t_max)/(243.04 + t_max))))))
    else:
        hr_approx = 71
        t_dew = round(t_max - ((100 - hr_approx) / 5.0), 1)

    heat_index = round(t_max + 1.2, 1)
    vto_kt = round(max_wind_10m / 1.852, 1)

    # Índice Caipirinha del día con calibración física
    c_score, c_tag, c_badge_style, penalties, c_bar = calculate_caipirinha_index(
        temp_max=t_max,
        temp_beach_avg=t_beach_avg,
        rain_beach=rain_beach,
        rain_day=rain_day,
        max_gust=max_gust_10m,
        mslp_hpa=min_mslp,
        wind_10m=max_wind_10m,
        tcc_pct=day_tcc,
        lcc_pct=day_lcc,
        ssrd_wm2=day_ssrd,
    )

    day_name = SPANISH_WEEKDAYS[target_date.weekday()]
    date_title = f"{day_name} {target_date.day:02d} {SPANISH_MONTHS[target_date.month]}"
    rain_beach_tag = "0.0 mm (SECO)" if rain_beach < 0.1 else f"{rain_beach:.1f} mm"

    # =========================================================================
    # 1. HEADER & PANEL DE TELEMETRÍA (Mobile o Desktop)
    # =========================================================================
    if is_mobile:
        console.print(Panel(
            f"[bold bright_magenta]📍 {preset['name']}[/bold bright_magenta]\n"
            f"[bold yellow]{date_title}[/bold yellow] • [dim]Corrida: {init_str[:16]} • WN3 (250m)[/dim]",
            border_style="bright_magenta",
            box=box.ROUNDED,
        ))

        telemetry_mobile = (
            f"[bold cyan]T° 5km:[/bold cyan]    [bold white]{t_max:.1f}°C[/bold white] (Mín {t_min:.1f}° • Máx {t_max:.1f}°)\n"
            f"[bold cyan]Sens / HR:[/bold cyan] [bold white]{heat_index:.1f}°C[/bold white] • HR: ~{hr_approx}% (Rocío {t_dew:.1f}°)\n"
            f"[bold cyan]Vto / Ráf:[/bold cyan] [bold white]{max_wind_10m:.0f} km/h ({vto_kt:.0f} kt)[/bold white] • Ráf: [bold yellow]{max_gust_10m:.0f} km/h[/bold yellow]\n"
            f"[bold cyan]Pres / ΔP:[/bold cyan] [bold white]{min_mslp:.0f} hPa[/bold white] (ΔP/6h: {delta_p:.1f} hPa)\n"
            f"[bold cyan]Lluvia 24h:[/bold cyan][bold white]{rain_day:.1f} mm[/bold white] (IMERG P90: {rain_day_p90:.1f} mm)\n"
            f"[bold cyan]Lluvia 10-17h:[/bold cyan] [bold green]{rain_beach_tag}[/bold green]\n\n"
            f"[bold yellow]SCORE PLAYA:[/bold yellow] [{c_badge_style}]{c_score:.1f} / 10.0[/{c_badge_style}] [{c_bar}]\n"
            f"[dim]Base: 10.0 | Lluv: -{penalties['rain_beach']:.1f} | Vto: -{penalties['wind']:.1f} | Nub: -{penalties['cloud']:.1f} | Frío: -{penalties['chill']:.1f}[/dim]"
        )
        console.print(Panel(telemetry_mobile, title="📊 TELEMETRÍA PLAYA & SCORE", border_style="cyan", box=box.ROUNDED))
    else:
        header_text = Text()
        header_text.append("⚡  WEATHERNEXT 3.0 • DATA PLAYA & TELEMETRÍA DE MESOESCALA\n", style="bold bright_magenta")
        header_text.append(f"📍 {preset['name']} • {date_title}\n", style="bold yellow")
        header_text.append(f"Modelo: Google DeepMind WN3 (0.1° / 10 km • 250 miembros) | Corrida: {init_str}", style="dim")
        console.print(Panel(header_text, border_style="bright_magenta", box=box.ROUNDED))

        telemetry_left = (
            f"[bold cyan]Temperatura 5km:[/bold cyan]    [bold white]{t_max:.1f}°C[/bold white] (Mín: {t_min:.1f}°C • Máx: {t_max:.1f}°C)\n"
            f"[bold cyan]Sensación / Rocío:[/bold cyan]   [bold white]{heat_index:.1f}°C[/bold white] / [dim]{t_dew:.1f}°C (HR: ~{hr_approx}%)[/dim]\n"
            f"[bold cyan]Viento Superficie:[/bold cyan]   [bold white]{max_wind_10m:.1f} km/h[/bold white] ({vto_kt:.1f} kt) • SE\n"
            f"[bold cyan]Ráfagas P90:[/bold cyan]         [bold white]{max_gust_10m:.1f} km/h[/bold white] (10m) / [dim]{max_gust_100m:.1f}k (100m)[/dim]\n"
            f"[bold cyan]Presión Barométrica:[/bold cyan] [bold white]{min_mslp:.1f} hPa[/bold white] (ΔP/6h: {delta_p:.1f} hPa)\n"
            f"[bold cyan]Precipitación Día:[/bold cyan]   [bold white]{rain_day:.1f} mm[/bold white] (Media) • [dim]IMERG P90: {rain_day_p90:.1f} mm[/dim]\n"
            f"[bold cyan]Lluvia Playa 10-17h:[/bold cyan] [bold green]{rain_beach_tag}[/bold green]"
        )
        telemetry_right = (
            f"[bold yellow]SCORE PLAYA & CAIPIRINHA:[/bold yellow] [{c_badge_style}]{c_score:.1f} / 10.0[/{c_badge_style}]\n"
            f"[bold cyan][{c_bar}] {(c_score*10):.0f}% CONDICIÓN PLAYERA[/bold cyan]\n\n"
            f"[bold white]Desglose Numérico de Deducciones:[/bold white]\n"
            f"  • Base Teórica Óptima:             [bold white]10.0 pts[/bold white]\n"
            f"  • Lluvia Horario Playa (10-17h):    [bold green]-{penalties['rain_beach']:.1f} pts[/bold green]\n"
            f"  • Viento / Ráfagas Costeras (10m):  [bold yellow]-{penalties['wind']:.1f} pts[/bold yellow]\n"
            f"  • Nubosidad / Falta de Sol:         [bold yellow]-{penalties['cloud']:.1f} pts[/bold yellow]\n"
            f"  • Wind Chill / Frío en Playa:       [bold yellow]-{penalties['chill']:.1f} pts[/bold yellow]\n\n"
            f"[bold]Diagnóstico Técnico:[/bold] [{c_badge_style}]{c_tag}[/{c_badge_style}]"
        )
        grid = Table.grid(expand=True, padding=(0, 1))
        grid.add_column(ratio=5)
        grid.add_column(ratio=5)
        grid.add_row(
            Panel(telemetry_left, title="📊 TELEMETRÍA FÍSICA IN SITU", border_style="cyan", box=box.ROUNDED),
            Panel(telemetry_right, title="🍹 DESGLOSE MATEMÁTICO DEL SCORE", border_style="yellow", box=box.ROUNDED),
        )
        console.print(grid)

    # =========================================================================
    # 2. DETALLE HORA A HORA (SCORE ASIGNADO ÚNICAMENTE EN HORAS DE SOL 08:00 A 18:00)
    # =========================================================================
    # Filtrar ventana activa de 08:00 a 21:00
    hours_to_show = [h for h in sub_today.iter_rows(named=True) if 8 <= h["hora_local"] <= 21]
    if not hours_to_show:
        hours_to_show = sub_today.iter_rows(named=True)

    if is_mobile:
        h_table = Table(
            title=f"⏱️ DETALLE HORA A HORA • {date_title}",
            box=box.ROUNDED,
            header_style="bold cyan",
            padding=(0, 0),
            expand=True,
        )
        h_table.add_column("Hora", width=5, no_wrap=True)
        h_table.add_column("Cielo", justify="center", width=4, no_wrap=True)
        h_table.add_column("T°", justify="center", width=4, no_wrap=True)
        h_table.add_column("Lluvia", justify="right", style="cyan", width=6, no_wrap=True)
        h_table.add_column("Vto/Ráf", justify="right", width=9, no_wrap=True)
        h_table.add_column("Score", justify="center", width=6, no_wrap=True)

        for hr in hours_to_show:
            hr_tcc = hr.get("tcc_p50_pct")
            hr_lcc = hr.get("lcc_p50_pct")
            hr_ssrd = hr.get("ssrd_1h_p50_wm2")
            h_icon, _, _ = get_weather_condition(
                hr["tp_1h_mean_mm"],
                hr["imerg_1h_p90_mm"],
                hr["pressure_p50_hpa"],
                hr["wind_10m_p90_kmh"],
                hr["hora_local"],
                tcc_pct=hr_tcc,
                lcc_pct=hr_lcc,
                ssrd_wm2=hr_ssrd,
            )
            is_solar_beach = 8 <= hr["hora_local"] <= 18
            if is_solar_beach:
                h_score, _, h_style, _, _ = calculate_caipirinha_index(
                    temp_max=hr["temp_5km_p50_c"],
                    temp_beach_avg=hr["temp_5km_p50_c"],
                    rain_beach=hr["tp_1h_mean_mm"],
                    rain_day=hr["tp_1h_mean_mm"],
                    max_gust=hr["wind_10m_p90_kmh"],
                    mslp_hpa=hr["pressure_p50_hpa"],
                    wind_10m=hr["wind_10m_p50_kmh"],
                    imerg_p90=hr["imerg_1h_p90_mm"],
                    tcc_pct=hr_tcc,
                    lcc_pct=hr_lcc,
                    ssrd_wm2=hr_ssrd,
                )
                score_str = f"{h_score:.1f}" if h_score < 10.0 else "10/10"
                score_widget = Text(score_str, style=h_style)
            else:
                score_widget = Text("-", style="dim")

            r_str = f"{hr['tp_1h_mean_mm']:.1f}m" if hr["tp_1h_mean_mm"] >= 0.1 else " -  "
            vto_str = f"{hr['wind_10m_p50_kmh']:.0f}/{hr['wind_10m_p90_kmh']:.0f}k"

            h_table.add_row(
                f"{hr['hora_local']:02d}:00",
                h_icon,
                f"{hr['temp_5km_p50_c']:.0f}°",
                r_str,
                vto_str,
                score_widget,
            )
        console.print(h_table)
    else:
        h_table = Table(
            title=f"⏱️  DETALLE HORA A HORA (Score Activo en Horario Solar • {date_title})",
            box=box.SIMPLE_HEAVY,
            header_style="bold cyan",
            padding=(0, 1),
            expand=True,
        )
        h_table.add_column("Hora", style="bold white", width=6, no_wrap=True)
        h_table.add_column("Cielo", justify="center", width=5, no_wrap=True)
        h_table.add_column("Condición", width=19, no_wrap=True)
        h_table.add_column("T°5km", justify="center", width=7, no_wrap=True)
        h_table.add_column("Lluvia (1h)", justify="right", style="cyan", width=12, no_wrap=True)
        h_table.add_column("Viento Med / Ráf", justify="right", width=16, no_wrap=True)
        h_table.add_column("Score Playa", justify="center", width=13, no_wrap=True)

        for hr in hours_to_show:
            hr_tcc = hr.get("tcc_p50_pct")
            hr_lcc = hr.get("lcc_p50_pct")
            hr_ssrd = hr.get("ssrd_1h_p50_wm2")
            h_icon, h_desc, _ = get_weather_condition(
                hr["tp_1h_mean_mm"],
                hr["imerg_1h_p90_mm"],
                hr["pressure_p50_hpa"],
                hr["wind_10m_p90_kmh"],
                hr["hora_local"],
                tcc_pct=hr_tcc,
                lcc_pct=hr_lcc,
                ssrd_wm2=hr_ssrd,
            )
            is_solar_beach = 8 <= hr["hora_local"] <= 18
            if is_solar_beach:
                h_score, _, h_style, _, _ = calculate_caipirinha_index(
                    temp_max=hr["temp_5km_p50_c"],
                    temp_beach_avg=hr["temp_5km_p50_c"],
                    rain_beach=hr["tp_1h_mean_mm"],
                    rain_day=hr["tp_1h_mean_mm"],
                    max_gust=hr["wind_10m_p90_kmh"],
                    mslp_hpa=hr["pressure_p50_hpa"],
                    wind_10m=hr["wind_10m_p50_kmh"],
                    imerg_p90=hr["imerg_1h_p90_mm"],
                    tcc_pct=hr_tcc,
                    lcc_pct=hr_lcc,
                    ssrd_wm2=hr_ssrd,
                )
                score_str = f"🍹 {h_score:.1f} / 10"
                score_widget = Text(score_str, style=h_style)
            else:
                score_widget = Text("🌙 Noche (-)", style="dim")

            r_str = f"{hr['tp_1h_mean_mm']:.1f}/{hr['imerg_1h_p90_mm']:.1f} mm" if (hr["tp_1h_mean_mm"] >= 0.1 or hr["imerg_1h_p90_mm"] >= 0.2) else "     -     "
            vto_str = f"{hr['wind_10m_p50_kmh']:.0f}k (ráf {hr['wind_10m_p90_kmh']:.0f}k)"

            h_table.add_row(
                f"{hr['hora_local']:02d}:00",
                h_icon,
                h_desc,
                f"{hr['temp_5km_p50_c']:.1f}°C",
                r_str,
                vto_str,
                score_widget,
            )
        console.print(h_table)

    # =========================================================================
    # 3. TENDENCIA PRÓXIMOS DÍAS (Compacto)
    # =========================================================================
    daily_stats = []
    for d in all_dates:
        sub_d = df.filter(pl.col("fecha_local") == d)
        b_sub = sub_d.filter((pl.col("hora_local") >= 10) & (pl.col("hora_local") <= 17))
        b_solar = sub_d.filter((pl.col("hora_local") >= 10) & (pl.col("hora_local") <= 16))
        d_min = float(sub_d["temp_5km_p50_c"].min())
        d_max = float(sub_d["temp_5km_p50_c"].max())
        d_b_avg = float(b_sub["temp_5km_p50_c"].mean()) if len(b_sub) > 0 else d_max
        d_rain = float(sub_d["tp_1h_mean_mm"].sum())
        d_rain_b = float(b_sub["tp_1h_mean_mm"].sum()) if len(b_sub) > 0 else 0.0
        d_gust10 = float(b_sub["wind_10m_p90_kmh"].max()) if len(b_sub) > 0 else float(sub_d["wind_10m_p90_kmh"].max())
        d_mslp = float(sub_d["pressure_p50_hpa"].min())
        d_vto10 = float(b_sub["wind_10m_p50_kmh"].mean()) if len(b_sub) > 0 else float(sub_d["wind_10m_p50_kmh"].mean())

        d_has_clouds = "tcc_p50_pct" in sub_d.columns
        if d_has_clouds and len(b_solar) > 0:
            d_tcc = float(b_solar["tcc_p50_pct"].mean())
            d_lcc = float(b_solar["lcc_p50_pct"].mean())
            d_ssrd = float(b_solar["ssrd_1h_p50_wm2"].mean())
        elif d_has_clouds:
            d_tcc = float(sub_d["tcc_p50_pct"].mean())
            d_lcc = float(sub_d["lcc_p50_pct"].mean())
            d_ssrd = float(sub_d["ssrd_1h_p50_wm2"].mean())
        else:
            d_tcc, d_lcc, d_ssrd = None, None, None

        sc, tag, st, _, _ = calculate_caipirinha_index(
            temp_max=d_max,
            temp_beach_avg=d_b_avg,
            rain_beach=d_rain_b,
            rain_day=d_rain,
            max_gust=d_gust10,
            mslp_hpa=d_mslp,
            wind_10m=d_vto10,
            tcc_pct=d_tcc,
            lcc_pct=d_lcc,
            ssrd_wm2=d_ssrd,
        )
        m_row = b_sub.row(len(b_sub) // 2, named=True) if len(b_sub) > 0 else sub_d.row(0, named=True)
        m_tcc = m_row.get("tcc_p50_pct")
        m_lcc = m_row.get("lcc_p50_pct")
        m_ssrd = m_row.get("ssrd_1h_p50_wm2")
        icon, desc, _ = get_weather_condition(
            m_row["tp_1h_mean_mm"],
            m_row["imerg_1h_p90_mm"],
            d_mslp,
            d_gust10,
            13,
            tcc_pct=m_tcc,
            lcc_pct=m_lcc,
            ssrd_wm2=m_ssrd,
        )

        dia_label = "Hoy" if d == now_local else f"{SPANISH_WEEKDAYS[d.weekday()]} {d.day:02d}"
        daily_stats.append({
            "date": d,
            "label": dia_label,
            "icon": icon,
            "t_min": d_min,
            "t_max": d_max,
            "rain": d_rain,
            "score": sc,
            "style": st,
        })

    g_min = min(s["t_min"] for s in daily_stats)
    g_max = max(s["t_max"] for s in daily_stats)

    if is_mobile:
        w_table = Table(
            title="📅 TENDENCIA PRÓXIMOS DÍAS",
            box=box.ROUNDED,
            header_style="bold magenta",
            padding=(0, 0),
            expand=True,
        )
        w_table.add_column("Día", width=7, no_wrap=True)
        w_table.add_column("Cielo", justify="center", width=4, no_wrap=True)
        w_table.add_column("T° Mín..Máx", justify="center", width=11, no_wrap=True)
        w_table.add_column("Lluvia", justify="right", style="cyan", width=7, no_wrap=True)
        w_table.add_column("Score Playa", justify="center", width=11, no_wrap=True)

        for ds in daily_stats:
            r_txt = f"{ds['rain']:.1f}mm" if ds['rain'] >= 0.2 else " 0.0mm"
            w_table.add_row(
                ds["label"],
                ds["icon"],
                f"{ds['t_min']:.0f}..{ds['t_max']:.0f}°C",
                r_txt,
                Text(f"{ds['score']:.1f} / 10", style=ds["style"]),
            )
        console.print(w_table)
    else:
        w_table = Table(
            title="📅  TENDENCIA PRÓXIMOS DÍAS (Barras Térmicas & Score)",
            box=box.SIMPLE_HEAVY,
            header_style="bold magenta",
            padding=(0, 1),
            expand=True,
        )
        w_table.add_column("Día", style="bold white", width=8, no_wrap=True)
        w_table.add_column("Cielo", justify="center", width=5, no_wrap=True)
        w_table.add_column("Mín", justify="right", width=5, no_wrap=True)
        w_table.add_column("Rango Térmico", justify="center", width=14, no_wrap=True)
        w_table.add_column("Máx", justify="left", width=5, no_wrap=True)
        w_table.add_column("Lluvia Total", justify="right", style="cyan", width=12, no_wrap=True)
        w_table.add_column("Score Playa", justify="center", width=15, no_wrap=True)

        for ds in daily_stats:
            bar = render_apple_temp_bar(ds["t_min"], ds["t_max"], g_min, g_max, width=12)
            r_txt = f"{ds['rain']:.1f} mm" if ds["rain"] >= 0.2 else "0.0 mm"
            w_table.add_row(
                ds["label"],
                ds["icon"],
                f"{ds['t_min']:.0f}°",
                bar,
                f"{ds['t_max']:.0f}°",
                r_txt,
                Text(f"🍹 {ds['score']:.1f} / 10", style=ds["style"]),
            )
        console.print(w_table)

    # =========================================================================
    # 4. VENTANAS OPERATIVAS & ALERTAS CUANTITATIVAS (DATA PURA)
    # =========================================================================
    # Calcular ventana óptima basada en score >= 7.5 (sol + confort térmico + viento calmo)
    opt_hours = []
    for r in sub_today.iter_rows(named=True):
        if 8 <= r["hora_local"] <= 18:
            sc_h, _, _, _, _ = calculate_caipirinha_index(
                temp_max=r["temp_5km_p50_c"],
                temp_beach_avg=r["temp_5km_p50_c"],
                rain_beach=r["tp_1h_mean_mm"],
                rain_day=r["tp_1h_mean_mm"],
                max_gust=r["wind_10m_p90_kmh"],
                mslp_hpa=r["pressure_p50_hpa"],
                wind_10m=r["wind_10m_p50_kmh"],
                imerg_p90=r["imerg_1h_p90_mm"],
                tcc_pct=r.get("tcc_p50_pct"),
                lcc_pct=r.get("lcc_p50_pct"),
                ssrd_wm2=r.get("ssrd_1h_p50_wm2"),
            )
            if sc_h >= 7.5:
                opt_hours.append(r)
    if opt_hours:
        opt_start = f"{opt_hours[0]['hora_local']:02d}:00"
        opt_end = f"{opt_hours[-1]['hora_local']:02d}:00"
        opt_tmin = min(r["temp_5km_p50_c"] for r in opt_hours)
        opt_tmax = max(r["temp_5km_p50_c"] for r in opt_hours)
        opt_wmax = max(r["wind_10m_p50_kmh"] for r in opt_hours)
        opt_txt = f"{opt_start} a {opt_end} BRT | Lluvia: 0.0mm | Vto: <{opt_wmax:.0f}k | T°: {opt_tmin:.0f}..{opt_tmax:.0f}°C"
    else:
        opt_txt = "Sin ventana seca continua (condición inestable)"

    # Pico de viento
    windy_hours = [r for r in sub_today.iter_rows(named=True) if r["wind_10m_p90_kmh"] >= 25.0 or r["wind_100m_p90_kmh"] >= 35.0]
    if windy_hours:
        w_start = f"{windy_hours[0]['hora_local']:02d}:00"
        w_end = f"{windy_hours[-1]['hora_local']:02d}:00"
        w_max = max(r["wind_10m_p90_kmh"] for r in windy_hours)
        wind_txt = f"{w_start} a {w_end} BRT | Ráfagas máx: {w_max:.0f} km/h (10m)"
    else:
        wind_txt = f"Viento calmo todo el día (ráfagas < 25 km/h)"

    # Lluvia / Chubascos
    rainy_hours = [r for r in sub_today.iter_rows(named=True) if r["imerg_1h_p90_mm"] >= 0.3 or r["tp_1h_mean_mm"] >= 0.1]
    if rainy_hours:
        rn_start = f"{rainy_hours[0]['hora_local']:02d}:00"
        rn_end = f"{rainy_hours[-1]['hora_local']:02d}:00"
        rn_max = max(r["imerg_1h_p90_mm"] for r in rainy_hours)
        rn_sum = sum(r["tp_1h_mean_mm"] for r in rainy_hours)
        rain_txt = f"{rn_start} a {rn_end} BRT | Acumulado: ~{rn_sum:.1f}mm (IMERG P90 máx: {rn_max:.1f} mm/h)"
    else:
        rain_txt = "Cero probabilidad de precipitación en 24h"

    if is_mobile:
        alerts = (
            f"• [bold green]VENTANA PLAYA ÓPTIMA:[/bold green] {opt_txt}\n"
            f"• [bold yellow]PICOS DE VIENTO:[/bold yellow] {wind_txt}\n"
            f"• [bold cyan]RIESGO CHUBASCO:[/bold cyan] {rain_txt}"
        )
        console.print(Panel(alerts, title="🎯 VENTANAS OPERATIVAS", border_style="green", box=box.ROUNDED))
    else:
        alerts = (
            f"• [bold green]VENTANA PLAYA ÓPTIMA:[/bold green] {opt_txt}\n"
            f"• [bold yellow]PICOS DE VIENTO COSTEIRO:[/bold yellow] {wind_txt}\n"
            f"• [bold cyan]VENTANA CHUBASCO CONVECTIVO:[/bold cyan] {rain_txt}"
        )
        console.print(Panel(alerts, title="🎯 VENTANAS OPERATIVAS & ALERTAS CUANTITATIVAS", border_style="green", box=box.ROUNDED))


def main():
    parser = argparse.ArgumentParser(description="Weather App TUI — Telemetría Física Pura & Caipirinha Beach Index")
    parser.add_argument("--mobile", "-m", action="store_true", help="Formato optimizado para celular / terminal móvil (<50 columnas)")
    parser.add_argument("--desktop", action="store_true", help="Formato amplio para escritorio (>=90 columnas)")
    parser.add_argument("--date", "-d", help="Fecha específica a consultar (formato: YYYY-MM-DD)", default=None)

    # Ubicaciones
    parser.add_argument("--rio", action="store_true", help="Pronóstico para Río de Janeiro (Copacabana / Ipanema)")
    parser.add_argument("--copacabana", "--copa", action="store_true", help="Pronóstico para Copacabana / Ipanema")
    parser.add_argument("--ipanema", action="store_true", help="Pronóstico para Copacabana / Ipanema")
    parser.add_argument("--arpoador", action="store_true", help="Pronóstico para Copacabana / Ipanema")
    parser.add_argument("--ilha-grande", dest="ilha_grande", action="store_true", help="Pronóstico para Ilha Grande")
    parser.add_argument("--angra", action="store_true", help="Pronóstico para Angra dos Reis")
    parser.add_argument("--paraty", action="store_true", help="Pronóstico para Paraty")
    parser.add_argument("--buzios", action="store_true", help="Pronóstico para Búzios")
    parser.add_argument("--casa", action="store_true", help="Pronóstico para Ñuñoa (Casa)")
    parser.add_argument("--trabajo", action="store_true", help="Pronóstico para Las Condes (Trabajo)")
    parser.add_argument("--lugar", "-l", help="Alias de ubicación", default=None)

    args = parser.parse_args()

    # Detección de formato: Mobile-First por defecto (<50 columnas) salvo que se pase --desktop
    global console
    if args.desktop:
        is_mobile = False
        target_width = max(96, console.width) if console.width else 96
        console = Console(width=target_width, height=console.size.height or 45)
    else:
        is_mobile = True
        # Ancho fijo optimizado para smartphone (48 columnas)
        target_width = 48
        console = Console(width=target_width, height=console.size.height or 45)

    # Resolver ubicación con auto-enrutamiento inteligente por fecha de itinerario
    explicit_loc = None
    if args.ilha_grande:
        explicit_loc = "ilha_grande"
    elif args.angra:
        explicit_loc = "angra"
    elif args.paraty:
        explicit_loc = "paraty"
    elif args.buzios:
        explicit_loc = "buzios"
    elif args.casa:
        explicit_loc = "casa"
    elif args.trabajo:
        explicit_loc = "trabajo"
    elif args.rio or args.copacabana or args.ipanema or args.arpoador:
        explicit_loc = "rio"
    elif args.lugar:
        explicit_loc = args.lugar

    if explicit_loc:
        loc = explicit_loc
    else:
        # Auto-enrutamiento por fecha del itinerario (horario local Brasil UTC-3)
        if args.date:
            try:
                eval_date = datetime.strptime(args.date, "%Y-%m-%d").date()
            except ValueError:
                eval_date = (datetime.now() + SANTIAGO_TZ_OFFSET).date()
        else:
            eval_date = (datetime.now() + SANTIAGO_TZ_OFFSET).date()

        if date(2026, 11, 3) <= eval_date <= date(2026, 11, 6):
            loc = "ilha_grande"
        elif date(2026, 10, 30) <= eval_date <= date(2026, 11, 2) or date(2026, 11, 7) <= eval_date <= date(2026, 11, 14):
            loc = "rio"
        else:
            loc = "casa"

    display_weather_app(location_key=loc, is_mobile=is_mobile, target_date_str=args.date)


if __name__ == "__main__":
    main()
