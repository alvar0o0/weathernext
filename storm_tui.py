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

# Presets de ubicaciones mapeados a las celdas de la grilla de WeatherNext (0.25° x 0.25°)
PRESETS = {
    "casa": {
        "alias": "casa",
        "name": "Ñuñoa (Casa)",
        "desc": "Santiago Oriente • Plaza Ñuñoa",
        "target_cell": (-33.50, -70.50),
    },
    "nunoa": {
        "alias": "nunoa",
        "name": "Ñuñoa",
        "desc": "Santiago Oriente • Plaza Ñuñoa",
        "target_cell": (-33.50, -70.50),
    },
    "trabajo": {
        "alias": "trabajo",
        "name": "Las Condes (Trabajo)",
        "desc": "Santiago Oriente • Eje Apoquindo / Manquehue",
        "target_cell": (-33.50, -70.50),
    },
    "las_condes": {
        "alias": "las_condes",
        "name": "Las Condes",
        "desc": "Santiago Oriente • Eje Apoquindo / Manquehue",
        "target_cell": (-33.50, -70.50),
    },
    "las_condes_alto": {
        "alias": "las_condes_alto",
        "name": "Las Condes Alto / Cantagallo",
        "desc": "Precordillera Norte • Cantagallo / San Carlos",
        "target_cell": (-33.25, -70.50),
    },
    "poniente": {
        "alias": "poniente",
        "name": "Santiago Centro / Poniente",
        "desc": "Valle Central • Santiago / Pudahuel / Maipú",
        "target_cell": (-33.50, -70.75),
    },
    "norte": {
        "alias": "norte",
        "name": "Santiago Norte (Chicureo)",
        "desc": "Chicureo • Colina • Lampa",
        "target_cell": (-33.25, -70.75),
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


def analyze_storm(df: pl.DataFrame, max_hours: int = 120) -> tuple[pl.DataFrame, dict]:
    """
    Agrega los 64 miembros del ensamble por paso de 6 horas y calcula estadísticas
    críticas (Media, P10, P50, P90, Máximo, Probabilidad de lluvia y acumulados).
    """
    filtered = df.filter(pl.col("forecast_hours") <= max_hours)

    p_col = pl.col("precip_6h_mm") if "precip_6h_mm" in filtered.columns else (pl.col("precip_6h").clip(lower_bound=0.0) * 1000.0)
    pres_col = pl.col("pressure_hpa") if "pressure_hpa" in filtered.columns else (pl.col("pressure_msl") / 100.0)
    wind_col = pl.col("wind_speed_kmh") if "wind_speed_kmh" in filtered.columns else ((pl.col("wind_u_10m").pow(2) + pl.col("wind_v_10m").pow(2)).sqrt() * 3.6)

    summary = filtered.group_by(["forecast_hours", "forecast_time"]).agg([
        p_col.mean().round(1).alias("precip_mean"),
        p_col.quantile(0.10).round(1).alias("p10"),
        p_col.quantile(0.50).round(1).alias("p50"),
        p_col.quantile(0.90).round(1).alias("p90"),
        p_col.max().round(1).alias("max"),
        (p_col > 0.5).mean().mul(100).round(0).alias("prob_pct"),
        pl.col("temp_2m_celsius").quantile(0.50).round(1).alias("temp_p50"),
        wind_col.quantile(0.90).round(1).alias("wind_p90"),
        pres_col.quantile(0.50).round(1).alias("pres_p50"),
    ]).sort("forecast_hours")

    cum_p50 = summary["p50"].sum()
    cum_p90 = summary["p90"].sum()
    cum_mean = summary["precip_mean"].sum()

    peak_row = summary.sort("p90", descending=True).row(0, named=True)
    min_pressure = summary["pres_p50"].min()
    max_wind = summary["wind_p90"].max()

    metrics = {
        "cum_mean": cum_mean,
        "cum_p50": cum_p50,
        "cum_p90": cum_p90,
        "peak_time": peak_row["forecast_time"],
        "peak_hours": peak_row["forecast_hours"],
        "peak_p50": peak_row["p50"],
        "peak_p90": peak_row["p90"],
        "peak_max": peak_row["max"],
        "min_pressure": min_pressure,
        "max_wind": max_wind,
        "init_time": df["init_time"][0],
        "total_horizons": len(summary),
    }

    return summary, metrics


def determine_alert_level(metrics: dict) -> tuple[str, str, str]:
    """Determina el nivel de alerta sinóptica en base a umbrales de lluvia y viento."""
    p90 = metrics["cum_p90"]
    p50 = metrics["cum_p50"]
    wind = metrics["max_wind"]

    if p90 >= 90 or p50 >= 60 or wind >= 60:
        return "ROJA", "ALERTA ROJA (Temporal Severo / Río Atmosférico Intenso)", "bold white on red"
    elif p90 >= 45 or p50 >= 25 or wind >= 45:
        return "NARANJA", "ALERTA NARANJA (Temporal Moderado a Significativo)", "bold black on yellow"
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


def display_commute_view(df: pl.DataFrame, is_mobile: bool = False):
    """
    Vista especializada de Traslado Diario (Commute):
    Analiza ventanas de viaje entre Ñuñoa (Casa) y Las Condes (Trabajo)
    para lunes, martes y miércoles.
    """
    df_oriente, _ = filter_by_location(df, "casa")
    summary, _ = analyze_storm(df_oriente, max_hours=72)

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
        for row in summary.iter_rows(named=True):
            ft_local = row["forecast_time"] + SANTIAGO_TZ_OFFSET
            hour = ft_local.hour
            day_str = f"{SPANISH_WEEKDAYS[ft_local.weekday()]} {ft_local.day:02d}/{ft_local.month:02d}"
            p50 = row["p50"]
            p90 = row["p90"]

            if hour == 9:
                if not first:
                    blocks_text.append("─" * div_len + "\n", style="dim")
                first = False
                blocks_text.append(f"📅 {day_str} • Mañana (08-10h CLT)\n", style="bold white")
                blocks_text.append("➔ Sentido: Ñuñoa ➔ Las Condes\n", style="cyan")
                blocks_text.append(f"💧 Lluvia: {p50:4.1f} mm (P90: {p90:4.1f} mm)\n", style="bold")
                if p50 >= 8.0:
                    blocks_text.append("🔴 Impacto: CRÍTICO (Anegamientos)\n", style="bold red")
                elif p50 >= 3.0:
                    blocks_text.append("🟠 Impacto: ALTO (Congestión Severa)\n", style="bold yellow")
                elif p50 >= 0.5:
                    blocks_text.append("🟡 Impacto: MODERADO (Resbaladizo)\n", style="cyan")
                else:
                    blocks_text.append("🟢 Impacto: Despejado / Sin riesgo\n", style="green")

            elif hour == 21:
                if not first:
                    blocks_text.append("─" * div_len + "\n", style="dim")
                first = False
                blocks_text.append(f"📅 {day_str} • Tarde (18-20h CLT)\n", style="bold white")
                blocks_text.append("➔ Sentido: Las Condes ➔ Ñuñoa\n", style="cyan")
                blocks_text.append(f"💧 Lluvia: {p50:4.1f} mm (P90: {p90:4.1f} mm)\n", style="bold")
                if p50 >= 8.0:
                    blocks_text.append("🔴 Impacto: CRÍTICO (Tráfico Lento)\n", style="bold red")
                elif p50 >= 3.0:
                    blocks_text.append("🟠 Impacto: ALTO (Retención / Visib.)\n", style="bold yellow")
                elif p50 >= 0.5:
                    blocks_text.append("🟡 Impacto: MODERADO (Pesado)\n", style="cyan")
                else:
                    blocks_text.append("🟢 Impacto: Normal / Sin alerta\n", style="green")

        console.print(Panel(blocks_text, title="🚦 Ventanas de Viaje", border_style="cyan", box=box.ROUNDED))

        rec_text = (
            "• [bold red]Martes 06 (Mañana):[/bold red] Pico máximo (~13-18 mm).\n"
            "  Anegamientos en Manquehue y Tobalaba.\n"
            "  [italic]👉 Teletrabajo altamente recomendado.[/italic]\n"
            "• [bold yellow]Miércoles 07:[/bold yellow] Lluvia continua (~4-6 mm).\n"
            "  Traslado posible con precaución."
        )
        console.print(Panel(rec_text, title="💡 Recomendación", border_style="yellow", box=box.ROUNDED))
        return

    # Vista escritorio estándar (ancho)
    console.print(Panel(
        "[bold cyan]🚗 ANÁLISIS DE TRASLADO DIARIO: ÑUÑOA ⇄ LAS CONDES[/bold cyan]\n"
        "[dim]Evaluación de ventanas de hora punta (Mañana 06:00-12:00 CLT / Tarde 18:00-24:00 CLT)[/dim]",
        border_style="magenta",
        box=box.ROUNDED
    ))

    commute_table = Table(box=box.ROUNDED, header_style="bold cyan", padding=(0, 1))
    commute_table.add_column("Ventana de Traslado", style="bold white", width=22)
    commute_table.add_column("Sentido", justify="center", width=18)
    commute_table.add_column("Lluvia P50", justify="right", style="bold cyan", width=12)
    commute_table.add_column("Riesgo P90", justify="right", style="bold orange3", width=12)
    commute_table.add_column("Impacto en Tráfico", justify="left", width=34)

    for row in summary.iter_rows(named=True):
        ft_local = row["forecast_time"] + SANTIAGO_TZ_OFFSET
        hour = ft_local.hour
        day_str = f"{SPANISH_WEEKDAYS[ft_local.weekday()]} {ft_local.day:02d}/{ft_local.month:02d}"

        p50 = row["p50"]
        p90 = row["p90"]

        if hour == 9:
            ventana = f"{day_str} Mañana (08-10h)"
            sentido = "Ñuñoa ➔ Las Condes"
            if p50 >= 8.0:
                impacto = Text("🔴 CRÍTICO: Inundaciones / Colapso", style="bold red")
            elif p50 >= 3.0:
                impacto = Text("🟠 ALTO: Congestión / Anegamientos", style="bold yellow")
            elif p50 >= 0.5:
                impacto = Text("🟡 MODERADO: Pavimento Resbaladizo", style="cyan")
            else:
                impacto = Text("🟢 Despejado / Sin complicaciones", style="green")

            commute_table.add_row(ventana, sentido, f"{p50:4.1f} mm", f"{p90:4.1f} mm", impacto)

        elif hour == 15 or hour == 21:
            if hour == 21:
                ventana = f"{day_str} Tarde (18-20h)"
                sentido = "Las Condes ➔ Ñuñoa"
                if p50 >= 8.0:
                    impacto = Text("🔴 CRÍTICO: Tráfico Lento Severo", style="bold red")
                elif p50 >= 3.0:
                    impacto = Text("🟠 ALTO: Retención / Visibilidad Baja", style="bold yellow")
                elif p50 >= 0.5:
                    impacto = Text("🟡 MODERADO: Tránsito Pesado", style="cyan")
                else:
                    impacto = Text("🟢 Normal / Sin alerta", style="green")

                commute_table.add_row(ventana, sentido, f"{p50:4.1f} mm", f"{p90:4.1f} mm", impacto)

    console.print(commute_table)

    console.print(Panel(
        "💡 [bold yellow]Recomendación Estratégica para tu Semana:[/bold yellow]\n"
        "• [bold red]Martes 06 (Mañana):[/bold red] El temporal alcanza su pico máximo (~13.4 a 17.8 mm en 6h) justo entre 09:00 y 14:00 CLT.\n"
        "  [italic]Ejes Manquehue, Tobalaba y Américo Vespucio con alto riesgo de anegamiento. Se recomienda encarecidamente teletrabajo.[/italic]\n"
        "• [bold yellow]Miércoles 07:[/bold yellow] La lluvia se mantendrá constante pero con menor violencia (~4 a 6 mm/6h). Traslado posible con precaución.",
        border_style="yellow",
        box=box.ROUNDED
    ))


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
        "NARANJA": "ALERTA NARANJA • Temporal Moderado",
        "AMARILLA": "ALERTA AMARILLA • Chubascos Activos",
        "VERDE": "ALERTA VERDE • Sin Riesgo Mayor",
    }
    alert_tag = short_labels.get(level_code, level_label)
    summary_body.append(f"⚠️  {alert_tag}\n\n", style=level_style)

    summary_body.append(f"🌧️ Acumulado ({days} Días):\n", style="bold cyan")
    summary_body.append(f"  • P50 (Mediana):  {metrics['cum_p50']:>6.1f} mm\n", style="bold white")
    summary_body.append(f"  • P90 (Crítico):  {metrics['cum_p90']:>6.1f} mm\n", style="bold orange3")
    summary_body.append(f"  • Media Ensamble: {metrics['cum_mean']:>6.1f} mm\n\n", style="dim")

    summary_body.append("⚡ Pico del Temporal:\n", style="bold magenta")
    summary_body.append(f"  • {peak_day_name} {peak_dt_local.strftime('%d/%m %H:%M CLT')} (+{metrics['peak_hours']}h)\n", style="bold white")
    summary_body.append(f"  • P50: {metrics['peak_p50']:.1f} mm (P90: {metrics['peak_p90']:.1f} mm/6h)\n\n", style="bold white")

    summary_body.append("🌪️ Dinámica Atmosférica:\n", style="bold bright_blue")
    summary_body.append(f"  • Viento P90:     {metrics['max_wind']:.1f} km/h\n", style="bold white")
    summary_body.append(f"  • Presión Mínima: {metrics['min_pressure']:.1f} hPa", style="bold white")

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
        "[bold white]TOTAL[/bold white]",
        f"[bold cyan]{metrics['cum_p50']:4.1f}[/bold cyan]/[bold orange3]{metrics['cum_p90']:4.1f}mm[/bold orange3]",
        ""
    )
    console.print(daily_table)

    # 4. Cronograma Horario Compacto (4 columnas, encaja en <45 caracteres)
    if hours is not None:
        horizon_limit = max(1, hours // 6)
    else:
        horizon_limit = 6  # 36 horas por defecto en celular

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

    # 1. ENCABEZADO
    header_text = Text()
    header_text.append("🌦️  WEATHERNEXT 2.0 ", style="bold cyan")
    header_text.append("• GOOGLE DEEPMIND NEURAL ENSEMBLE\n", style="bold white")
    header_text.append(f"Ubicación: {preset['name']} ({preset['desc']})\n", style="bold yellow")
    header_text.append(f"Celda Modelo: ({target_coord[0]:.2f}°, {target_coord[1]:.2f}°) | Corrida: {init_str} | Snapshot: {filepath.name}", style="dim")

    console.print(Panel(header_text, border_style="cyan", box=box.ROUNDED))

    # 2. TARJETAS DE ALERTA Y MÉTRICAS CLAVE
    p50_val = f"{metrics['cum_p50']:.1f} mm"
    p90_val = f"{metrics['cum_p90']:.1f} mm"
    peak_dt = metrics['peak_time']
    peak_dt_local = peak_dt + SANTIAGO_TZ_OFFSET
    peak_day_name = SPANISH_WEEKDAYS[peak_dt_local.weekday()]
    peak_local = f"{peak_day_name} {peak_dt_local.strftime('%d/%m %H:%M CLT')}"
    peak_info = f"{peak_local} (+{metrics['peak_hours']}h)\n[bold]{metrics['peak_p50']:.1f} mm[/bold] (P90: {metrics['peak_p90']:.1f} mm/6h)"
    synoptic_info = f"Presión Mín: [bold]{metrics['min_pressure']:.1f} hPa[/bold]\nViento P90:  [bold]{metrics['max_wind']:.1f} km/h[/bold]"

    card_alert = Panel(
        Text(f"\n{level_code}: {level_label}\n", justify="center", style=level_style),
        title="⚠️ Nivel de Alerta",
        border_style="yellow" if "NARANJA" in level_code or "AMARILLA" in level_code else ("red" if "ROJA" in level_code else "green"),
        box=box.ROUNDED
    )

    card_rain = Panel(
        f"[bold cyan]P50 (Mediana):[/bold cyan]  [bold white]{p50_val}[/bold white]\n"
        f"[bold orange3]P90 (Crítico):[/bold orange3] [bold white]{p90_val}[/bold white]\n"
        f"[dim]Media:          {metrics['cum_mean']:.1f} mm[/dim]",
        title=f"🌧️ Acumulado ({days} Días)",
        border_style="blue",
        box=box.ROUNDED
    )

    card_peak = Panel(
        f"{peak_info}\n[dim]Máx extremo: {metrics['peak_max']:.1f} mm[/dim]",
        title="⚡ Pico del Temporal",
        border_style="magenta",
        box=box.ROUNDED
    )

    card_syn = Panel(
        f"{synoptic_info}\n[dim]64 miembros activos[/dim]",
        title="🌪️ Dinámica Atmosférica",
        border_style="bright_blue",
        box=box.ROUNDED
    )

    cards_grid = Table.grid(expand=True, padding=(0, 1))
    cards_grid.add_column(ratio=1)
    cards_grid.add_column(ratio=1)
    cards_grid.add_row(card_alert, card_rain)
    cards_grid.add_row(card_peak, card_syn)
    console.print(cards_grid)

    # 3. CRONOGRAMA DETALLADO (TABLA CON HIDROGRAMA DE BARRAS)
    table = Table(
        title=f"📊 Cronograma por Ventanas de 6 Horas en {preset['name']} (Próximos {days} Días)",
        box=box.SIMPLE_HEAVY,
        header_style="bold cyan",
        show_lines=False,
        padding=(0, 1),
        pad_edge=False,
    )

    table.add_column("Hora (CLT)", style="bold white", width=15, no_wrap=True)
    table.add_column("H+", justify="right", style="cyan", width=5, no_wrap=True)
    table.add_column("Prob", justify="right", width=5, no_wrap=True)
    table.add_column("Bar", width=8, no_wrap=True)
    table.add_column("P50 / P90", justify="right", style="bold cyan", width=12, no_wrap=True)
    table.add_column("Vto", justify="right", width=7, no_wrap=True)
    table.add_column("Pres", justify="right", width=9, no_wrap=True)

    prev_pres = None
    for row in summary.iter_rows(named=True):
        ft_utc = row["forecast_time"]
        ft_local = ft_utc + SANTIAGO_TZ_OFFSET
        day_name = SPANISH_WEEKDAYS[ft_local.weekday()]
        local_str = f"{day_name} {ft_local.strftime('%d/%m %H:%M')}"
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

        bar = render_rain_bar(row["p50"], max_scale=15.0, width=8)

        p50 = row['p50']
        p90 = row['p90']

        wind_s = f"{row['wind_p90']:4.1f}k"

        pres = row["pres_p50"]
        if prev_pres is not None:
            diff = pres - prev_pres
            if diff < -1.5:
                pres_s = Text(f"{pres:5.1f}↓↓", style="bold red")
            elif diff < -0.3:
                pres_s = Text(f"{pres:5.1f}↓", style="yellow")
            elif diff > 0.5:
                pres_s = Text(f"{pres:5.1f}↑", style="green")
            else:
                pres_s = Text(f"{pres:5.1f}=", style="dim")
        else:
            pres_s = Text(f"{pres:5.1f}", style="dim")
        prev_pres = pres

        rain_txt = f"{p50:4.1f}/{p90:4.1f}mm" if (p50 > 0 or p90 > 0) else "   -   "
        table.add_row(
            local_str,
            h,
            prob_txt,
            bar,
            rain_txt,
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
        pl.col("wind_p90").max().round(1).alias("viento_max"),
        pl.col("pres_p50").min().round(1).alias("pres_min"),
    ]).sort("fecha_local")

    daily_table = Table(box=box.ROUNDED, header_style="bold magenta")
    daily_table.add_column("Día", style="bold white", width=25)
    daily_table.add_column("Lluvia P50 (Esperada)", justify="right", style="bold cyan", width=24)
    daily_table.add_column("Lluvia P90 (Peor Caso)", justify="right", style="bold orange3", width=22)
    daily_table.add_column("Ráfagas Máx", justify="right", width=14)
    daily_table.add_column("Presión Mín", justify="right", width=14)

    for d in daily.iter_rows(named=True):
        f = d["fecha_local"]
        dia_str = f"{SPANISH_DAYS_FULL[f.weekday()]} {f.day} de {SPANISH_MONTHS_FULL[f.month]}"
        p50_val = d['lluvia_p50']
        p90_str = f"{d['lluvia_p90']:5.1f} mm"
        
        bar_len = int(min(14, p50_val / 2.5))
        bar_daily = " " + ("█" * bar_len) if bar_len > 0 else ""
        p50_str = f"{p50_val:5.1f} mm{bar_daily}"

        daily_table.add_row(
            dia_str,
            p50_str,
            p90_str,
            f"{d['viento_max']:.1f} km/h",
            f"{d['pres_min']:.1f} hPa",
        )

    daily_table.add_section()
    daily_table.add_row(
        "[bold white]TOTAL EVENTO[/bold white]",
        f"[bold cyan]{metrics['cum_p50']:.1f} mm[/bold cyan]",
        f"[bold orange3]{metrics['cum_p90']:.1f} mm[/bold orange3]",
        f"[bold]{metrics['max_wind']:.1f} km/h[/bold]",
        f"[bold]{metrics['min_pressure']:.1f} hPa[/bold]",
    )
    console.print(daily_table)


def display_tui(
    parquet_path: str | None = None,
    location_key: str = "casa",
    days: int = 5,
    make_html: bool = True,
    is_mobile: bool = False,
    hours: int | None = None,
):
    """Renderiza el dashboard interactivo en la terminal con Rich."""
    raw_df, filepath = load_latest_snapshot(parquet_path)
    df, preset = filter_by_location(raw_df, preset_key=location_key)

    max_hours = days * 24
    summary, metrics = analyze_storm(df, max_hours=max_hours)
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
    parser.add_argument("--days", "-d", type=int, default=5, help="Días de pronóstico a visualizar (defecto: 5)")
    parser.add_argument("--hours", type=int, default=None, help="Número de horas a mostrar en el cronograma (ej: 36, 48)")
    parser.add_argument("--mobile", "-m", action="store_true", help="Formato optimizado para celular / pantallas estrechas (<65 columnas)")
    parser.add_argument("--no-html", action="store_true", help="No generar el archivo HTML")
    parser.add_argument("--fetch", action="store_true", help="Extraer primero el snapshot más reciente desde BigQuery")

    # Modos de ubicación
    parser.add_argument("--casa", action="store_true", help="Pronóstico para Ñuñoa (Casa)")
    parser.add_argument("--trabajo", action="store_true", help="Pronóstico para Las Condes (Trabajo)")
    parser.add_argument("--poniente", action="store_true", help="Pronóstico para Santiago Centro / Poniente")
    parser.add_argument("--lugar", "-l", help="Alias de ubicación (casa, trabajo, poniente, norte, cordillera)", default=None)
    parser.add_argument("--commute", "-c", action="store_true", help="Modo especial de Traslado Diario (Ñuñoa ⇄ Las Condes)")

    args = parser.parse_args()

    # Detección automática o explícita de terminal móvil
    global console
    is_mobile = args.mobile or console.width < 68
    if is_mobile:
        target_width = 50 if (args.mobile and console.width >= 68) else console.width
        console = Console(width=target_width, height=console.size.height or 40)

    if args.fetch:
        console.print("[bold yellow]Extrayendo última corrida desde BigQuery...[/bold yellow]")
        from query_weathernext import fetch_weather_data
        fetch_weather_data()

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
    elif args.lugar:
        loc = args.lugar
    else:
        loc = "casa" # Ñuñoa por defecto

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
