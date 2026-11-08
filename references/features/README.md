# Mapa de Funcionalidades (Feature Map) — WeatherNext

Este documento registra los flujos de usuario, componentes y comandos de verificación del proyecto **weathernext** bajo la metodología A-Stack.

---

## 1. Analizador y Visor de Temporales (TUI & Ensamble Spaghetti)
* **Descripción**: Analiza el ensamble de 64 miembros neuronales de Google DeepMind para eventos extremos (sistemas frontales, ríos atmosféricos y bajas segregadas convectivas). Calcula probabilidades de lluvia por ventanas de 6h, percentiles reales de acumulado por trayectoria de miembro (`true_cum_p50`/`p90`), estimación de ráfagas turbulentas (`100m_wind`), monitoreo operacional de Isoterma 0°C y riesgo aluvional, diagnóstico de choque térmico / inestabilidad convectiva, caída de presión barométrica y genera gráficos interactivos HTML (Plotly) y tablas en terminal con Rich.
* **Componentes**: [`storm_tui.py`](../../storm_tui.py), [`storm`](../../storm), [`data/temporal_santiago.html`](../../data/temporal_santiago.html).
* **Comando de Verificación**:
  ```bash
  # Por defecto: Ñuñoa (Casa) - Horizonte 7 días
  ./storm
  # Modo Celular / SSH móvil (diseño adaptado para <60 columnas):
  ./storm --mobile
  # Modo Traslado Diario (Commute) dinámico en celular:
  ./storm -m -c
  # Modo Traslado Diario (Commute) estándar escritorio:
  ./storm --commute
  # Específico Las Condes (Trabajo):
  ./storm --trabajo
  # Específico Santiago Poniente / Centro:
  ./storm --poniente
  # Específico Precordillera / Las Condes Alto / Farellones:
  ./storm --cordillera
  # Específico Santiago Norte (Chicureo / Colina):
  ./storm --norte
  # Modo Clusters Temporales de Miembros (Ensemble Timing):
  ./storm --clusters
  ./storm --clusters --cordillera
  # Con extracción fresca desde BigQuery:
  ./storm --fetch
  # Con ventana personalizada de días u horas:
  ./storm --days 5
  ./storm -m --hours 48
  ```

---

## 2. Extractor Seguro BigQuery -> Data Lake Local Parquet
* **Descripción**: Extrae corridas operacionales de WeatherNext 2.0 (`init_time`) con filtros geoespaciales (`ST_DWITHIN`), desanidado condicional de tensores (`forecast`, `ensemble`), extracción de componentes de viento a 10m y 100m (`100m_u/v_component_of_wind`), guardrails de costo (`dry_run` + `maximum_bytes_billed`) y persistencia en Parquet versionado con Polars.
* **Componentes**: [`query_weathernext.py`](../../query_weathernext.py), [`config.ini`](../../config.ini), [`data/weathernext_latest.parquet`](../../data/weathernext_latest.parquet).
* **Comando de Verificación**:
  ```bash
  uv run python query_weathernext.py
  ```

---

## 3. Medidor de Consumo y Conciliación de Cuota Free Tier
* **Descripción**: Medidor de doble entrada que rastrea en SQLite local (`data/usage.db`) las consultas ejecutadas y concilia automáticamente contra `INFORMATION_SCHEMA.JOBS_BY_PROJECT` en Google Cloud. Protege el límite de 1 TB mensual a costo $0 USD.
* **Componentes**: [`meter`](../../meter), [`meter.py`](../../meter.py), [`usage_tracker.py`](../../usage_tracker.py).
* **Comando de Verificación**:
  ```bash
  ./meter
  ```

---

## 4. Explorador Offline de Snapshots
* **Descripción**: Motor analítico de costo $0 en memoria que inspecciona esquemas, resúmenes estadísticos de variables físicas y dimensiones de los archivos Parquet en disco.
* **Componentes**: [`explore_local.py`](../../explore_local.py).
* **Comando de Verificación**:
  ```bash
  uv run python explore_local.py
  ```

---

## 5. Extractor y Analizador de Alta Resolución WeatherNext 3.0 (5 km / 10 km • Paso 1h • 250 Miembros)
* **Descripción**: Pipeline Zarr v3 directo contra `gs://weathernext3_statistics_spatial` (sin *requester-pays*, costo $0) que sortea el límite de 1.000 suscriptores de BigQuery Analytics Hub usando peticiones HTTP `Range` combinadas con descompresión parcial en streaming vía `libzstd.so.1` (`ZSTD_decompressStream`). Extrae precipitación horaria calibrada por ERA5 (`total_precipitation_1hr`) y satélite (`imerg_tp_1hr`) a `0.1°` (~10 km), viento a 100m/10m, presión MSL y el cabezal topográfico de estaciones `station_head_temperature_2m` a `0.05°` (~5 km).
* **Componentes**: [`query_weathernext3.py`](../../query_weathernext3.py), [`storm_tui.py`](../../storm_tui.py), [`storm`](../../storm).
* **Comandos de Verificación**:
  ```bash
  # Visualizar dashboard WeatherNext 3.0 hora a hora (paso de 1h)
  ./storm --wn3 --hourly
  ./storm --wn3 --hours 48

  # Modo Celular / SSH móvil para WeatherNext 3.0 (<60 columnas en Termux/Prompt/Blink):
  ./storm --wn3 --mobile
  ./storm -3m
  ./storm -3m --hourly
  ./storm -3m --hours 24
  ./storm -3m --cordillera
  ./storm -3m --pie-andino

  # Visualizar Pie Andino (La Reina / Peñalolén), Cajón del Maipo o Cordillera (Farellones 2400m) a paso de 1h
  ./storm --wn3 --pie-andino --hours 36
  ./storm --wn3 --maipo --hours 36
  ./storm --wn3 --cordillera --hours 36

  # Extraer nueva corrida de WeatherNext 3.0 desde GCS Zarr v3 (con barra de progreso en vivo)
  ./storm --wn3 --fetch
  ./storm --fetch-wn3
  ```

---

## 6. Planificador de Vacaciones & Soporte Regional Brasil (Río de Janeiro, Ilha Grande & Costa Verde)
* **Descripción**: Módulo especializado y dinámico para monitorear el viaje del 30-10 al 14-11 a Río de Janeiro e Ilha Grande (3 al 6 de Noviembre). Incorpora un bounding box de alta resolución a 0.1° (~10 km) en GCS Zarr v3 para la costa fluminense (`LAT_RIO: 665..678`, `LON_RIO: 3152..3183`), evaluación del cruce marítimo en la Bahía de Angra dos Reis a Vila do Abraão (viento, oleaje, ráfagas y lluvia en ventana de ferris), itinerario día por día con semáforo de actividades (playa, sol, chubascos tropicales), consejos para la maleta y compatibilidad total con terminales de escritorio y celulares (<50 columnas vía `-3m`).
* **Componentes**: [`query_weathernext3.py`](../../query_weathernext3.py), [`storm_tui.py`](../../storm_tui.py), [`storm`](../../storm), [`data/weathernext3_rio_latest.parquet`](../../data/weathernext3_rio_latest.parquet).
* **Comandos de Verificación**:
  ```bash
  # Itinerario completo de vacaciones en escritorio
  ./storm -3 --vacaciones
  ./storm --vacaciones

  # Itinerario completo optimizado para celular (<50 columnas)
  ./storm -3m --vacaciones
  ./storm -m --vacaciones

  # Consultar zonas específicas de Brasil (desktop y celular)
  ./storm -3 --rio
  ./storm -3m --ilha-grande
  ./storm -3 --angra
  ./storm -3 --paraty
  ./storm -3 --buzios

  # Extraer nueva corrida de WeatherNext 3.0 para la región de Brasil (216 horas / 9 días continuos)
  ./storm --fetch-rio
  ```

---

## 7. Weather App Visual & Caipirinha Beach Index (Telemetría Pura Mobile-First)
* **Descripción**: Interfaz visual ultra-compacta diseñada para celular y terminal móvil (`<50` columnas). Entrega telemetría atmosférica pura (T° nativa a 5km interpolada desde cabezal topográfico `station_head_temperature_2m_p50`, sensación térmica, punto de rocío exacto `dewpoint_2m_p50_c`, humedad relativa calculada por Magnus-Tetens, viento medio y ráfagas P90 superficiales a 10m y 100m, presión barométrica y tendencia $\Delta P / 6\text{h}$, radiación solar directa `ssrd_1h_p50_wm2`, nubosidad total y baja `tcc/lcc_p50_pct`), detalle hora por hora con **Índice de Caipirinha & Playa (1 a 10) calibrado físicamente** (penaliza falta de sol/radiación solar, viento costero en arena a 10m y Wind Chill playero en traje de baño bajo 27°C si concurren viento o nubes; activo estrictamente durante horas de sol 08:00 a 18:00 BRT, marcando `-` de noche), auto-enrutamiento inteligente de ubicación según el itinerario de vacaciones (03 al 06 Nov $\rightarrow$ Ilha Grande), desglose matemático explícito de deducciones, cálculo de ventana óptima diurna según confort real, tendencia de próximos días y alertas cuantitativas.
* **Componentes**: [`weather_app.py`](../../weather_app.py), [`weather`](../../weather), [`query_weathernext3.py`](../../query_weathernext3.py), [`storm_tui.py`](../../storm_tui.py).
* **Comandos de Verificación**:
  ```bash
  # Modo celular por defecto / SSH móvil (<50 columnas) — auto-enruta a Ilha Grande del 3 al 6 Nov:
  ./weather
  ./weather -m
  ./storm --app -m

  # Consultar fecha específica (ej: domingo 01 de noviembre o martes 03)
  ./weather --date 2026-11-03
  ./weather -d 2026-11-03

  # Forzar modo extendido para escritorio (>=90 columnas)
  ./weather --desktop

  # Consultar destinos específicos
  ./weather --ilha-grande
  ./weather --copacabana
  ./weather --buzios
  ./weather --casa
  ```



