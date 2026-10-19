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

  # Visualizar Pie Andino (La Reina / Peñalolén), Cajón del Maipo o Cordillera (Farellones 2400m) a paso de 1h
  ./storm --wn3 --pie-andino --hours 36
  ./storm --wn3 --maipo --hours 36
  ./storm --wn3 --cordillera --hours 36

  # Extraer nueva corrida de WeatherNext 3.0 desde GCS Zarr v3 (con barra de progreso en vivo)
  ./storm --wn3 --fetch
  ./storm --fetch-wn3
  ```

