# Mapa de Funcionalidades (Feature Map) — WeatherNext

Este documento registra los flujos de usuario, componentes y comandos de verificación del proyecto **weathernext** bajo la metodología A-Stack.

---

## 1. Analizador y Visor de Temporales (TUI & Ensamble Spaghetti)
* **Descripción**: Analiza el ensamble de 64 miembros neuronales de Google DeepMind para eventos extremos. Calcula probabilidades de lluvia por ventanas de 6h, percentiles P50/P90, caída de presión barométrica y genera gráficos interactivos HTML (Plotly) y tablas en terminal con Rich.
* **Componentes**: [`storm_tui.py`](../../storm_tui.py), [`storm`](../../storm), [`data/temporal_santiago.html`](../../data/temporal_santiago.html).
* **Comando de Verificación**:
  ```bash
  ./storm
  # Con extracción previa desde BigQuery:
  ./storm --fetch
  # Con ventana extendida de días:
  ./storm --days 7
  ```

---

## 2. Extractor Seguro BigQuery -> Data Lake Local Parquet
* **Descripción**: Extrae corridas operacionales de WeatherNext 2.0 (`init_time`) con filtros geoespaciales (`ST_DWITHIN`), desanidado condicional de tensores (`forecast`, `ensemble`), guardrails de costo (`dry_run` + `maximum_bytes_billed`) y persistencia en Parquet versionado con Polars.
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
