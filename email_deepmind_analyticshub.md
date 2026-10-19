# Propuesta de Correo a Google DeepMind / Google Cloud WeatherNext

**Destinatario principal:** `weathernext@google.com`  
**Destinatarios secundarios (si aplica):** Soporte de Google Cloud Analytics Hub / Google DeepMind Weather Team  
**Remitente / Proyecto GCP:**  
- **Correo autorizado:** `<TU_CORREO_AUTORIZADO@GMAIL.COM>`  
- **GCP Project ID:** `<TU_GCP_PROJECT_ID>`  
- **GCP Organization / User ID:** Usuario registrado en el programa oficial de acceso a WeatherNext 3.0  

---

## Versión Oficial en Inglés (Ready to Send)

**Subject:** [WeatherNext 3.0] Analytics Hub Linked Dataset Quota (1,000) Reached on `866962084172:WeatherNext_3`

Dear WeatherNext & Google DeepMind Team,

First, congratulations on the launch of **WeatherNext 3.0**! The spatial upgrade (0.05° station head / 0.1° synoptic) and 1-hour temporal resolution represent a massive milestone for operational ensemble forecasting.

I am writing to report a hard infrastructure quota issue affecting user onboarding via **BigQuery Analytics Hub**. My account (`<TU_CORREO_AUTORIZADO@GMAIL.COM>`) was approved for WeatherNext 3 access, but when attempting to subscribe to the official shared listing in BigQuery Analytics Hub, the subscription fails immediately with the following error:

### Error Details
* **Source Dataset:** `866962084172:WeatherNext_3`
* **Target GCP Project:** `<TU_GCP_PROJECT_ID>`
* **Target Dataset Name:** `weathernext_3`
* **BigQuery API Error Response:**
  ```text
  Dataset 866962084172:WeatherNext_3 already has the maximum number (1000) of linked datasets pointing to it.
  ```
* **HTTP / RPC Status:** `400 / 403 RESOURCE_EXHAUSTED` (BigQuery Analytics Hub default limit of 1,000 linked dataset subscriptions per source listing reached).

### Impact
Because `866962084172:WeatherNext_3` reached the global cap of 1,000 subscriber projects, newly approved researchers and developers cannot link the dataset to their BigQuery instances, blocking SQL-based analyses and automated cloud extraction pipelines.

### Proposed Solutions
Could your engineering team please evaluate one of the following remediation steps?
1. **Increase the Linked Dataset Quota:** Request an internal quota elevation in BigQuery Analytics Hub for `866962084172:WeatherNext_3` (e.g., from 1,000 to 5,000+ linked datasets).
2. **Publish Mirror / Regional Listings:** Deploy partitioned mirror listings in Analytics Hub (e.g., `WeatherNext_3_Pool_B` or regional mirrors) to distribute subscriber load.
3. **IAM Direct Read Authorization:** Alternatively, grant `roles/bigquery.dataViewer` directly to the `weathernext-users@` Google Group so authorized projects can query `866962084172.WeatherNext_3` directly without requiring a linked dataset copy.

In the meantime, we are consuming the statistics via Google Cloud Storage Zarr (`gs://weathernext3_statistics_spatial`), but direct BigQuery integration is critical for our production workflows and automated alerts.

Thank you for your time, support, and outstanding research.

Best regards,  
**[Tu Nombre / Organización]**  
Santiago, Chile  
GCP Project: `<TU_GCP_PROJECT_ID>`  
Email: `<TU_CORREO_AUTORIZADO@GMAIL.COM>`

---

## Versión en Español (Para Referencia Interna)

**Asunto:** [WeatherNext 3.0] Cuota límite de 1.000 datasets vinculados alcanzada en Analytics Hub (`866962084172:WeatherNext_3`)

Estimado equipo de WeatherNext y Google DeepMind:

En primer lugar, felicitaciones por el lanzamiento de **WeatherNext 3.0**. La mejora a 0.05° en cabezales de estaciones y resolución de 1 hora es un salto formidable para el pronóstico de ensambles.

Les escribo para reportar un bloqueo de infraestructura en **BigQuery Analytics Hub**. Mi cuenta (`<TU_CORREO_AUTORIZADO@GMAIL.COM>`) fue aprobada para acceder a WeatherNext 3, pero al intentar suscribirme al listado oficial desde la consola de BigQuery, el sistema arroja el siguiente error:

### Detalle del Error
* **Dataset de Origen:** `866962084172:WeatherNext_3`
* **Proyecto GCP de Destino:** `<TU_GCP_PROJECT_ID>`
* **Nombre de Dataset:** `weathernext_3`
* **Mensaje de Error en BigQuery:**
  ```text
  Dataset 866962084172:WeatherNext_3 already has the maximum number (1000) of linked datasets pointing to it.
  ```

### Causa e Impacto
BigQuery Analytics Hub impone por defecto un límite estricto de 1.000 suscripciones simultáneas (*linked datasets*) por dataset compartido. Al haberse copado los 1.000 proyectos a nivel mundial, nuevos usuarios autorizados quedamos impedidos de consultar los datos mediante SQL en BigQuery.

### Soluciones Sugeridas
1. **Aumentar la cuota interna de suscriptores** en BigQuery Analytics Hub para el dataset `866962084172:WeatherNext_3`.
2. **Publicar un listado espejo (*mirror listing*)** en Analytics Hub para repartir los nuevos suscriptores.
3. **Autorizar lectura directa por IAM** al grupo de usuarios autorizados sobre las tablas de `866962084172`.

Quedo atento a sus comentarios. Mientras tanto, nos encontramos consumiendo las estadísticas vía GCS Zarr (`gs://weathernext3_statistics_spatial`), pero el acceso nativo por BigQuery es fundamental para nuestros pipelines analíticos.

Saludos cordiales,  
**[Tu Nombre / Organización]**  
Santiago, Chile  
Proyecto GCP: `<TU_GCP_PROJECT_ID>`
