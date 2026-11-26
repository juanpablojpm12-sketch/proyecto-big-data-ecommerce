# Guía paso a paso para ejecutar el proyecto

No necesitas escribir código ni usar comandos de Git. Todo se hace con clics en el navegador.
Tiempo estimado: **3 a 4 horas**, contando la espera mientras suben los archivos y corre el pipeline.

> Las pantallas de Databricks cambian con frecuencia. Si un botón tiene otro nombre, busca uno parecido.

---

## Paso 1 · Descargar los datos de Kaggle (15 min)

1. Entra a <https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop>
   (crea una cuenta gratuita si no tienes).
2. Clic en **Download** y descarga el `.zip`.
3. Descomprímelo. Deben quedar 5 archivos: `2019-Oct.csv`, `2019-Nov.csv`, `2019-Dec.csv`, `2020-Jan.csv` y
   `2020-Feb.csv`.

⚠️ **No subas estos CSV a GitHub**, porque pesan demasiado. Solo van a Databricks.

---

## Paso 2 · Subir el código a GitHub (15 min)

1. En GitHub: **New repository** → nombre `proyecto-big-data-ecommerce` → **Public** → **Create repository**.
2. En el repositorio vacío: clic en **uploading an existing file**.
3. Descomprime el `.zip` del proyecto y **arrastra todo el contenido** de la carpeta
   `proyecto-big-data-ecommerce` (no la carpeta en sí, sino lo que tiene adentro).
4. Escribe un mensaje como `Versión inicial del proyecto` y clic en **Commit changes**.
5. Revisa que exista la carpeta `.github/workflows`. Si no aparece (pasa en Mac, porque oculta las carpetas que
   empiezan con punto), haz esto:
   **Add file → Create new file**. En el nombre escribe `.github/workflows/ci.yml`, pega el contenido de ese
   archivo y haz **Commit**.
6. Ve a la pestaña **Actions**. En unos 3 minutos la prueba debe quedar en verde ✅. Toma una captura para la
   sección de CI/CD.

---

## Paso 3 · Crear la cuenta de Databricks Free Edition (10 min)

1. Busca "Databricks Free Edition" y regístrate con tu correo o tu cuenta de Google o Microsoft.
2. Databricks crea un *workspace* gratuito con el catálogo `workspace`.

---

## Paso 4 · Traer el código de GitHub a Databricks (5 min)

1. En el menú izquierdo: **Workspace** → entra a tu carpeta de usuario → **Create → Git folder**.
2. Pega la URL de tu repositorio (`https://github.com/TU_USUARIO/proyecto-big-data-ecommerce`) y clic en **Create**.
3. Aparece la carpeta con los notebooks. Si te pide conectar GitHub, acepta la conexión con tu cuenta.

---

## Paso 5 · Crear el esquema y subir los datos (30-60 min, la mayor parte es espera)

1. Abre `notebooks/01_setup`. Arriba a la derecha, en el selector de cómputo, elige **Serverless**.
2. Ejecuta **solo las dos primeras celdas de código** (`Run cell`). Esto crea el esquema `ecommerce` y los
   volúmenes `raw` y `checkpoints`.
3. Menú izquierdo **Catalog** → `workspace` → `ecommerce` → **Volumes** → `raw` →
   **Upload to this volume**.
   - 💡 **Para la presentación:** sube primero **solo** `2019-Oct.csv`, `2019-Nov.csv` y `2019-Dec.csv`. En el
     paso 7 subirás los otros dos para demostrar la ingesta incremental.
4. Vuelve a `01_setup` y ejecuta la última celda para confirmar que se ven los archivos.

---

## Paso 6 · Crear el Job del pipeline (15 min)

1. Menú izquierdo **Jobs & Pipelines → Create → Job**. Nómbralo `pipeline_ecommerce_medallion`.
2. Primera tarea:
   - *Task name*: `bronze_ingesta`
   - *Type*: **Notebook**
   - *Path*: el notebook `02_bronze_ingesta` dentro de tu Git folder
   - *Compute*: **Serverless**
3. Clic en **+ Add task → Notebook** y crea las demás. En **Depends on**, pon la tarea que va antes:

| Task name | Notebook | Depends on |
|---|---|---|
| `bronze_ingesta` | `02_bronze_ingesta` | — |
| `silver_limpieza` | `03_silver_limpieza` | `bronze_ingesta` |
| `gold_agregados` | `04_gold_agregados` | `silver_limpieza` |
| `modelo_ciencia_datos` | `05_modelo_ciencia_datos` | `gold_agregados` |
| `beneficio_costo` | `06_beneficio_costo` | `gold_agregados` |

4. **Run now**. Ve a la pestaña **Runs** y espera a que todas las tareas queden en verde.
5. 📸 Toma una captura del gráfico de tareas (DAG) en verde. Es la mejor evidencia del pipeline, que vale el 50 %.

**¿No alcanzas a crear el Job?** Abre `notebooks/99_ejecutar_todo` y dale **Run all**. Ejecuta los mismos pasos
en orden.

---

## Paso 7 · Demostrar la ingesta incremental (10 min)

1. Sube `2020-Jan.csv` y `2020-Feb.csv` al volume `raw`.
2. Vuelve a ejecutar el Job (**Run now**).
3. Abre la ejecución de `bronze_ingesta`. En la tabla de **auditoría** verás que solo se agregaron los 2
   archivos nuevos y que los anteriores no se volvieron a cargar. 📸 Toma una captura: esto demuestra la
   *Velocidad* y la automatización.
4. Abre la ejecución de `silver_limpieza`. 📸 Toma una captura de `log_calidad_datos` y de los motivos de rechazo.

---

## Paso 8 · Revisar MLflow (5 min)

1. Abre la ejecución de `modelo_ciencia_datos` y mira los gráficos y las métricas.
2. Menú izquierdo **Experiments**. Abre el experimento del notebook y luego la corrida
   `regresion_logistica_conversion`.
3. 📸 Toma capturas de las **métricas** (AUC, exactitud, etc.) y de la pestaña **Artifacts** (curva ROC).
   La rúbrica pide "MLflow logs como anexo".

---

## Paso 9 · Crear el dashboard (45 min)

1. Menú izquierdo **Dashboards → Create dashboard**.
2. Pestaña **Data → Create from SQL**. Pega cada consulta de `dashboard/consultas_dashboard.sql`
   (una por dataset) y ponle el nombre indicado en el comentario.
3. Pestaña **Canvas → Add a visualization**. Elige el dataset y el tipo de gráfico que indica el comentario
   de cada consulta. El asistente de IA del dashboard también puede crearlos si se lo pides en español, por
   ejemplo: *"gráfico de líneas de ingresos_usd por fecha"*.
4. Orden sugerido:
   - Fila 1: 4 tarjetas KPI.
   - Fila 2: ingresos diarios y embudo.
   - Fila 3: carritos abandonados y top marcas.
   - Fila 4: mapa de calor, segmentos RFM y grupos de riesgo.
5. **Publish**. 📸 Toma una captura completa y guárdala como `dashboard.png`.

---

## Paso 10 · Completar el README y entregar (30 min)

1. En GitHub, entra a la carpeta `docs` → **Add file → Upload files** y sube tus capturas (`dashboard.png`,
   `job_dag.png`, `mlflow.png`, `auditoria_bronze.png`, `calidad_silver.png`).
2. Abre `README.md` → ícono del lápiz ✏️ y completa:
   - la tabla de **resultados por escenario** (sección 2) con lo que mostró el notebook 06;
   - los **resultados del modelo** (sección 5): AUC, exactitud y p-valor del Chi-cuadrado;
   - la imagen del dashboard en la sección 6: `![Dashboard](docs/dashboard.png)`;
   - si quieres, el número exacto de filas de Bronze en la tabla de las 5 V.
3. **Commit changes** y comparte el enlace del repositorio con el docente.

---

## Plan sugerido hasta el viernes 27

| Día | Tareas |
|---|---|
| **Miércoles** | Pasos 1 a 6: datos, GitHub, Databricks y primera ejecución del Job |
| **Jueves** | Pasos 7 a 9: incremental, MLflow y dashboard |
| **Viernes** | Paso 10: completar el README y repasar la explicación de cada sección |

## Problemas comunes

| Problema | Solución |
|---|---|
| "Table or view not found" | Ejecuta antes `01_setup` y verifica que los CSV estén en el volume `raw`. |
| El Job dice que se superó la cuota | Free Edition tiene una cuota diaria. Espera al día siguiente; los datos no se borran. |
| Quieres volver a cargar todo desde cero | En SQL ejecuta `DROP SCHEMA workspace.ecommerce CASCADE` y repite desde el paso 5. Esto también borra los CSV subidos, así que tendrás que subirlos otra vez. |
| `%run ./00_config` falla | Los notebooks deben estar todos en la misma carpeta `notebooks`. |
