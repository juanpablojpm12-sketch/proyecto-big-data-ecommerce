# Proyecto Final · Big Data — Analítica del embudo de compra de un e-commerce de cosméticos

| | |
|---|---|
| **Módulo** | Big Data |
| **Programa** | Especialización en Ciencia de Datos — UNAULA |
| **Docente** | Yeis Livis Taborda Henao |
| **Estudiante** | Juan Pablo Martinez Benjumea |
| **Plataforma** | Databricks Free Edition (Lakehouse, Delta Lake, Unity Catalog, Lakeflow Jobs, MLflow, AI/BI) |
| **Datos** | Kaggle · [eCommerce Events History in Cosmetics Shop](https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop) |

> **Para ejecutar el proyecto paso a paso, ver [`docs/GUIA_PASO_A_PASO.md`](docs/GUIA_PASO_A_PASO.md).**

---

## Contenido

1. [Caso de negocio](#1-caso-de-negocio)
2. [Relación beneficio / costo](#2-relación-beneficio--costo)
3. [Arquitectura propuesta](#3-arquitectura-propuesta)
4. [Pipeline de ingesta y estrategia Medallion](#4-pipeline-de-ingesta-y-estrategia-medallion)
5. [Modelos de ciencia de datos](#5-modelos-de-ciencia-de-datos)
6. [Visualización](#6-visualización)
7. [Integración continua (CI/CD)](#7-integración-continua-cicd)
8. [Estructura del repositorio](#8-estructura-del-repositorio)

---

## 1. Caso de negocio

### Descripción del problema
Una tienda online de cosméticos de tamaño mediano registra **cada clic** de sus clientes: cuándo ven un producto,
cuándo lo agregan al carrito, cuándo lo retiran y cuándo compran. Son millones de eventos al mes, pero la empresa
solo mira el total de ventas. No sabe:

- en qué paso del proceso de compra (ver → carrito → comprar) pierde a los clientes;
- **cuánto dinero queda abandonado en carritos** que nunca se pagan;
- qué marcas y categorías convierten mejor, ni a qué horas compran sus clientes;
- qué clientes son valiosos y cuáles están dejando de comprar.

Con hojas de cálculo no es posible: un solo mes supera el límite de filas de Excel, y los datos llegan
sucios (marcas vacías, precios negativos, eventos duplicados).

### Objetivos del proyecto
**General:** construir una plataforma de datos en Databricks que transforme los eventos crudos de navegación en
información confiable para aumentar las ventas recuperando carritos abandonados.

**Específicos:**
1. Automatizar la ingesta incremental de los archivos de eventos (arquitectura Medallion: Bronze, Silver, Gold).
2. Garantizar la calidad de los datos con reglas de validación, cuarentena de registros y log de calidad.
3. Medir el embudo de conversión, el valor de los carritos abandonados, marcas, categorías y horarios.
4. Construir un modelo que estime la **probabilidad de compra** de cada sesión con carrito.
5. Presentar los resultados en un dashboard para Gerencia y Marketing.

### Por qué es un problema de Big Data — las 5 V

| V | Cómo se cumple en este proyecto |
|---|---|
| **Volumen** | Cerca de 20 millones de eventos en 5 archivos mensuales (≈ 2 GB en CSV). Supera el límite de Excel (1.048.576 filas) en cada mes. |
| **Velocidad** | Los eventos se generan en tiempo real con cada clic. Llegan archivos nuevos cada mes; Auto Loader carga **solo lo nuevo** de forma incremental y el Job puede programarse diariamente. |
| **Variedad** | Fechas con zona horaria en texto, IDs numéricos muy largos, precios decimales, categorías jerárquicas (`appliances.environment.vacuum`), marcas en texto libre y 4 tipos de evento. |
| **Veracidad** | Marcas y categorías vacías, precios negativos, eventos duplicados exactos. Se tratan en la capa Silver con reglas de calidad, tabla de rechazados y log. |
| **Valor** | Identificar el dinero perdido en carritos abandonados y priorizar acciones de recuperación (ver sección 2). |

---

## 2. Relación beneficio / costo

### Lógica económica
Cada mes, miles de sesiones agregan productos al carrito y no compran. Ese **valor abandonado** se calcula con
datos reales en la tabla `gold_carritos_abandonados_mensual`. Si con la información de la plataforma (recordatorios
a sesiones de alto riesgo, cupones, campañas en horas pico, foco en marcas de alta conversión) se recupera solo
una fracción de ese valor, el ingreso adicional paga la inversión.

```
Ventas recuperadas / mes = Valor abandonado promedio mensual × % de recuperación
Utilidad extra / mes     = Ventas recuperadas × Margen bruto
Relación B/C (12 meses)  = Utilidad extra 12 meses ÷ Costo total 12 meses
ROI                      = (Beneficio − Costo) ÷ Costo
```

### Supuestos de costo (editables en `notebooks/06_beneficio_costo.py`)

| Concepto | Valor | Tipo |
|---|---|---|
| Implementación del pipeline (≈ 2 meses de un ingeniero de datos) | USD 8.000 | Único |
| Plataforma Databricks de pago (cómputo + almacenamiento) | USD 600 / mes | Recurrente |
| Operación y análisis (tiempo parcial) | USD 900 / mes | Recurrente |
| **Costo total a 12 meses** | **USD 26.000** | |
| Margen bruto supuesto en cosméticos | 40 % | |

### Resultados por escenario
El notebook `06_beneficio_costo` calcula la tabla con los datos reales y la guarda en `gold_beneficio_costo`:

| Escenario | % recuperación | Ventas recuperadas/mes | Utilidad extra 12 m | Relación B/C | ROI | Meses para recuperar la inversión |
|---|---|---|---|---|---|---|
| Conservador | 5 % | *(completar)* | *(completar)* | *(completar)* | *(completar)* | *(completar)* |
| Base | 10 % | *(completar)* | *(completar)* | *(completar)* | *(completar)* | *(completar)* |
| Optimista | 15 % | *(completar)* | *(completar)* | *(completar)* | *(completar)* | *(completar)* |

> Copiar aquí los valores que muestra el notebook 06 después de ejecutar el pipeline.

**Beneficios adicionales no monetizados:** decisiones con datos confiables en vez de intuición, eliminación del
trabajo manual de consolidar archivos, trazabilidad de cada dato (archivo de origen y fecha de carga) y una base
reutilizable para nuevos modelos (recomendación de productos, pronóstico de demanda).

---

## 3. Arquitectura propuesta

![Arquitectura](docs/arquitectura.png)

### Explicación técnica

| Componente | Función |
|---|---|
| **Kaggle (fuente)** | 5 archivos CSV mensuales con los eventos de la tienda (oct-2019 a feb-2020). |
| **Unity Catalog Volume `raw`** | Zona de aterrizaje: aquí se suben los archivos tal como vienen. |
| **Auto Loader (`cloudFiles`)** | Detecta archivos nuevos y los carga de forma incremental a Bronze. Guarda checkpoint y esquema en el volume `checkpoints`. |
| **Delta Lake** | Formato de todas las tablas: transacciones ACID, historial de versiones (*time travel*) y lectura rápida. |
| **Capas Bronze / Silver / Gold** | Arquitectura Medallion: crudo → limpio → listo para el negocio (sección 4). |
| **Lakeflow Jobs (serverless)** | Orquesta los notebooks en orden con dependencias; se puede programar diariamente. |
| **MLflow** | Registra parámetros, métricas, curva ROC y el modelo entrenado. |
| **AI/BI Dashboard + SQL Warehouse** | Visualización para usuarios de negocio. |
| **GitHub + GitHub Actions** | Control de versiones del código y pruebas automáticas (CI). Databricks lee el código con un *Git folder*. |

**Por qué Databricks:** une en una sola plataforma el almacenamiento (Lakehouse), el procesamiento distribuido con
Spark, la orquestación, el registro de modelos y la visualización. La arquitectura Medallion es el patrón
recomendado por la propia plataforma. La versión gratuita (Free Edition) usa cómputo *serverless*, por lo que no
hay que administrar clusters.

---

## 4. Pipeline de ingesta y estrategia Medallion

### Flujo del Job `pipeline_ecommerce_medallion`

```
bronze_ingesta ──► silver_limpieza ──► gold_agregados ──┬──► modelo_ciencia_datos
                                                       └──► beneficio_costo
```

El Job está definido como código en [`resources/job_pipeline.yml`](resources/job_pipeline.yml)
(Databricks Asset Bundle) y también puede crearse desde la interfaz (ver guía).

### 🥉 Bronze — `notebooks/02_bronze_ingesta.py`
- **Auto Loader** con `trigger(availableNow=True)`: procesa todos los archivos pendientes y se detiene.
- **Incremental**: el checkpoint recuerda los archivos ya cargados; si llega `2020-Mar.csv`, solo se carga ese.
- Todas las columnas se guardan como **texto** para no perder datos por errores de tipo.
- Columnas de linaje: `_archivo_origen`, `_fecha_ingesta`; columna `_rescued_data` para datos inesperados.
- Auditoría: filas cargadas por archivo.

**Tabla:** `bronze_eventos`

### 🥈 Silver — `notebooks/03_silver_limpieza.py`

| Regla | Acción |
|---|---|
| Fecha con formato inválido | Cuarentena (`fecha_invalida`) |
| Tipo de evento distinto de view / cart / remove_from_cart / purchase | Cuarentena (`tipo_evento_invalido`) |
| Producto o usuario vacío | Cuarentena |
| Precio no numérico o negativo | Cuarentena (`precio_invalido` / `precio_negativo`) |
| Eventos repetidos exactos | Se eliminan (se cuentan en el log) |
| Marca o categoría vacía | Se reemplaza por `sin_marca` / `sin_categoria` |

- Conversión de tipos con `try_cast` / `try_to_timestamp`: un dato malo no detiene el pipeline.
- Enriquecimiento: `fecha`, `mes`, `hora`, `dia_semana`, `es_fin_de_semana`, `categoria_principal`.
- Columnas renombradas a español.

**Tablas:** `silver_eventos`, `silver_eventos_rechazados` (con motivo), `log_calidad_datos` (una fila por ejecución).

### 🥇 Gold — `notebooks/04_gold_agregados.py`

| Tabla | Para qué sirve |
|---|---|
| `gold_kpis_generales` | Ingresos, usuarios, compradores y conversión total |
| `gold_ventas_diarias` | Tendencia diaria de ventas y tráfico |
| `gold_embudo_mensual` | Conversión vista → carrito → compra por mes |
| `gold_sesiones` | Una fila por sesión con su comportamiento (insumo del modelo) |
| `gold_carritos_abandonados_mensual` | Sesiones y valor abandonado por mes |
| `gold_marcas` / `gold_categorias` | Ranking de ingresos y conversión |
| `gold_actividad_hora` | Mapa de calor día × hora |
| `gold_clientes_rfm` | Segmentación de clientes (Recencia, Frecuencia, Monto) |

### Modelo de datos

```
bronze_eventos (texto crudo + linaje)
      │  tipado, validación, deduplicación
      ▼
silver_eventos ───────────────► silver_eventos_rechazados
(1 fila = 1 evento limpio)       log_calidad_datos
      │
      ├─► gold_sesiones (1 fila = 1 sesión) ─► gold_carritos_abandonados_mensual
      │                                     └► gold_probabilidad_compra (modelo)
      ├─► gold_kpis_generales · gold_ventas_diarias · gold_embudo_mensual
      ├─► gold_marcas · gold_categorias · gold_actividad_hora
      └─► gold_clientes_rfm
```

---

## 5. Modelos de ciencia de datos

Notebook: `notebooks/05_modelo_ciencia_datos.py`

1. **Estadística descriptiva:** distribución de eventos por tipo; media, mediana, desviación estándar, cuartiles y
   asimetría del precio de compra; histograma de precios, serie de ingresos diarios y embudo.
2. **Análisis estadístico:**
   - Prueba **Chi-cuadrado** de independencia: ¿la probabilidad de comprar cambia entre semana y fin de semana?
   - **Matriz de correlación** entre las variables de comportamiento de la sesión.
3. **Modelado:** **regresión logística** que estima la probabilidad de que una sesión con carrito termine en compra.
   - Variables: vistas, productos agregados y retirados del carrito, productos y categorías distintas, precio
     promedio visto, valor del carrito, duración, hora de inicio y fin de semana.
   - Evaluación sobre 25 % de prueba: **AUC-ROC**, exactitud, precisión, sensibilidad y F1; **curva ROC** y peso de
     cada variable.
   - Todo queda registrado en **MLflow** (parámetros, métricas, gráficos, matriz de confusión y modelo).
   - Las probabilidades se guardan en `gold_probabilidad_compra` con tres grupos: *Alto riesgo de abandono*,
     *Riesgo medio* y *Probable compra*. Marketing puede priorizar a los de alto riesgo.

**Resultados (completar después de ejecutar):** AUC = ___ · Exactitud = ___ · Chi-cuadrado p-valor = ___

**Limitación:** las variables se calculan con la sesión completa (incluye eventos posteriores a la compra). En
producción se calcularían solo con los eventos anteriores al pago para predecir en tiempo real.

---

## 6. Visualización

Dashboard en **Databricks AI/BI** construido con las consultas de [`dashboard/consultas_dashboard.sql`](dashboard/consultas_dashboard.sql):

| Visual | Fuente |
|---|---|
| Tarjetas KPI: ingresos, usuarios, compradores, conversión | `gold_kpis_generales` |
| Línea de ingresos diarios | `gold_ventas_diarias` |
| Embudo vista → carrito → compra | `gold_embudo_mensual` |
| Valor abandonado y tasa de abandono por mes | `gold_carritos_abandonados_mensual` |
| Top 10 marcas por ingresos | `gold_marcas` |
| Mapa de calor día × hora | `gold_actividad_hora` |
| Segmentos de clientes RFM | `gold_clientes_rfm` |
| Sesiones por grupo de riesgo (modelo) | `gold_probabilidad_compra` |
| Tabla de calidad de datos | `log_calidad_datos` |

*(Agregar aquí capturas del dashboard: `docs/dashboard.png`)*

---

## 7. Integración continua (CI/CD)

- **Control de versiones:** este repositorio en GitHub. Databricks se conecta con un **Git folder**, así el código
  que se ejecuta es el mismo del repositorio.
- **CI (GitHub Actions):** [`.github/workflows/ci.yml`](.github/workflows/ci.yml) se ejecuta en cada *push*:
  1. revisa la sintaxis de todos los notebooks;
  2. ejecuta [`tests/test_pipeline_local.py`](tests/test_pipeline_local.py), que corre las capas Silver y Gold, el
     modelo y el beneficio/costo con **PySpark local** sobre datos sintéticos con el mismo esquema (incluye
     fechas inválidas, precios negativos y duplicados) y verifica las reglas de calidad.
- **CD (opcional):** el Job está declarado en `databricks.yml` + `resources/job_pipeline.yml` y se despliega con
  `databricks bundle deploy`.

---

## 8. Estructura del repositorio

```
├── README.md                         ← este documento
├── databricks.yml                    ← Databricks Asset Bundle
├── resources/job_pipeline.yml        ← definición del Job (orquestación)
├── notebooks/
│   ├── 00_config.py                  ← nombres de catálogo, esquema, volúmenes y tablas
│   ├── 01_setup.py                   ← crea esquema y volúmenes (una sola vez)
│   ├── 02_bronze_ingesta.py          ← BRONZE: Auto Loader incremental
│   ├── 03_silver_limpieza.py         ← SILVER: calidad, cuarentena, duplicados, log
│   ├── 04_gold_agregados.py          ← GOLD: tablas de negocio
│   ├── 05_modelo_ciencia_datos.py    ← descriptiva, Chi-cuadrado, regresión logística + MLflow
│   ├── 06_beneficio_costo.py         ← ROI con datos reales
│   └── 99_ejecutar_todo.py           ← alternativa manual al Job
├── dashboard/consultas_dashboard.sql ← consultas del dashboard
├── docs/
│   ├── arquitectura.png / .dot       ← diagrama de arquitectura
│   └── GUIA_PASO_A_PASO.md           ← cómo ejecutar todo
├── tests/test_pipeline_local.py      ← pruebas automáticas (CI)
├── .github/workflows/ci.yml          ← GitHub Actions
└── requirements-dev.txt
```

**Fuente de datos:** Kechinov, M. *eCommerce Events History in Cosmetics Shop*, Kaggle (datos del proyecto
Open CDP / REES46). Los CSV no se incluyen en el repositorio por su tamaño.
