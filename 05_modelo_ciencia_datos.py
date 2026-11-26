# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Modelos de Ciencia de Datos
# MAGIC
# MAGIC Tres partes, como pide la rúbrica:
# MAGIC 1. **Estadística descriptiva**: cómo se comportan precios, eventos y clientes.
# MAGIC 2. **Análisis estadístico**: ¿la conversión es distinta en fin de semana? (prueba Chi-cuadrado) y correlaciones.
# MAGIC 3. **Modelado**: regresión logística que estima la **probabilidad de que una sesión con carrito termine en compra**.
# MAGIC    Se evalúa con la **curva ROC / AUC** y todo queda registrado en **MLflow** (parámetros, métricas, gráficos y modelo).
# MAGIC
# MAGIC **Uso de negocio del modelo:** identificar sesiones con baja probabilidad de compra para enviarles
# MAGIC un recordatorio o cupón y recuperar carritos abandonados.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F

plt.rcParams.update({"figure.figsize": (9, 4.5), "axes.grid": True, "grid.alpha": 0.3})
COLOR = "#2a6f97"

silver = spark.table(tabla("silver_eventos"))
sesiones = spark.table(tabla("gold_sesiones"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Estadística descriptiva

# COMMAND ----------

# 1.1 Distribución de eventos por tipo
eventos_tipo = silver.groupBy("tipo_evento").count().toPandas().sort_values("count", ascending=False)
eventos_tipo["porcentaje"] = (eventos_tipo["count"] / eventos_tipo["count"].sum() * 100).round(2)
display(eventos_tipo)

# COMMAND ----------

# 1.2 Medidas de tendencia central y dispersión del precio de los productos comprados
compras = silver.filter(F.col("tipo_evento") == "purchase")
resumen_precio = compras.select(
    F.count("precio").alias("n"),
    F.round(F.mean("precio"), 2).alias("media"),
    F.round(F.expr("percentile_approx(precio, 0.5)"), 2).alias("mediana"),
    F.round(F.stddev("precio"), 2).alias("desv_estandar"),
    F.round(F.min("precio"), 2).alias("minimo"),
    F.round(F.expr("percentile_approx(precio, 0.25)"), 2).alias("q1"),
    F.round(F.expr("percentile_approx(precio, 0.75)"), 2).alias("q3"),
    F.round(F.max("precio"), 2).alias("maximo"),
    F.round(F.skewness("precio"), 2).alias("asimetria"),
)
display(resumen_precio)

# COMMAND ----------

# 1.3 Gráficos descriptivos
muestra_precios = compras.select("precio").sample(fraction=0.3, seed=42).limit(200_000).toPandas()
ventas_diarias = spark.table(tabla("gold_ventas_diarias")).toPandas().sort_values("fecha")
embudo = spark.table(tabla("gold_embudo_mensual")).toPandas()

fig, ejes = plt.subplots(1, 3, figsize=(16, 4.5))
ejes[0].hist(muestra_precios["precio"].clip(upper=muestra_precios["precio"].quantile(0.99)), bins=40, color=COLOR)
ejes[0].set_title("Distribución del precio de compra (USD)")
ejes[0].set_xlabel("Precio"); ejes[0].set_ylabel("Frecuencia")

ejes[1].plot(pd.to_datetime(ventas_diarias["fecha"]), ventas_diarias["ingresos_usd"], color=COLOR)
ejes[1].set_title("Ingresos diarios (USD)"); ejes[1].tick_params(axis="x", rotation=45)

totales = [embudo["usuarios_vieron"].sum(), embudo["usuarios_carrito"].sum(), embudo["usuarios_compraron"].sum()]
ejes[2].bar(["Vieron", "Carrito", "Compraron"], totales, color=[COLOR, "#61a5c2", "#e07a5f"])
ejes[2].set_title("Embudo de conversión (usuarios-mes)")
plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Análisis estadístico

# COMMAND ----------

# 2.1 Prueba Chi-cuadrado: ¿la compra depende de si la sesión es en fin de semana?
from scipy.stats import chi2_contingency

tabla_contingencia = (sesiones.groupBy("fin_de_semana")
                      .agg(F.sum("compro").alias("con_compra"),
                           F.sum(1 - F.col("compro")).alias("sin_compra"))
                      .orderBy("fin_de_semana").toPandas())
chi2, p_valor, gl, _ = chi2_contingency(tabla_contingencia[["con_compra", "sin_compra"]].values)
tabla_contingencia["tasa_compra_pct"] = (tabla_contingencia["con_compra"] /
                                         (tabla_contingencia["con_compra"] + tabla_contingencia["sin_compra"]) * 100).round(3)
display(tabla_contingencia)
print(f"Chi-cuadrado = {chi2:,.2f} | grados de libertad = {gl} | p-valor = {p_valor:.4g}")
print("Conclusión:", "la diferencia ES estadísticamente significativa (p < 0.05)" if p_valor < 0.05
      else "NO hay diferencia significativa (p >= 0.05)")

# COMMAND ----------

# 2.2 Matriz de correlación entre variables de la sesión
VARIABLES = ["vistas", "agregados_carrito", "retirados_carrito", "productos_distintos",
             "categorias_distintas", "precio_promedio_visto", "valor_agregado_carrito",
             "duracion_min", "hora_inicio", "fin_de_semana"]

muestra_sesiones = (sesiones.filter(F.col("agregados_carrito") > 0)
                    .select(*VARIABLES, "compro")
                    .sample(fraction=0.5, seed=42).limit(300_000).toPandas())
muestra_sesiones = muestra_sesiones.fillna(0)

correlacion = muestra_sesiones.corr().round(2)
fig, ax = plt.subplots(figsize=(10, 8))
im = ax.imshow(correlacion, cmap="RdBu_r", vmin=-1, vmax=1)
ax.set_xticks(range(len(correlacion))); ax.set_xticklabels(correlacion.columns, rotation=60, ha="right")
ax.set_yticks(range(len(correlacion))); ax.set_yticklabels(correlacion.columns)
for i in range(len(correlacion)):
    for j in range(len(correlacion)):
        ax.text(j, i, correlacion.iloc[i, j], ha="center", va="center", fontsize=8)
fig.colorbar(im); ax.set_title("Correlación entre variables de la sesión"); ax.grid(False)
plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Modelado: probabilidad de compra de una sesión con carrito
# MAGIC
# MAGIC - **Población:** sesiones que agregaron al menos un producto al carrito.
# MAGIC - **Variable objetivo:** `compro` (1 = la sesión terminó en compra, 0 = carrito abandonado).
# MAGIC - **Modelo:** regresión logística (interpretable) con variables estandarizadas.
# MAGIC - **Evaluación:** AUC-ROC, exactitud, precisión, sensibilidad (recall) y F1 sobre un 25% de prueba.

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (accuracy_score, f1_score, precision_score, recall_score,
                             roc_auc_score, roc_curve, confusion_matrix)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

X = muestra_sesiones[VARIABLES]
y = muestra_sesiones["compro"].astype(int)
X_entreno, X_prueba, y_entreno, y_prueba = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)

PARAMS = {"C": 1.0, "max_iter": 500, "class_weight": "balanced", "test_size": 0.25,
          "filas_entrenamiento": len(X_entreno), "filas_prueba": len(X_prueba)}

with mlflow.start_run(run_name="regresion_logistica_conversion") as corrida:
    modelo = Pipeline([
        ("escalar", StandardScaler()),
        ("logistica", LogisticRegression(C=PARAMS["C"], max_iter=PARAMS["max_iter"],
                                         class_weight=PARAMS["class_weight"])),
    ])
    modelo.fit(X_entreno, y_entreno)

    prob = modelo.predict_proba(X_prueba)[:, 1]
    pred = (prob >= 0.5).astype(int)
    metricas = {
        "auc_roc": roc_auc_score(y_prueba, prob),
        "exactitud": accuracy_score(y_prueba, pred),
        "precision": precision_score(y_prueba, pred, zero_division=0),
        "sensibilidad_recall": recall_score(y_prueba, pred, zero_division=0),
        "f1": f1_score(y_prueba, pred, zero_division=0),
        "tasa_compra_real": float(y.mean()),
    }

    # Curva ROC
    fpr, tpr, _ = roc_curve(y_prueba, prob)
    fig_roc, ax = plt.subplots(figsize=(6, 5))
    ax.plot(fpr, tpr, color=COLOR, lw=2, label=f"Regresión logística (AUC = {metricas['auc_roc']:.3f})")
    ax.plot([0, 1], [0, 1], "--", color="gray", label="Azar (AUC = 0.5)")
    ax.set_xlabel("Tasa de falsos positivos"); ax.set_ylabel("Tasa de verdaderos positivos")
    ax.set_title("Curva ROC — probabilidad de compra"); ax.legend(loc="lower right")
    plt.tight_layout()

    # Importancia de variables (coeficientes estandarizados)
    coef = pd.Series(modelo.named_steps["logistica"].coef_[0], index=VARIABLES).sort_values()
    fig_coef, ax2 = plt.subplots(figsize=(7, 5))
    ax2.barh(coef.index, coef.values, color=[("#e07a5f" if v < 0 else COLOR) for v in coef.values])
    ax2.set_title("Peso de cada variable (coeficientes)"); plt.tight_layout()

    # Registro en MLflow
    mlflow.log_params(PARAMS)
    mlflow.log_metrics(metricas)
    mlflow.log_figure(fig_roc, "curva_roc.png")
    mlflow.log_figure(fig_coef, "coeficientes.png")
    mlflow.log_dict({"matriz_confusion": confusion_matrix(y_prueba, pred).tolist()}, "matriz_confusion.json")
    # Guardar el modelo entrenado (compatible con MLflow 2.x y 3.x)
    modelo_guardado = False
    for destino in ({"name": "modelo_conversion"}, {"artifact_path": "modelo_conversion"}):
        try:
            mlflow.sklearn.log_model(modelo, serialization_format="cloudpickle",
                                     input_example=X_prueba.head(5), **destino)
            modelo_guardado = True
            break
        except TypeError:
            continue
        except Exception as error:  # el run igual conserva métricas y gráficos
            print("Aviso: no se pudo guardar el modelo en MLflow:", error)
            break
    mlflow.set_tag("modelo_guardado", str(modelo_guardado))

    id_corrida = corrida.info.run_id

print(f"MLflow run_id: {id_corrida}")
display(pd.DataFrame([metricas]).round(4))
display(fig_roc)
display(fig_coef)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Guardar las probabilidades en una tabla Gold (para el dashboard y el equipo de marketing)

# COMMAND ----------

resultado = X_prueba.copy()
resultado["compro_real"] = y_prueba.values
resultado["probabilidad_compra"] = prob.round(4)
resultado["grupo_riesgo"] = pd.cut(resultado["probabilidad_compra"], bins=[-0.01, 0.3, 0.6, 1.0],
                                   labels=["Alto riesgo de abandono", "Riesgo medio", "Probable compra"]).astype(str)

(spark.createDataFrame(resultado)
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(tabla("gold_probabilidad_compra")))

display(spark.table(tabla("gold_probabilidad_compra"))
        .groupBy("grupo_riesgo")
        .agg(F.count("*").alias("sesiones"),
             F.round(F.avg("compro_real") * 100, 2).alias("tasa_compra_real_pct"),
             F.round(F.sum("valor_agregado_carrito"), 2).alias("valor_carrito_usd"))
        .orderBy("grupo_riesgo"))
