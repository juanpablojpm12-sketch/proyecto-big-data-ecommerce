# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Relación Beneficio / Costo (con los datos reales de la capa Gold)
# MAGIC
# MAGIC **Idea central:** cada mes queda dinero "olvidado" en carritos abandonados. Si la plataforma de datos
# MAGIC permite recuperar un porcentaje de ese dinero (recordatorios, cupones a sesiones de alto riesgo,
# MAGIC promociones en horas pico), ese ingreso extra paga la inversión.
# MAGIC
# MAGIC Los **supuestos** de la celda siguiente son editables. Los valores de carritos abandonados salen
# MAGIC directamente de `gold_carritos_abandonados_mensual`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# ------------------------- SUPUESTOS (editables) -------------------------
MARGEN_BRUTO = 0.40                 # 40 % de cada venta es utilidad bruta (cosméticos)
COSTO_IMPLEMENTACION_USD = 8_000    # costo único: diseño y construcción del pipeline (≈ 2 meses de 1 ingeniero de datos)
COSTO_PLATAFORMA_MES_USD = 600      # Databricks (cómputo + almacenamiento) en versión de pago
COSTO_OPERACION_MES_USD = 900       # mantenimiento y analista (tiempo parcial)
HORIZONTE_MESES = 12
ESCENARIOS = {"Conservador": 0.05, "Base": 0.10, "Optimista": 0.15}   # % del valor abandonado que se recupera
# --------------------------------------------------------------------------

import pandas as pd

abandonos = spark.table(tabla("gold_carritos_abandonados_mensual")).toPandas().sort_values("mes")
display(abandonos)

valor_abandonado_mes = float(abandonos["valor_abandonado_usd"].mean())
ingresos_mes = float(abandonos["ingresos_usd"].mean())
print(f"Valor promedio abandonado en carritos por mes: USD {valor_abandonado_mes:,.0f}")
print(f"Ingresos promedio por mes:                     USD {ingresos_mes:,.0f}")

# COMMAND ----------

costo_total = COSTO_IMPLEMENTACION_USD + (COSTO_PLATAFORMA_MES_USD + COSTO_OPERACION_MES_USD) * HORIZONTE_MESES
costo_mensual = COSTO_PLATAFORMA_MES_USD + COSTO_OPERACION_MES_USD

filas = []
for nombre, tasa in ESCENARIOS.items():
    ventas_recuperadas_mes = valor_abandonado_mes * tasa
    utilidad_mes = ventas_recuperadas_mes * MARGEN_BRUTO
    beneficio_total = utilidad_mes * HORIZONTE_MESES
    ganancia_neta_mes = utilidad_mes - costo_mensual
    filas.append({
        "escenario": nombre,
        "tasa_recuperacion_pct": tasa * 100,
        "ventas_recuperadas_mes_usd": round(ventas_recuperadas_mes, 0),
        "aumento_ingresos_pct": round(ventas_recuperadas_mes / ingresos_mes * 100, 2) if ingresos_mes else None,
        "utilidad_extra_mes_usd": round(utilidad_mes, 0),
        "beneficio_12m_usd": round(beneficio_total, 0),
        "costo_12m_usd": round(costo_total, 0),
        "relacion_beneficio_costo": round(beneficio_total / costo_total, 2),
        "roi_pct": round((beneficio_total - costo_total) / costo_total * 100, 1),
        "meses_recuperar_inversion": (round(COSTO_IMPLEMENTACION_USD / ganancia_neta_mes, 1)
                                      if ganancia_neta_mes > 0 else "No se recupera"),
    })

resultado = pd.DataFrame(filas)
display(resultado)

spark.createDataFrame(resultado.astype({"meses_recuperar_inversion": str})) \
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabla("gold_beneficio_costo"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Cómo leer la tabla:** si la *relación beneficio/costo* es mayor que 1, el proyecto genera más
# MAGIC utilidad de la que cuesta. El ROI indica la ganancia neta por cada dólar invertido en 12 meses.
# MAGIC Copia esta tabla al README (sección 2) después de ejecutar el pipeline.
