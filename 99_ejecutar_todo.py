# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Ejecutar todo el pipeline en orden (alternativa manual al Job)
# MAGIC
# MAGIC Lo recomendado es usar el **Job** `pipeline_ecommerce_medallion` (ver `docs/GUIA_PASO_A_PASO.md`).
# MAGIC Si no alcanzas a crear el Job, este notebook corre los pasos uno tras otro y muestra el tiempo de cada uno.

# COMMAND ----------

import time

PASOS = [
    "./02_bronze_ingesta",
    "./03_silver_limpieza",
    "./04_gold_agregados",
    "./05_modelo_ciencia_datos",
    "./06_beneficio_costo",
]

for paso in PASOS:
    inicio = time.time()
    print(f"▶ Ejecutando {paso} ...")
    dbutils.notebook.run(paso, 3600)
    print(f"  ✔ terminado en {(time.time() - inicio) / 60:.1f} min")

print("Pipeline completo ✅")
