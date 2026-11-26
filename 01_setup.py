# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Setup inicial (se ejecuta UNA sola vez)
# MAGIC
# MAGIC Crea el esquema y los dos volúmenes donde vivirán los datos.
# MAGIC Después de ejecutarlo, sube los 5 archivos CSV de Kaggle al volumen **raw**
# MAGIC (Catalog → workspace → ecommerce → Volumes → raw → *Upload to this volume*).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{ESQUEMA} "
          "COMMENT 'Proyecto final Big Data - e-commerce de cosméticos (arquitectura Medallion)'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{ESQUEMA}.{VOLUMEN_RAW} "
          "COMMENT 'Zona de aterrizaje: archivos CSV originales de Kaggle'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{ESQUEMA}.{VOLUMEN_CHECKPOINTS} "
          "COMMENT 'Checkpoints y esquemas de Auto Loader'")

print("Listo. Ahora sube los CSV a:", RUTA_ORIGEN)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación: ¿ya están los archivos?
# MAGIC Ejecuta esta celda después de subir los CSV. Deben aparecer `2019-Oct.csv`, `2019-Nov.csv`,
# MAGIC `2019-Dec.csv`, `2020-Jan.csv` y `2020-Feb.csv`.

# COMMAND ----------

archivos = dbutils.fs.ls(RUTA_ORIGEN)
for a in archivos:
    print(f"{a.name:<20} {a.size / 1024 / 1024:>10.1f} MB")
print(f"\nTotal: {len(archivos)} archivos, {sum(a.size for a in archivos) / 1024**3:.2f} GB")
