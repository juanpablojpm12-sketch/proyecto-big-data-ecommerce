# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Capa SILVER — Limpieza, validación y estandarización
# MAGIC
# MAGIC **Objetivo:** convertir los datos crudos de Bronze en datos **confiables**.
# MAGIC
# MAGIC | Paso | Qué se hace | V del Big Data |
# MAGIC |---|---|---|
# MAGIC | 1. Tipado | Texto → fecha/hora, números enteros y decimales (con `try_cast`, sin romper el proceso) | Veracidad |
# MAGIC | 2. Reglas de calidad | Se rechazan filas con fecha inválida, tipo de evento desconocido, IDs vacíos o precio negativo | Veracidad |
# MAGIC | 3. Cuarentena | Las filas rechazadas **no se borran**: van a `silver_eventos_rechazados` con el motivo | Veracidad |
# MAGIC | 4. Duplicados | Se eliminan eventos repetidos exactos | Veracidad |
# MAGIC | 5. Enriquecimiento | Marca/categoría vacías → `sin_marca` / `sin_categoria`; columnas de fecha, hora, día y categoría principal | Valor |
# MAGIC | 6. Log de calidad | Se registra cada ejecución en `log_calidad_datos` | Trazabilidad |
# MAGIC
# MAGIC Las columnas se renombran a español para que el modelo de datos sea fácil de leer.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

bronze = spark.table(tabla("bronze_eventos"))
filas_bronze = bronze.count()

# ---------------------------------------------------------------
# 1. Tipado seguro (try_cast devuelve NULL en vez de fallar)
# ---------------------------------------------------------------
tipado = bronze.select(
    F.expr("try_to_timestamp(regexp_replace(event_time, ' UTC$', ''), 'yyyy-MM-dd HH:mm:ss')").alias("fecha_evento"),
    F.lower(F.trim(F.col("event_type"))).alias("tipo_evento"),
    F.expr("try_cast(product_id AS BIGINT)").alias("producto_id"),
    F.expr("try_cast(category_id AS BIGINT)").alias("categoria_id"),
    F.lower(F.trim(F.col("category_code"))).alias("categoria_codigo"),
    F.lower(F.trim(F.col("brand"))).alias("marca"),
    F.expr("try_cast(price AS DOUBLE)").alias("precio"),
    F.expr("try_cast(user_id AS BIGINT)").alias("usuario_id"),
    F.trim(F.col("user_session")).alias("sesion_id"),
    F.col("_archivo_origen").alias("archivo_origen"),
)

# ---------------------------------------------------------------
# 2. Reglas de calidad: se asigna el PRIMER motivo de rechazo
# ---------------------------------------------------------------
motivo = (
    F.when(F.col("fecha_evento").isNull(), "fecha_invalida")
    .when(~F.col("tipo_evento").isin(EVENTOS_VALIDOS) | F.col("tipo_evento").isNull(), "tipo_evento_invalido")
    .when(F.col("producto_id").isNull(), "producto_vacio")
    .when(F.col("usuario_id").isNull(), "usuario_vacio")
    .when(F.col("precio").isNull(), "precio_invalido")
    .when(F.col("precio") < 0, "precio_negativo")
    .otherwise(F.lit(None))
)
evaluado = tipado.withColumn("motivo_rechazo", motivo)

rechazados = evaluado.filter(F.col("motivo_rechazo").isNotNull())
validos = evaluado.filter(F.col("motivo_rechazo").isNull()).drop("motivo_rechazo")

# ---------------------------------------------------------------
# 3. Cuarentena de filas rechazadas
# ---------------------------------------------------------------
(rechazados.withColumn("fecha_proceso", F.current_timestamp())
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(tabla("silver_eventos_rechazados")))

# COMMAND ----------

# ---------------------------------------------------------------
# 4. Eliminación de duplicados exactos
# ---------------------------------------------------------------
columnas_clave = ["fecha_evento", "tipo_evento", "producto_id", "usuario_id", "sesion_id", "precio"]
sin_duplicados = validos.dropDuplicates(columnas_clave)

# ---------------------------------------------------------------
# 5. Estandarización y enriquecimiento
# ---------------------------------------------------------------
silver = (
    sin_duplicados
    .withColumn("marca", F.when(F.col("marca").isNull() | (F.col("marca") == ""), "sin_marca")
                .otherwise(F.col("marca")))
    .withColumn("categoria_codigo", F.when(F.col("categoria_codigo").isNull() | (F.col("categoria_codigo") == ""),
                                           "sin_categoria").otherwise(F.col("categoria_codigo")))
    .withColumn("categoria_principal", F.split(F.col("categoria_codigo"), r"\.").getItem(0))
    .withColumn("fecha", F.to_date("fecha_evento"))
    .withColumn("mes", F.date_format("fecha_evento", "yyyy-MM"))
    .withColumn("hora", F.hour("fecha_evento"))
    .withColumn("dia_semana_num", F.dayofweek("fecha_evento"))          # 1 = domingo ... 7 = sábado
    .withColumn("dia_semana", F.date_format("fecha_evento", "E"))         # Sun, Mon, ...
    .withColumn("es_fin_de_semana", F.col("dia_semana_num").isin(1, 7))
    .withColumn("fecha_proceso", F.current_timestamp())
)

(silver.write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(tabla("silver_eventos")))

print("Tabla Silver escrita:", tabla("silver_eventos"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Log de calidad de datos (una fila por ejecución)

# COMMAND ----------

silver_t = spark.table(tabla("silver_eventos"))
filas_rechazadas = spark.table(tabla("silver_eventos_rechazados")).count()
filas_validas = filas_bronze - filas_rechazadas
filas_silver = silver_t.count()

metricas = silver_t.agg(
    F.avg(F.when(F.col("marca") == "sin_marca", 1).otherwise(0)).alias("pct_sin_marca"),
    F.avg(F.when(F.col("categoria_codigo") == "sin_categoria", 1).otherwise(0)).alias("pct_sin_categoria"),
).first()

log = spark.createDataFrame(
    [(filas_bronze, filas_rechazadas, filas_validas - filas_silver, filas_silver,
      round(float(metricas["pct_sin_marca"] or 0) * 100, 2),
      round(float(metricas["pct_sin_categoria"] or 0) * 100, 2))],
    "filas_bronze BIGINT, filas_rechazadas BIGINT, duplicados_eliminados BIGINT, filas_silver BIGINT, "
    "pct_sin_marca DOUBLE, pct_sin_categoria DOUBLE",
).withColumn("fecha_ejecucion", F.current_timestamp())

log.write.mode("append").option("mergeSchema", "true").saveAsTable(tabla("log_calidad_datos"))

display(spark.table(tabla("log_calidad_datos")).orderBy(F.desc("fecha_ejecucion")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Motivos de rechazo

# COMMAND ----------

display(
    spark.table(tabla("silver_eventos_rechazados"))
    .groupBy("motivo_rechazo").count().orderBy(F.desc("count"))
)
