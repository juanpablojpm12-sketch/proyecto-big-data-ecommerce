# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Capa GOLD — Tablas listas para el negocio
# MAGIC
# MAGIC **Objetivo:** responder las preguntas del caso de negocio con tablas agregadas, pequeñas y rápidas,
# MAGIC que alimentan el dashboard y el modelo.
# MAGIC
# MAGIC | Tabla Gold | Pregunta de negocio que responde |
# MAGIC |---|---|
# MAGIC | `gold_kpis_generales` | ¿Cuánto vendimos, cuántos clientes y cuál es la conversión total? |
# MAGIC | `gold_ventas_diarias` | ¿Cómo evolucionan las ventas y el tráfico día a día? |
# MAGIC | `gold_embudo_mensual` | ¿En qué paso del embudo (ver → carrito → compra) perdemos clientes? |
# MAGIC | `gold_carritos_abandonados_mensual` | ¿Cuánto dinero queda en carritos abandonados cada mes? |
# MAGIC | `gold_marcas` | ¿Qué marcas venden más y cuáles convierten mejor? |
# MAGIC | `gold_categorias` | ¿Qué categorías generan más ingresos? |
# MAGIC | `gold_actividad_hora` | ¿A qué hora y qué día compran los clientes? |
# MAGIC | `gold_sesiones` | Comportamiento por sesión (insumo del modelo de ciencia de datos) |
# MAGIC | `gold_clientes_rfm` | Segmentación de clientes por Recencia, Frecuencia y Monto |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

SILVER = tabla("silver_eventos")


def crear_tabla_gold(nombre: str, consulta_sql: str):
    """Ejecuta una consulta SQL y guarda el resultado como tabla Gold (se reemplaza en cada ejecución)."""
    df = spark.sql(consulta_sql)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabla(nombre))
    print(f"✔ {tabla(nombre)}: {spark.table(tabla(nombre)).count():,} filas")

# COMMAND ----------

# KPIs generales (una sola fila)
crear_tabla_gold("gold_kpis_generales", f"""
    SELECT
        COUNT(*)                                                     AS total_eventos,
        COUNT(DISTINCT usuario_id)                                   AS usuarios_unicos,
        COUNT(DISTINCT sesion_id)                                    AS sesiones,
        COUNT(DISTINCT producto_id)                                  AS productos,
        SUM(CASE WHEN tipo_evento = 'purchase' THEN 1 ELSE 0 END)    AS unidades_vendidas,
        ROUND(SUM(CASE WHEN tipo_evento = 'purchase' THEN precio ELSE 0 END), 2) AS ingresos_usd,
        COUNT(DISTINCT CASE WHEN tipo_evento = 'purchase' THEN usuario_id END)   AS clientes_compradores,
        ROUND(COUNT(DISTINCT CASE WHEN tipo_evento = 'purchase' THEN usuario_id END)
              / COUNT(DISTINCT usuario_id) * 100, 2)                 AS tasa_conversion_usuarios_pct,
        MIN(fecha)                                                   AS fecha_inicio,
        MAX(fecha)                                                   AS fecha_fin
    FROM {SILVER}
""")

# COMMAND ----------

# Ventas y tráfico diario
crear_tabla_gold("gold_ventas_diarias", f"""
    SELECT
        fecha,
        COUNT(*)                                                    AS eventos,
        SUM(CASE WHEN tipo_evento = 'view'     THEN 1 ELSE 0 END)   AS vistas,
        SUM(CASE WHEN tipo_evento = 'cart'     THEN 1 ELSE 0 END)   AS agregados_carrito,
        SUM(CASE WHEN tipo_evento = 'purchase' THEN 1 ELSE 0 END)   AS unidades_vendidas,
        ROUND(SUM(CASE WHEN tipo_evento = 'purchase' THEN precio ELSE 0 END), 2) AS ingresos_usd,
        COUNT(DISTINCT usuario_id)                                  AS usuarios_activos,
        COUNT(DISTINCT CASE WHEN tipo_evento = 'purchase' THEN usuario_id END) AS compradores
    FROM {SILVER}
    GROUP BY fecha
    ORDER BY fecha
""")

# COMMAND ----------

# Embudo de conversión mensual (a nivel de usuarios)
crear_tabla_gold("gold_embudo_mensual", f"""
    WITH por_mes AS (
        SELECT
            mes,
            COUNT(DISTINCT CASE WHEN tipo_evento = 'view'     THEN usuario_id END) AS usuarios_vieron,
            COUNT(DISTINCT CASE WHEN tipo_evento = 'cart'     THEN usuario_id END) AS usuarios_carrito,
            COUNT(DISTINCT CASE WHEN tipo_evento = 'purchase' THEN usuario_id END) AS usuarios_compraron,
            SUM(CASE WHEN tipo_evento = 'remove_from_cart' THEN 1 ELSE 0 END)      AS productos_retirados_carrito
        FROM {SILVER}
        GROUP BY mes
    )
    SELECT *,
        ROUND(usuarios_carrito   / NULLIF(usuarios_vieron, 0)  * 100, 2) AS tasa_vista_a_carrito_pct,
        ROUND(usuarios_compraron / NULLIF(usuarios_carrito, 0) * 100, 2) AS tasa_carrito_a_compra_pct,
        ROUND(usuarios_compraron / NULLIF(usuarios_vieron, 0)  * 100, 2) AS tasa_conversion_total_pct
    FROM por_mes
    ORDER BY mes
""")

# COMMAND ----------

# Resumen por sesión (insumo para carritos abandonados y para el modelo)
crear_tabla_gold("gold_sesiones", f"""
    SELECT
        sesion_id,
        MIN(usuario_id)                                                 AS usuario_id,
        MIN(fecha_evento)                                               AS inicio_sesion,
        date_format(MIN(fecha_evento), 'yyyy-MM')                       AS mes,
        ROUND((unix_timestamp(MAX(fecha_evento)) - unix_timestamp(MIN(fecha_evento))) / 60, 2) AS duracion_min,
        hour(MIN(fecha_evento))                                         AS hora_inicio,
        CASE WHEN dayofweek(MIN(fecha_evento)) IN (1, 7) THEN 1 ELSE 0 END AS fin_de_semana,
        COUNT(*)                                                        AS eventos,
        SUM(CASE WHEN tipo_evento = 'view'             THEN 1 ELSE 0 END) AS vistas,
        SUM(CASE WHEN tipo_evento = 'cart'             THEN 1 ELSE 0 END) AS agregados_carrito,
        SUM(CASE WHEN tipo_evento = 'remove_from_cart' THEN 1 ELSE 0 END) AS retirados_carrito,
        SUM(CASE WHEN tipo_evento = 'purchase'         THEN 1 ELSE 0 END) AS unidades_compradas,
        COUNT(DISTINCT producto_id)                                     AS productos_distintos,
        COUNT(DISTINCT categoria_principal)                             AS categorias_distintas,
        ROUND(AVG(precio), 2)                                           AS precio_promedio_visto,
        ROUND(SUM(CASE WHEN tipo_evento = 'cart'     THEN precio ELSE 0 END), 2) AS valor_agregado_carrito,
        ROUND(SUM(CASE WHEN tipo_evento = 'purchase' THEN precio ELSE 0 END), 2) AS valor_comprado,
        CASE WHEN SUM(CASE WHEN tipo_evento = 'purchase' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS compro
    FROM {SILVER}
    WHERE sesion_id IS NOT NULL AND sesion_id <> ''
    GROUP BY sesion_id
""")

# COMMAND ----------

# Carritos abandonados por mes: sesiones que agregaron al carrito y NO compraron
crear_tabla_gold("gold_carritos_abandonados_mensual", f"""
    SELECT
        mes,
        SUM(CASE WHEN agregados_carrito > 0 THEN 1 ELSE 0 END)                   AS sesiones_con_carrito,
        SUM(CASE WHEN agregados_carrito > 0 AND compro = 1 THEN 1 ELSE 0 END)    AS sesiones_con_compra,
        SUM(CASE WHEN agregados_carrito > 0 AND compro = 0 THEN 1 ELSE 0 END)    AS sesiones_abandonadas,
        ROUND(SUM(CASE WHEN agregados_carrito > 0 AND compro = 0 THEN valor_agregado_carrito ELSE 0 END), 2)
                                                                                 AS valor_abandonado_usd,
        ROUND(SUM(CASE WHEN agregados_carrito > 0 AND compro = 0 THEN 1 ELSE 0 END)
              / NULLIF(SUM(CASE WHEN agregados_carrito > 0 THEN 1 ELSE 0 END), 0) * 100, 2)
                                                                                 AS tasa_abandono_pct,
        ROUND(SUM(valor_comprado), 2)                                            AS ingresos_usd
    FROM {tabla('gold_sesiones')}
    GROUP BY mes
    ORDER BY mes
""")

# COMMAND ----------

# Desempeño por marca
crear_tabla_gold("gold_marcas", f"""
    SELECT
        marca,
        SUM(CASE WHEN tipo_evento = 'view'     THEN 1 ELSE 0 END)   AS vistas,
        SUM(CASE WHEN tipo_evento = 'cart'     THEN 1 ELSE 0 END)   AS agregados_carrito,
        SUM(CASE WHEN tipo_evento = 'purchase' THEN 1 ELSE 0 END)   AS unidades_vendidas,
        ROUND(SUM(CASE WHEN tipo_evento = 'purchase' THEN precio ELSE 0 END), 2) AS ingresos_usd,
        ROUND(AVG(precio), 2)                                       AS precio_promedio,
        ROUND(SUM(CASE WHEN tipo_evento = 'purchase' THEN 1 ELSE 0 END)
              / NULLIF(SUM(CASE WHEN tipo_evento = 'view' THEN 1 ELSE 0 END), 0) * 100, 2) AS conversion_vista_compra_pct
    FROM {SILVER}
    GROUP BY marca
    ORDER BY ingresos_usd DESC
""")

# COMMAND ----------

# Desempeño por categoría principal
crear_tabla_gold("gold_categorias", f"""
    SELECT
        categoria_principal,
        COUNT(DISTINCT producto_id)                                 AS productos,
        SUM(CASE WHEN tipo_evento = 'view'     THEN 1 ELSE 0 END)   AS vistas,
        SUM(CASE WHEN tipo_evento = 'purchase' THEN 1 ELSE 0 END)   AS unidades_vendidas,
        ROUND(SUM(CASE WHEN tipo_evento = 'purchase' THEN precio ELSE 0 END), 2) AS ingresos_usd
    FROM {SILVER}
    GROUP BY categoria_principal
    ORDER BY ingresos_usd DESC
""")

# COMMAND ----------

# Actividad por día de la semana y hora (mapa de calor)
crear_tabla_gold("gold_actividad_hora", f"""
    SELECT
        dia_semana_num,
        dia_semana,
        hora,
        COUNT(*)                                                    AS eventos,
        SUM(CASE WHEN tipo_evento = 'purchase' THEN 1 ELSE 0 END)   AS unidades_vendidas,
        ROUND(SUM(CASE WHEN tipo_evento = 'purchase' THEN precio ELSE 0 END), 2) AS ingresos_usd
    FROM {SILVER}
    GROUP BY dia_semana_num, dia_semana, hora
    ORDER BY dia_semana_num, hora
""")

# COMMAND ----------

# Segmentación RFM de clientes (Recencia, Frecuencia, Monto)
crear_tabla_gold("gold_clientes_rfm", f"""
    WITH compras AS (
        SELECT usuario_id,
               DATEDIFF((SELECT MAX(fecha) FROM {SILVER}), MAX(fecha)) AS recencia_dias,
               COUNT(DISTINCT sesion_id)                               AS frecuencia_compras,
               ROUND(SUM(precio), 2)                                   AS monto_usd
        FROM {SILVER}
        WHERE tipo_evento = 'purchase'
        GROUP BY usuario_id
    ),
    puntajes AS (
        SELECT *,
               6 - NTILE(5) OVER (ORDER BY recencia_dias)   AS puntaje_r,   -- más reciente = 5
               NTILE(5) OVER (ORDER BY frecuencia_compras)  AS puntaje_f,
               NTILE(5) OVER (ORDER BY monto_usd)           AS puntaje_m
        FROM compras
    )
    SELECT *,
        CASE
            WHEN puntaje_r >= 4 AND puntaje_f >= 4 AND puntaje_m >= 4 THEN '1. Campeones'
            WHEN puntaje_f >= 4                                     THEN '2. Leales'
            WHEN puntaje_r >= 4                                     THEN '3. Recientes / nuevos'
            WHEN puntaje_r <= 2 AND puntaje_m >= 4                  THEN '4. Valiosos en riesgo'
            ELSE '5. Ocasionales'
        END AS segmento
    FROM puntajes
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vista rápida de resultados

# COMMAND ----------

display(spark.table(tabla("gold_kpis_generales")))
display(spark.table(tabla("gold_embudo_mensual")))
display(spark.table(tabla("gold_carritos_abandonados_mensual")))
