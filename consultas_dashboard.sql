-- =====================================================================
-- Consultas para el AI/BI Dashboard "E-commerce cosméticos - Big Data"
-- En Databricks: Dashboards → Create dashboard → pestaña "Data" →
-- "Create from SQL" → pega UNA consulta por dataset y ponle el nombre indicado.
-- =====================================================================

-- Dataset 1: kpis  (widgets tipo "Counter": ingresos, usuarios, conversión, unidades)
SELECT ingresos_usd, usuarios_unicos, clientes_compradores, tasa_conversion_usuarios_pct,
       unidades_vendidas, total_eventos, fecha_inicio, fecha_fin
FROM workspace.ecommerce.gold_kpis_generales;

-- Dataset 2: ventas_diarias  (gráfico de líneas: X = fecha, Y = ingresos_usd)
SELECT fecha, ingresos_usd, unidades_vendidas, usuarios_activos, compradores
FROM workspace.ecommerce.gold_ventas_diarias
ORDER BY fecha;

-- Dataset 3: embudo  (gráfico de barras: X = etapa, Y = usuarios)
SELECT '1. Vieron un producto' AS etapa, SUM(usuarios_vieron)    AS usuarios FROM workspace.ecommerce.gold_embudo_mensual
UNION ALL
SELECT '2. Agregaron al carrito',        SUM(usuarios_carrito)               FROM workspace.ecommerce.gold_embudo_mensual
UNION ALL
SELECT '3. Compraron',                   SUM(usuarios_compraron)             FROM workspace.ecommerce.gold_embudo_mensual
ORDER BY etapa;

-- Dataset 4: carritos_abandonados  (barras: X = mes, Y = valor_abandonado_usd; línea: tasa_abandono_pct)
SELECT mes, sesiones_con_carrito, sesiones_abandonadas, valor_abandonado_usd, tasa_abandono_pct, ingresos_usd
FROM workspace.ecommerce.gold_carritos_abandonados_mensual
ORDER BY mes;

-- Dataset 5: top_marcas  (barras horizontales: Y = marca, X = ingresos_usd)
SELECT marca, ingresos_usd, unidades_vendidas, conversion_vista_compra_pct
FROM workspace.ecommerce.gold_marcas
WHERE marca <> 'sin_marca'
ORDER BY ingresos_usd DESC
LIMIT 10;

-- Dataset 6: actividad_hora  (mapa de calor: X = hora, Y = dia_semana, color = ingresos_usd)
SELECT dia_semana_num, dia_semana, hora, eventos, ingresos_usd
FROM workspace.ecommerce.gold_actividad_hora
ORDER BY dia_semana_num, hora;

-- Dataset 7: segmentos_rfm  (gráfico de torta o barras: segmento vs clientes)
SELECT segmento, COUNT(*) AS clientes, ROUND(SUM(monto_usd), 2) AS ingresos_usd,
       ROUND(AVG(frecuencia_compras), 2) AS compras_promedio
FROM workspace.ecommerce.gold_clientes_rfm
GROUP BY segmento
ORDER BY segmento;

-- Dataset 8: riesgo_abandono (resultado del modelo; barras: grupo_riesgo vs valor_carrito_usd)
SELECT grupo_riesgo, COUNT(*) AS sesiones,
       ROUND(AVG(probabilidad_compra) * 100, 1) AS prob_compra_promedio_pct,
       ROUND(SUM(valor_agregado_carrito), 2)    AS valor_carrito_usd
FROM workspace.ecommerce.gold_probabilidad_compra
GROUP BY grupo_riesgo
ORDER BY prob_compra_promedio_pct;

-- Dataset 9: calidad_datos  (tabla: evidencia de la capa Silver)
SELECT fecha_ejecucion, filas_bronze, filas_rechazadas, duplicados_eliminados, filas_silver,
       pct_sin_marca, pct_sin_categoria
FROM workspace.ecommerce.log_calidad_datos
ORDER BY fecha_ejecucion DESC;
