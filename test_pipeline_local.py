"""
Prueba local del pipeline (Silver → Gold → Modelo → Beneficio/Costo) con datos sintéticos.

Se ejecuta en GitHub Actions (CI) y en cualquier computador con Java + PySpark:
    pip install -r requirements-dev.txt
    pytest -q

Cómo funciona: lee cada notebook de Databricks (.py), ejecuta sus celdas de código con una
SparkSession local y reemplaza `%run ./00_config` por una configuración local.
La capa Bronze (Auto Loader) solo existe en Databricks, por eso aquí se simula su tabla de salida.
"""
import os
import random
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import pytest
from pyspark.sql import SparkSession

# Funciona con los notebooks en la carpeta "notebooks/" o en la raíz del repositorio
_AQUI = Path(__file__).resolve().parent
NOTEBOOKS = next(c for c in [_AQUI, _AQUI / "notebooks", _AQUI.parent / "notebooks", _AQUI.parent]
                 if (c / "03_silver_limpieza.py").exists())
BD = "ecommerce_test"


# ---------------------------------------------------------------- utilidades
class _DbutilsFalso:
    class fs:
        @staticmethod
        def ls(_):
            return []


def _config_local(spark):
    return {
        "spark": spark,
        "dbutils": _DbutilsFalso(),
        "display": lambda *a, **k: None,
        "CATALOGO": "local",
        "ESQUEMA": BD,
        "RUTA_ORIGEN": "/tmp/no_usado",
        "RUTA_CHECKPOINTS": "/tmp/no_usado",
        "tabla": lambda n: f"{BD}.{n}",
        "EVENTOS_VALIDOS": ["view", "cart", "remove_from_cart", "purchase"],
    }


def ejecutar_notebook(nombre, contexto):
    """Ejecuta las celdas de código de un notebook Databricks en formato .py"""
    texto = (NOTEBOOKS / nombre).read_text(encoding="utf-8")
    celdas = texto.split("# COMMAND ----------")
    for celda in celdas:
        lineas = [l for l in celda.strip().splitlines() if l.strip()]
        if not lineas or lineas == ["# Databricks notebook source"]:
            continue
        if all(l.startswith("# MAGIC") or l.startswith("# Databricks") for l in lineas):
            continue  # celda markdown o %run (la config ya está en el contexto)
        exec(compile(celda, nombre, "exec"), contexto)


def _datos_sinteticos(n_sesiones=2500, semilla=7):
    """Genera eventos con el MISMO esquema del CSV de Kaggle (todo como texto, como en Bronze)."""
    rnd = random.Random(semilla)
    marcas = ["runail", "irisk", "masura", "grattol", "estel", "", None]
    categorias = ["appliances.environment.vacuum", "furniture.bathroom.bath", "stationery.cartrige", "", None]
    filas = []
    inicio = datetime(2019, 10, 1)
    for s in range(n_sesiones):
        usuario = str(500000000 + rnd.randint(0, 900))
        sesion = f"sesion-{s}"
        t = inicio + timedelta(minutes=rnd.randint(0, 60 * 24 * 120))
        quiere_comprar = rnd.random() < 0.35
        productos = [str(5000000 + rnd.randint(0, 300)) for _ in range(rnd.randint(1, 6))]
        for p in productos:
            precio = f"{rnd.uniform(0.5, 40):.2f}"
            marca, cat = rnd.choice(marcas), rnd.choice(categorias)
            base = [p, "1487580005134238553", cat, marca, precio, usuario, sesion]
            for _ in range(rnd.randint(1, 4)):
                t += timedelta(seconds=rnd.randint(5, 300))
                filas.append([t.strftime("%Y-%m-%d %H:%M:%S UTC"), "view"] + base)
            if rnd.random() < 0.5:
                t += timedelta(seconds=rnd.randint(5, 300))
                filas.append([t.strftime("%Y-%m-%d %H:%M:%S UTC"), "cart"] + base)
                if quiere_comprar:
                    t += timedelta(seconds=rnd.randint(5, 300))
                    filas.append([t.strftime("%Y-%m-%d %H:%M:%S UTC"), "purchase"] + base)
                elif rnd.random() < 0.3:
                    filas.append([t.strftime("%Y-%m-%d %H:%M:%S UTC"), "remove_from_cart"] + base)
    # Filas sucias para probar las reglas de calidad
    filas += [
        ["no-es-fecha", "view", "5000001", "1", None, None, "3.0", "500000001", "x"],
        ["2019-10-05 10:00:00 UTC", "click", "5000001", "1", None, None, "3.0", "500000001", "x"],
        ["2019-10-05 10:00:00 UTC", "view", None, "1", None, None, "3.0", "500000001", "x"],
        ["2019-10-05 10:00:00 UTC", "purchase", "5000001", "1", None, None, "-7.94", "500000001", "x"],
        ["2019-10-05 10:00:00 UTC", "view", "5000001", "1", None, None, "abc", "500000001", "x"],
    ]
    filas += filas[:200]  # duplicados exactos
    return filas


# ---------------------------------------------------------------- fixtures
@pytest.fixture(scope="module")
def contexto():
    tmp = tempfile.mkdtemp()
    spark = (SparkSession.builder.master("local[2]").appName("prueba_pipeline")
             .config("spark.sql.warehouse.dir", os.path.join(tmp, "warehouse"))
             .config("spark.sql.ansi.enabled", "true")      # igual que Databricks serverless
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {BD}")

    os.environ["MLFLOW_ALLOW_FILE_STORE"] = "true"
    import mlflow
    mlflow.set_tracking_uri("file://" + os.path.join(tmp, "mlruns"))

    columnas = ["event_time", "event_type", "product_id", "category_id", "category_code",
                "brand", "price", "user_id", "user_session"]
    bronze = spark.createDataFrame(_datos_sinteticos(), ", ".join(f"{c} STRING" for c in columnas))
    from pyspark.sql import functions as F
    (bronze.withColumn("_rescued_data", F.lit(None).cast("string"))
     .withColumn("_archivo_origen", F.lit("2019-Oct.csv"))
     .withColumn("_fecha_ingesta", F.current_timestamp())
     .write.mode("overwrite").saveAsTable(f"{BD}.bronze_eventos"))

    ctx = _config_local(spark)
    yield ctx
    spark.stop()


# ---------------------------------------------------------------- pruebas
def test_silver(contexto):
    ejecutar_notebook("03_silver_limpieza.py", contexto)
    spark = contexto["spark"]
    rech = {r["motivo_rechazo"]: r["count"] for r in
            spark.table(f"{BD}.silver_eventos_rechazados").groupBy("motivo_rechazo").count().collect()}
    assert rech == {"fecha_invalida": 1, "tipo_evento_invalido": 1, "producto_vacio": 1,
                    "precio_negativo": 1, "precio_invalido": 1}
    silver = spark.table(f"{BD}.silver_eventos")
    assert silver.count() == silver.dropDuplicates(
        ["fecha_evento", "tipo_evento", "producto_id", "usuario_id", "sesion_id", "precio"]).count()
    assert silver.filter("marca IS NULL OR categoria_codigo IS NULL").count() == 0
    log = spark.table(f"{BD}.log_calidad_datos").first()
    assert log["duplicados_eliminados"] > 0


def test_gold(contexto):
    ejecutar_notebook("04_gold_agregados.py", contexto)
    spark = contexto["spark"]
    kpi = spark.table(f"{BD}.gold_kpis_generales").first()
    assert kpi["ingresos_usd"] > 0 and kpi["usuarios_unicos"] > 0
    ab = spark.table(f"{BD}.gold_carritos_abandonados_mensual").toPandas()
    assert (ab["sesiones_con_carrito"] == ab["sesiones_con_compra"] + ab["sesiones_abandonadas"]).all()
    assert spark.table(f"{BD}.gold_clientes_rfm").count() > 0
    assert set(spark.table(f"{BD}.gold_sesiones").toPandas()["compro"].unique()) <= {0, 1}


def test_modelo(contexto):
    ejecutar_notebook("05_modelo_ciencia_datos.py", contexto)
    assert 0.5 <= contexto["metricas"]["auc_roc"] <= 1.0
    assert contexto["spark"].table(f"{BD}.gold_probabilidad_compra").count() > 0


def test_beneficio_costo(contexto):
    ejecutar_notebook("06_beneficio_costo.py", contexto)
    assert contexto["spark"].table(f"{BD}.gold_beneficio_costo").count() == 3
