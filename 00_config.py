# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuración del proyecto
# MAGIC
# MAGIC Este notebook **no se ejecuta solo**: los demás notebooks lo llaman con `%run ./00_config`
# MAGIC para compartir los mismos nombres de catálogo, esquema, volúmenes y tablas.
# MAGIC
# MAGIC | Elemento | Valor |
# MAGIC |---|---|
# MAGIC | Catálogo | `workspace` (el que trae Databricks Free Edition) |
# MAGIC | Esquema | `ecommerce` |
# MAGIC | Volumen de archivos crudos | `/Volumes/workspace/ecommerce/raw` → aquí se suben los CSV de Kaggle |
# MAGIC | Volumen de checkpoints | `/Volumes/workspace/ecommerce/checkpoints` → lo usa Auto Loader |

# COMMAND ----------

# ---------------------------------------------------------------
# Nombres generales (si cambias algo aquí, cambia en todo el proyecto)
# ---------------------------------------------------------------
CATALOGO = "workspace"
ESQUEMA = "ecommerce"

VOLUMEN_RAW = "raw"
VOLUMEN_CHECKPOINTS = "checkpoints"

RUTA_ORIGEN = f"/Volumes/{CATALOGO}/{ESQUEMA}/{VOLUMEN_RAW}"            # CSV de Kaggle
RUTA_CHECKPOINTS = f"/Volumes/{CATALOGO}/{ESQUEMA}/{VOLUMEN_CHECKPOINTS}"


def tabla(nombre: str) -> str:
    """Devuelve el nombre completo de una tabla: catalogo.esquema.nombre"""
    return f"{CATALOGO}.{ESQUEMA}.{nombre}"


# Tipos de evento válidos según la documentación del dataset en Kaggle
EVENTOS_VALIDOS = ["view", "cart", "remove_from_cart", "purchase"]

print(f"Configuración cargada → esquema {CATALOGO}.{ESQUEMA} | origen {RUTA_ORIGEN}")
