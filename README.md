# SpellFJVA

```python
# ============================================================
# BLOQUE 0: Instalación de dependencias en Databricks
# Ejecutar UNA SOLA VEZ por cluster (o al reiniciarlo)
# ============================================================

# Playwright requiere instalar el paquete Y descargar el binario de Chromium
%pip install playwright

import subprocess
# Instala los binarios de browsers (solo Chromium para minimizar tamaño)
result = subprocess.run(
    ["python", "-m", "playwright", "install", "chromium"],
    capture_output=True, text=True
)
print(result.stdout)
print(result.stderr)

# También instalar las dependencias del sistema que necesita Chromium en Linux
result2 = subprocess.run(
    ["python", "-m", "playwright", "install-deps", "chromium"],
    capture_output=True, text=True
)
print(result2.stdout)
print(result2.stderr)

# ============================================================
# BLOQUE 1: Imports y parámetros de configuración
# ============================================================
import os
import time
import logging
import datetime
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def log_block(title):
    logging.info("=" * 80)
    logging.info(title)
    logging.info("=" * 80)

# ---------- Parámetros ----------
URL = "https://pad.minem.gob.pe/REINFO_WEB/Index.aspx"

# Ruta exacta en Databricks
OUTPUT_DIR = "/Workspace/Users/fabrizio.vasquez.a@mibanco.com.pe/Miscelania/Scripts/REINFO"

# Control de scraping
MAX_RETRIES   = 3
CHUNK_PAGES   = 100   # Guardar CSV cada N páginas
WAIT_SECONDS  = 30    # Timeout para selectores (ms → lo pasamos como ms a Playwright)
TIMEOUT_MS    = 30_000  # 30 segundos en milisegundos (unidad de Playwright)
PAGE_LOAD_TIMEOUT_MS = 60_000

# Nombre base de archivos
FILE_PREFIX = "reinfo_data"

# Crear carpeta si no existe
os.makedirs(OUTPUT_DIR, exist_ok=True)
log_block("Configuración cargada")
logging.info(f"OUTPUT_DIR: {OUTPUT_DIR}")

# ============================================================
# BLOQUE 2: Funciones auxiliares de extracción y guardado
# ============================================================

def guardar_csv(data: list, file_count: int) -> str:
    output_file = os.path.join(OUTPUT_DIR, f"{FILE_PREFIX}_{file_count}.csv")
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False, header=False)
    logging.info(f"Guardadas {len(data)} filas en {output_file}")
    return output_file


async def extraer_tabla(page) -> list:
    """Extrae filas visibles de tabla#stdregistro en la página actual."""
    rows = await page.query_selector_all("table#stdregistro tbody tr")
    table_data = []
    for row in rows:
        cols = await row.query_selector_all("td")
        cells = [await col.inner_text() for col in cols]
        table_data.append([c.strip() for c in cells])
    return table_data


async def ir_siguiente_pagina(page) -> bool:
    """
    Intenta hacer click en el botón siguiente página.
    Retorna True si tuvo éxito, False si no hay más páginas o agotó reintentos.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            next_btn = await page.query_selector(
                "input#ImgBtnSiguiente, img#ImgBtnSiguiente"
            )
            if next_btn is None:
                logging.info("Botón 'Siguiente' no encontrado. Fin de paginación.")
                return False

            is_disabled = await next_btn.get_attribute("disabled")
            if is_disabled is not None:
                logging.info("Botón 'Siguiente' deshabilitado. Última página alcanzada.")
                return False

            await next_btn.click()
            await page.wait_for_selector(
                "table#stdregistro tbody tr",
                timeout=TIMEOUT_MS
            )
            await asyncio.sleep(2)
            return True

        except PlaywrightTimeoutError:
            logging.warning(f"Timeout navegando a siguiente página. Intento {attempt}/{MAX_RETRIES}")
            await asyncio.sleep(5)
        except Exception as e:
            logging.warning(f"Error al ir a siguiente página: {e}. Intento {attempt}/{MAX_RETRIES}")
            await asyncio.sleep(5)

    logging.error("No se pudo continuar a la siguiente página tras reintentos.")
    return False
# ============================================================
# BLOQUE 3: Scraping principal con Playwright
# ============================================================
def guardar_csv(data: list, file_count: int) -> str:
    output_file = os.path.join(OUTPUT_DIR, f"{FILE_PREFIX}_{file_count}.csv")
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False, header=False)
    logging.info(f"Guardadas {len(data)} filas en {output_file}")
    return output_file


async def extraer_tabla(page) -> list:
    """Extrae filas visibles de tabla#stdregistro en la página actual."""
    rows = await page.query_selector_all("table#stdregistro tbody tr")
    table_data = []
    for row in rows:
        cols = await row.query_selector_all("td")
        cells = [await col.inner_text() for col in cols]
        table_data.append([c.strip() for c in cells])
    return table_data


async def ir_siguiente_pagina(page) -> bool:
    """
    Intenta hacer click en el botón siguiente página.
    Retorna True si tuvo éxito, False si no hay más páginas o agotó reintentos.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            next_btn = await page.query_selector(
                "input#ImgBtnSiguiente, img#ImgBtnSiguiente"
            )
            if next_btn is None:
                logging.info("Botón 'Siguiente' no encontrado. Fin de paginación.")
                return False

            is_disabled = await next_btn.get_attribute("disabled")
            if is_disabled is not None:
                logging.info("Botón 'Siguiente' deshabilitado. Última página alcanzada.")
                return False

            await next_btn.click()
            await page.wait_for_selector(
                "table#stdregistro tbody tr",
                timeout=TIMEOUT_MS
            )
            await asyncio.sleep(2)
            return True

        except PlaywrightTimeoutError:
            logging.warning(f"Timeout navegando a siguiente página. Intento {attempt}/{MAX_RETRIES}")
            await asyncio.sleep(5)
        except Exception as e:
            logging.warning(f"Error al ir a siguiente página: {e}. Intento {attempt}/{MAX_RETRIES}")
            await asyncio.sleep(5)

    logging.error("No se pudo continuar a la siguiente página tras reintentos.")
    return False

# ============================================================
# BLOQUE 4: Verificación rápida de archivos generados
# ============================================================
import glob

archivos = sorted(glob.glob(os.path.join(OUTPUT_DIR, f"{FILE_PREFIX}_*.csv")))
logging.info(f"Archivos generados: {len(archivos)}")

for f in archivos:
    df_check = pd.read_csv(f, header=None)
    logging.info(f"  {os.path.basename(f)}: {df_check.shape[0]} filas × {df_check.shape[1]} columnas")

# Mostrar muestra del último archivo
if archivos:
    df_last = pd.read_csv(archivos[-1], header=None)
    display(df_last.head(10))
```
