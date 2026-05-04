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
    """Guarda lista de filas en CSV y retorna la ruta."""
    output_file = os.path.join(OUTPUT_DIR, f"{FILE_PREFIX}_{file_count}.csv")
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False, header=False)
    logging.info(f"Guardadas {len(data)} filas en {output_file}")
    return output_file


def extraer_tabla(page) -> list:
    """
    Extrae todas las filas visibles de la tabla stdregistro en la página actual.
    Retorna lista de listas (cada fila = lista de celdas).
    """
    rows = page.query_selector_all("table#stdregistro tbody tr")
    table_data = []
    for row in rows:
        cols = row.query_selector_all("td")
        table_data.append([col.inner_text().strip() for col in cols])
    return table_data


def ir_siguiente_pagina(page, retry_count: int = 0) -> bool:
    """
    Intenta hacer click en el botón 'siguiente página'.
    Retorna True si tuvo éxito, False si no hay más páginas o falló tras reintentos.
    """
    while retry_count < MAX_RETRIES:
        try:
            next_btn = page.query_selector("input#ImgBtnSiguiente, img#ImgBtnSiguiente")
            if next_btn is None:
                logging.info("Botón 'Siguiente' no encontrado. Fin de paginación.")
                return False
            
            # Verificar si está deshabilitado (algunos sitios usan disabled o display:none)
            is_disabled = next_btn.get_attribute("disabled")
            if is_disabled is not None:
                logging.info("Botón 'Siguiente' deshabilitado. Última página alcanzada.")
                return False

            next_btn.click()
            # Esperar que la tabla se recargue
            page.wait_for_selector("table#stdregistro tbody tr", timeout=TIMEOUT_MS)
            time.sleep(2)  # Buffer adicional para renderizado completo
            return True

        except PlaywrightTimeoutError:
            retry_count += 1
            logging.warning(f"Timeout al intentar ir a siguiente página. Reintento {retry_count}/{MAX_RETRIES}")
            time.sleep(5)
        except Exception as e:
            retry_count += 1
            logging.warning(f"Error al ir a siguiente página: {e}. Reintento {retry_count}/{MAX_RETRIES}")
            time.sleep(5)

    logging.error("No se pudo continuar a la siguiente página tras reintentos.")
    return False

# ============================================================
# BLOQUE 3: Scraping principal con Playwright
# ============================================================

log_block("Iniciando extracción con Playwright")

data        = []
table_count = 0
file_count  = 1

with sync_playwright() as p:
    # --- Lanzar Chromium headless (obligatorio en Databricks) ---
    browser = p.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",          # Requerido en entornos sin root
            "--disable-dev-shm-usage",  # Evita crashes por memoria compartida limitada
            "--disable-gpu",         # Databricks no tiene GPU para renderizado
        ]
    )
    context = browser.new_context()
    page = context.new_page()
    page.set_default_timeout(TIMEOUT_MS)

    try:
        # --- Navegar a la URL ---
        log_block("Navegando a la URL")
        page.goto(URL, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT_MS)
        logging.info(f"Página cargada: {URL}")

        # --- Seleccionar "Listados Todas" en dropdown ---
        log_block("Configurando filtros")
        try:
            page.wait_for_selector("#ddllistado", timeout=TIMEOUT_MS)
            page.select_option("#ddllistado", label="Listados Todas")
            logging.info("Dropdown 'ddllistado' → 'Listados Todas' seleccionado")
        except PlaywrightTimeoutError:
            logging.error("Timeout: El dropdown #ddllistado no está disponible.")
            raise

        # --- Hacer clic en botón Buscar ---
        try:
            page.wait_for_selector("#btnBuscar", timeout=TIMEOUT_MS)
            page.click("#btnBuscar")
            logging.info("Click en botón #btnBuscar")
        except Exception as e:
            logging.error(f"Error al hacer click en botón de búsqueda: {e}")
            raise

        # --- Esperar que cargue la tabla ---
        try:
            page.wait_for_selector("table#stdregistro", timeout=PAGE_LOAD_TIMEOUT_MS)
            logging.info("Tabla #stdregistro detectada. Iniciando extracción...")
        except PlaywrightTimeoutError:
            logging.error("Timeout: La tabla no se cargó a tiempo.")
            raise

        # --- Loop de extracción por páginas ---
        log_block("Iniciando extracción de datos...")

        while True:
            try:
                table_data = extraer_tabla(page)

                if table_data:
                    data.extend(table_data)
                    table_count += 1
                    logging.info(f"Tabla {table_count} procesada — {len(table_data)} filas extraídas")
                else:
                    logging.warning(f"Página {table_count + 1}: tabla vacía, se omite.")

                # Guardar cada CHUNK_PAGES páginas
                if table_count >= CHUNK_PAGES:
                    guardar_csv(data, file_count)
                    data.clear()
                    table_count = 0
                    file_count += 1

                # Intentar ir a la siguiente página
                hay_siguiente = ir_siguiente_pagina(page)
                if not hay_siguiente:
                    break

            except Exception as e:
                logging.error(f"Error durante la extracción: {e}")
                break

    finally:
        # --- Guardar datos restantes ---
        if data:
            guardar_csv(data, file_count)

        # --- Cerrar navegador ---
        browser.close()
        log_block("Extracción finalizada")
        logging.info(f"Datos guardados en: {OUTPUT_DIR}")

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
