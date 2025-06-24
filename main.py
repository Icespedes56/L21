from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import shutil, os, zipfile
from pathlib import Path
import chardet
import pandas as pd
import uuid
import tempfile
from datetime import datetime, timedelta
from typing import List, Optional
import re

# ============================================================================
# IMPORTACIÓN DEL MÓDULO CONTROL APORTANTES
# ============================================================================
from control_aportantes_processor import (
    procesar_excel_aportantes,
    obtener_nits_unicos,
    obtener_detalle_por_nit,
    obtener_filtros_geograficos,
    obtener_municipios_por_departamento,
    filtrar_nits_por_geografia,
    obtener_estadisticas_geograficas,
    aportantes_sessions
)

app = FastAPI(title="Backend Planillas", description="API para procesamiento de planillas y cruce de LOG")

# --- Configuración CORS ---
origins = [
    "http://localhost",
    "http://localhost:3000", # React Create React App
    "http://localhost:5173", # Vite
    "http://127.0.0.1:5173", # Vite alternativo
    "http://127.0.0.1:3000", # React alternativo
    # Agrega más orígenes si tu frontend se va a desplegar en otros dominios.
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"], # Permite todos los métodos (POST, GET, etc.)
    allow_headers=["*"], # Permite todas las cabeceras
)
# --- Fin Configuración CORS ---

BASE_PATH = Path("procesos")
BASE_PATH.mkdir(exist_ok=True)

# Diccionario de correcciones más específico para caracteres problemáticos
correcciones = {
    "ï¿½": "Ñ", "Ã'": "Ñ", "Ã¡": "á", "Ã©": "é", "Ã­": "í",
    "Ã³": "ó", "Ãº": "ú", "Ã‰": "É", "Ã": "Ó", "Ãš": "Ú", "Ã¼": "ü",
    "Ã ": "Ñ", # Este es común cuando un 'Ñ' codificado en ISO-8859-1 es leído como UTF-8
    "´": "'",
    "`": "'"
}

def limpiar_zeros(valor):
    """Elimina ceros iniciales de una cadena si es una cadena y no vacía."""
    if isinstance(valor, str) and valor:
        return valor.lstrip("0")
    return valor

# Función auxiliar para extraer subcadenas de forma segura
def get_substring_safe(text, start, end):
    """
    Extrae una subcadena de forma segura. Si el rango excede la longitud del texto,
    devuelve una porción válida o una cadena vacía.
    """
    if not isinstance(text, str):
        return ""
    if start >= len(text):
        return ""
    if end > len(text):
        return text[start:]
    return text[start:end]

# ============================================================================
# CLASE PARA PROCESAMIENTO DE CRUCE LOG
# ============================================================================

class CruceLogProcessor:
    def __init__(self, ruta_log, ruta_txt, ruta_salida_zip, meses_referencia=2):
        self.ruta_log = ruta_log
        self.ruta_txt = ruta_txt
        self.ruta_salida_zip = ruta_salida_zip
        self.meses_referencia = meses_referencia
        
    def leer_archivos_tipo_I(self):
        """
        Lee los archivos tipo I y extrae el número clave del nombre del archivo
        junto con los datos de capital e interés
        """
        datos = {}
        archivos_procesados = 0
        archivos_con_error = []
        
        print(f"[DEBUG] Buscando archivos en: {self.ruta_txt}")
        
        if not os.path.exists(self.ruta_txt):
            print(f"[ERROR] El directorio no existe: {self.ruta_txt}")
            return datos, archivos_procesados, archivos_con_error
        
        # Buscar archivos tanto en el directorio principal como en subdirectorios
        archivos_encontrados = []
        
        for root, dirs, files in os.walk(self.ruta_txt):
            for archivo in files:
                if archivo.endswith((".TXT", ".txt")) and "_I_" in archivo:
                    archivos_encontrados.append(os.path.join(root, archivo))
        
        print(f"[DEBUG] Archivos tipo I encontrados: {len(archivos_encontrados)}")
        
        for ruta_archivo in archivos_encontrados:
            archivo = os.path.basename(ruta_archivo)
            archivos_procesados += 1
            print(f"[DEBUG] Procesando archivo #{archivos_procesados}: {archivo}")
            
            try:
                # Extraer el número clave del nombre del archivo
                patron = r'_(\d+)_(\d+)_'
                match = re.search(patron, archivo)
                if match:
                    numero_clave = match.group(2)
                    print(f"[DEBUG] Número clave extraído: {numero_clave}")
                    
                    with open(ruta_archivo, 'r', encoding='latin-1') as f:
                        lineas = f.readlines()
                        
                        if len(lineas) >= 4:
                            linea2 = lineas[1].strip()
                            linea3 = lineas[2].strip()
                            
                            print(f"[DEBUG] Línea 2: {linea2}")
                            print(f"[DEBUG] Línea 3: {linea3}")
                            
                            # Extraer capital de la línea 2
                            valor_capital = 0
                            if linea2.isdigit() and len(linea2) >= 5:
                                valor_capital = int(linea2[-10:]) if len(linea2) >= 10 else int(linea2[-5:])
                            
                            # Extraer interés de la línea 3
                            valor_interes = 0
                            if linea3.isdigit() and len(linea3) >= 3:
                                temp_interes = int(linea3)
                                if temp_interes > 0:
                                    valor_interes = temp_interes
                            
                            datos[numero_clave] = {
                                "capital": valor_capital,
                                "interes": valor_interes,
                                "archivo": archivo
                            }
                            
                            print(f"[DEBUG] Datos extraídos - Capital: {valor_capital}, Interés: {valor_interes}")
                        else:
                            archivos_con_error.append(f"{archivo} (pocas líneas: {len(lineas)})")
                            print(f"[ERROR] Archivo {archivo} tiene solo {len(lineas)} líneas")
                else:
                    archivos_con_error.append(f"{archivo} (patrón no encontrado)")
                    print(f"[ERROR] No se pudo extraer número clave de {archivo}")
                            
            except Exception as e:
                archivos_con_error.append(f"{archivo} (error: {str(e)})")
                print(f"[ERROR] No se pudo procesar el archivo {archivo}: {e}")
        
        print(f"[DEBUG] Archivos procesados exitosamente: {len(datos)}")
        print(f"[DEBUG] Archivos con errores: {len(archivos_con_error)}")
        
        return datos, archivos_procesados, archivos_con_error

    def modificar_linea_para_capital(self, linea, valor_capital):
        """Modifica la línea del LOG para reemplazar el valor de capital"""
        if len(linea) >= 88:
            linea_lista = list(linea)
            nuevo_valor_str = str(valor_capital)
            longitud_total = 15
            
            if len(nuevo_valor_str) <= longitud_total:
                nuevo_segmento = nuevo_valor_str.zfill(longitud_total)
            else:
                nuevo_segmento = nuevo_valor_str[-longitud_total:]
            
            for i, caracter in enumerate(nuevo_segmento):
                if 73 + i < len(linea_lista):
                    linea_lista[73 + i] = caracter
            
            return ''.join(linea_lista)
        
        return linea
    
    def modificar_linea_para_interes(self, linea, valor_interes):
        """Modifica la línea del LOG para reemplazar el valor de interés"""
        if len(linea) >= 88:
            linea_lista = list(linea)
            nuevo_valor_str = str(valor_interes)
            longitud_total = 15
            
            if len(nuevo_valor_str) <= longitud_total:
                nuevo_segmento = nuevo_valor_str.zfill(longitud_total)
            else:
                nuevo_segmento = nuevo_valor_str[-longitud_total:]
            
            for i, caracter in enumerate(nuevo_segmento):
                if 73 + i < len(linea_lista):
                    linea_lista[73 + i] = caracter
            
            return ''.join(linea_lista)
        
        return linea

    def procesar_archivos(self):
        """Procesa el cruce entre el archivo LOG y los archivos tipo I"""
        datos_i, archivos_procesados, archivos_con_error = self.leer_archivos_tipo_I()
        print(f"\n[DEBUG] Se leyeron {len(datos_i)} registros del tipo I")
        
        capital_actual, capital_anterior = [], []
        interes_actual, interes_anterior = [], []
        errores = []
        
        matches_encontrados = 0
        lineas_sin_match = []

        if not os.path.exists(self.ruta_log):
            print(f"[ERROR] El archivo LOG no existe: {self.ruta_log}")
            return {
                "Capital_Actual.txt": [],
                "Capital_Anterior.txt": [],
                "Interes_Actual.txt": [],
                "Interes_Anterior.txt": [],
                "Errores.txt": [],
                "estadisticas": {
                    "matches_encontrados": 0,
                    "capital_actual": 0,
                    "capital_anterior": 0,
                    "interes_actual": 0,
                    "interes_anterior": 0,
                    "total_archivos_i": archivos_procesados,
                    "errores": len(archivos_con_error)
                }
            }

        # Calcular fecha de referencia según los meses especificados
        fecha_hoy = datetime.now()
        fecha_referencia = fecha_hoy - timedelta(days=self.meses_referencia * 30)
        print(f"[DEBUG] Fecha de referencia ({self.meses_referencia} meses atrás): {fecha_referencia.strftime('%Y-%m-%d')}")

        with open(self.ruta_log, 'r', encoding='latin-1') as f:
            todas_las_lineas = f.readlines()
            lineas_log = todas_las_lineas[2:]  # Saltar las primeras 2 líneas
            print(f"\n[DEBUG] Se leyeron {len(lineas_log)} líneas del archivo LOG")

            for i, linea in enumerate(lineas_log, start=3):
                if len(linea) >= 52:
                    numero_en_log = linea[41:51].strip()
                    
                    if numero_en_log in datos_i:
                        matches_encontrados += 1
                        datos = datos_i[numero_en_log]
                        
                        print(f"[MATCH #{matches_encontrados}] Línea {i}: Número {numero_en_log}")
                        
                        # Extraer fecha de la línea del LOG
                        try:
                            if len(linea) >= 65:
                                fecha_str = linea[56:64]
                                fecha_log = datetime.strptime(fecha_str, "%Y%m%d")
                                es_actual = fecha_log >= fecha_referencia
                                
                                periodo = "Actual" if es_actual else "Anterior"
                                print(f"  -> Fecha: {fecha_log.strftime('%Y-%m-%d')}, Período: {periodo}")
                                
                                # Clasificar según las condiciones
                                if datos["capital"] > 0:
                                    linea_capital = self.modificar_linea_para_capital(linea, datos["capital"])
                                    if es_actual:
                                        capital_actual.append(linea_capital)
                                    else:
                                        capital_anterior.append(linea_capital)
                                
                                if datos["interes"] > 0:
                                    linea_interes = self.modificar_linea_para_interes(linea, datos["interes"])
                                    if es_actual:
                                        interes_actual.append(linea_interes)
                                    else:
                                        interes_anterior.append(linea_interes)
                                
                                if datos["capital"] == 0 and datos["interes"] == 0:
                                    errores.append(f"Línea {i}: {numero_en_log} - Capital y interés son 0\n")
                            else:
                                errores.append(f"Línea {i}: {numero_en_log} - Línea muy corta para extraer fecha\n")
                                
                        except ValueError as e:
                            errores.append(f"Línea {i}: {numero_en_log} - Error en fecha: {str(e)}\n")
                    else:
                        if len(lineas_sin_match) < 10:
                            lineas_sin_match.append(f"Línea {i}: '{numero_en_log}' no encontrado")

        # Agregar líneas sin match a errores
        if lineas_sin_match:
            errores.append("\n=== NÚMEROS NO ENCONTRADOS EN ARCHIVOS TIPO I ===\n")
            for error in lineas_sin_match:
                errores.append(f"{error}\n")

        # Agregar errores de archivos tipo I
        if archivos_con_error:
            errores.append("\n=== ERRORES EN ARCHIVOS TIPO I ===\n")
            for error in archivos_con_error:
                errores.append(f"{error}\n")

        estadisticas = {
            "matches_encontrados": matches_encontrados,
            "capital_actual": len(capital_actual),
            "capital_anterior": len(capital_anterior),
            "interes_actual": len(interes_actual),
            "interes_anterior": len(interes_anterior),
            "total_archivos_i": archivos_procesados,
            "errores": len(errores)
        }

        print(f"\n[RESUMEN FINAL]")
        print(f"Matches encontrados: {matches_encontrados}")
        print(f"Capital Actual: {len(capital_actual)}")
        print(f"Capital Anterior: {len(capital_anterior)}")
        print(f"Interés Actual: {len(interes_actual)}")
        print(f"Interés Anterior: {len(interes_anterior)}")

        return {
            "Capital_Actual.txt": capital_actual,
            "Capital_Anterior.txt": capital_anterior,
            "Interes_Actual.txt": interes_actual,
            "Interes_Anterior.txt": interes_anterior,
            "Errores.txt": errores,
            "estadisticas": estadisticas
        }

    def guardar_y_comprimir_archivos(self, resultados):
        """Guarda los resultados en archivos TXT y los comprime en un ZIP"""
        ruta_temp = os.path.dirname(self.ruta_salida_zip)
        archivos_txt = []

        for nombre_archivo, contenido in resultados.items():
            if nombre_archivo == "estadisticas":
                continue
                
            if contenido:
                ruta_completa = os.path.join(ruta_temp, nombre_archivo)
                
                with open(ruta_completa, 'w', encoding='latin-1') as f:
                    if nombre_archivo == "Errores.txt":
                        f.writelines(contenido)
                    else:
                        f.writelines(contenido)
                
                archivos_txt.append(ruta_completa)
                print(f"[ARCHIVO CREADO] {nombre_archivo} con {len(contenido)} líneas")

        if archivos_txt:
            with zipfile.ZipFile(self.ruta_salida_zip, 'w') as zipf:
                for archivo in archivos_txt:
                    zipf.write(archivo, os.path.basename(archivo))
            
            # Limpiar archivos temporales
            for archivo in archivos_txt:
                try:
                    os.remove(archivo)
                except:
                    pass
            
            print(f"\n✅ Archivo ZIP generado: {self.ruta_salida_zip}")
        else:
            print("\n⚠️ No se generó ZIP porque no hay archivos para incluir")

        return archivos_txt

# ============================================================================
# ENDPOINTS PRINCIPALES
# ============================================================================

@app.get("/")
async def root():
    """Endpoint raíz para verificar que la API está funcionando"""
    return {
        "message": "Backend Planillas API",
        "version": "1.0.0",
        "status": "running",
        "modulos": ["procesamiento", "cruce-log", "control-aportantes"]
    }

@app.get("/health")
async def health_check():
    """Endpoint de salud para monitoreo"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

# ============================================================================
# ENDPOINTS DEL MÓDULO CONTROL APORTANTES (COMPLETOS)
# ============================================================================

@app.post("/upload_aportantes")
async def upload_aportantes(file: UploadFile = File(...)):
    """Sube un archivo Excel de aportantes y retorna un session_id"""
    try:
        session_id = procesar_excel_aportantes(file.file)
        return {"session_id": session_id}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.get("/aportantes_nits/{session_id}")
def get_nits(session_id: str):
    """Obtiene la lista de NITs únicos de una sesión"""
    try:
        nits = obtener_nits_unicos(session_id)
        return {"nits": nits}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.get("/aportantes_detalle/{session_id}/{nit}")
def get_detalle_nit(session_id: str, nit: int):
    """Obtiene el detalle de todas las entidades asociadas a un NIT"""
    try:
        detalle = obtener_detalle_por_nit(session_id, nit)
        return {"detalle": detalle}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.get("/aportantes_filtros/{session_id}")
def get_filtros_geograficos(session_id: str):
    """Obtiene listas únicas de departamentos y municipios para filtros"""
    try:
        filtros = obtener_filtros_geograficos(session_id)
        return {"filtros": filtros}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.get("/aportantes_municipios/{session_id}/{departamento}")
def get_municipios_por_departamento(session_id: str, departamento: str):
    """Obtiene municipios de un departamento específico"""
    try:
        municipios = obtener_municipios_por_departamento(session_id, departamento)
        return {"municipios": municipios}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.get("/aportantes_filtrar/{session_id}")
def filtrar_nits_geografia(session_id: str, departamento: Optional[str] = None, municipio: Optional[str] = None):
    """Filtra NITs por criterios geográficos usando query parameters"""
    try:
        nits = filtrar_nits_por_geografia(session_id, departamento, municipio)
        return {"nits": nits}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.get("/aportantes_estadisticas/{session_id}")
def get_estadisticas_geograficas(session_id: str):
    """Obtiene estadísticas de NITs por departamento y municipio"""
    try:
        estadisticas = obtener_estadisticas_geograficas(session_id)
        return {"estadisticas": estadisticas}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.get("/aportantes_all_data/{session_id}")
def get_all_aportantes_data(session_id: str):
    """Obtiene todos los datos de aportantes para alimentar el mapa interactivo"""
    try:
        df = aportantes_sessions.get(session_id)
        if df is None:
            return JSONResponse(status_code=400, content={"error": "Sesión no encontrada"})
        
        # Convertir DataFrame a lista de diccionarios
        data = df.to_dict(orient="records")
        
        # Información adicional para el mapa
        total_registros = len(data)
        
        # Buscar columnas usando las funciones helper
        from control_aportantes_processor import encontrar_columna_departamento, encontrar_columna_municipio
        
        departamento_col = encontrar_columna_departamento(df)
        municipio_col = encontrar_columna_municipio(df)
        
        departamentos_unicos = df[departamento_col].nunique() if departamento_col else 0
        municipios_unicos = df[municipio_col].nunique() if municipio_col else 0
        
        return {
            "data": data,
            "stats": {
                "total_registros": total_registros,
                "departamentos_unicos": departamentos_unicos,
                "municipios_unicos": municipios_unicos
            }
        }
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

# ============================================================================
# ENDPOINTS DEL MÓDULO DE PROCESAMIENTO (EXISTENTE)
# ============================================================================

@app.post("/info-zip/")
async def obtener_info_zip(archivo: UploadFile = File(...)):
    try:
        # Leer el contenido del archivo ZIP
        contents = await archivo.read()
        
        # Crear un archivo temporal para leer el ZIP
        temp_zip_path = f"/tmp/{archivo.filename}"
        with open(temp_zip_path, "wb") as temp_file:
            temp_file.write(contents)
        
        # Validación de que el archivo es un ZIP
        if not zipfile.is_zipfile(temp_zip_path):
            os.remove(temp_zip_path)
            raise ValueError("El archivo subido no es un archivo ZIP válido.")
        
        # Contar archivos .txt con _I_ y _A_
        archivos_validos = 0
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if (file_info.filename.endswith('.txt') and 
                    ('_I_' in file_info.filename or '_A_' in file_info.filename)):
                    archivos_validos += 1
        
        # Limpiar archivo temporal
        os.remove(temp_zip_path)
        
        return JSONResponse({
            "total_archivos": archivos_validos,
            "peso_archivo": len(contents)
        })
        
    except Exception as e:
        if 'temp_zip_path' in locals() and os.path.exists(temp_zip_path):
            os.remove(temp_zip_path)
        raise HTTPException(status_code=400, detail=f"Error al analizar ZIP: {str(e)}")

@app.post("/procesar/")
async def procesar_archivo(archivo: UploadFile = File(...)):
    proceso_id = uuid.uuid4().hex
    carpeta_proceso = BASE_PATH / proceso_id
    
    try:
        carpeta_proceso.mkdir(parents=True, exist_ok=True) # Crea la carpeta del proceso

        carpeta_i = carpeta_proceso / "Archivos_I"
        carpeta_a = carpeta_proceso / "Archivos_A"
        carpeta_i.mkdir()
        carpeta_a.mkdir()

        # 1. Guardar y descomprimir archivo ZIP
        zip_path = carpeta_proceso / archivo.filename
        with open(zip_path, "wb") as buffer:
            shutil.copyfileobj(archivo.file, buffer)
        
        # Validación de que el archivo es un ZIP
        if not zipfile.is_zipfile(zip_path):
            raise ValueError("El archivo subido no es un archivo ZIP válido.")

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(carpeta_proceso)
        
        # Eliminar el archivo ZIP después de la extracción para limpiar
        os.remove(zip_path)

        # 2. Clasificar archivos en carpetas I y A y limpiar otros
        # Listamos los archivos para evitar problemas si se modifican durante la iteración
        extracted_files = list(carpeta_proceso.glob("*"))
        for extracted_file in extracted_files:
            if extracted_file.is_file():
                if "_I_" in extracted_file.name and extracted_file.suffix.lower() == '.txt':
                    shutil.move(str(extracted_file), carpeta_i / extracted_file.name)
                elif "_A_" in extracted_file.name and extracted_file.suffix.lower() == '.txt':
                    shutil.move(str(extracted_file), carpeta_a / extracted_file.name)
                else:
                    # Eliminar archivos que no son .txt o no tienen el prefijo esperado
                    try:
                        extracted_file.unlink() 
                    except OSError as e:
                        print(f"No se pudo eliminar el archivo {extracted_file.name}: {e}")

        # 3. Corregir codificación y reorganizar líneas
        for archivo_path in carpeta_i.glob("*.txt"):
            try:
                with archivo_path.open("rb") as f:
                    raw_content = f.read()
                    
                # Detectar la codificación
                result = chardet.detect(raw_content)
                encoding_to_use = result['encoding'] if result['encoding'] and result['confidence'] > 0.7 else "latin-1"
                
                # Decodificar el contenido
                decoded_text = raw_content.decode(encoding_to_use, errors="replace")
                
                # Aplicar correcciones de caracteres específicos
                for damaged, corrected in correcciones.items():
                    decoded_text = decoded_text.replace(damaged, corrected)
                
                lineas = decoded_text.splitlines()

                # Reorganizar líneas si hay suficientes
                if len(lineas) >= 7:
                    reorganizadas = [lineas[0], lineas[2], lineas[4], lineas[6]] + lineas[7:]
                    final_text = "\n".join(reorganizadas)
                else:
                    final_text = "\n".join(lineas)

                # Sobrescribir el archivo original con el contenido corregido y reorganizado en UTF-8
                with archivo_path.open("w", encoding="utf-8") as f:
                    f.write(final_text)

            except Exception as e:
                print(f"Error al corregir codificación o reorganizar {archivo_path.name}: {e}")

        # 4. Extraer información y generar Excel
        registros = []
        for archivo_path in carpeta_i.glob("*.txt"):
            try:
                # Leemos el archivo ya corregido y guardado en UTF-8
                with archivo_path.open("r", encoding="utf-8") as f:
                    lineas = f.readlines()
                
                # Asegurarse de que tenemos al menos 4 líneas
                if len(lineas) < 4:
                    print(f"Advertencia: El archivo {archivo_path.name} tiene menos de 4 líneas esperadas después de la reorganización. Saltando.")
                    continue
                
                cabecera = lineas[0].strip()
                aporte = lineas[1].strip()
                mora = lineas[2].strip()
                total = lineas[3].strip()

                registro = {
                    "Archivo": archivo_path.name,
                    "Numero Del Registro": get_substring_safe(cabecera, 5, 6),
                    "Código de Formato": get_substring_safe(cabecera, 6, 7),
                    "Código Formato": get_substring_safe(cabecera, 7, 8),
                    "No. Identificación ESAP": get_substring_safe(cabecera, 8, 17).strip(),
                    "Dígito Verificación": get_substring_safe(cabecera, 24, 25),
                    "Nombre Aportante": get_substring_safe(cabecera, 25, 69).strip(),
                    "Tipo Documento Aportante": get_substring_safe(cabecera, 225, 227).strip(),
                    "No. Identificación Aportante": get_substring_safe(cabecera, 227, 236).strip(),
                    "Dígito Verificación Aportante": get_substring_safe(cabecera, 243, 244),
                    "Tipo de Aportante": limpiar_zeros(get_substring_safe(cabecera, 244, 246)),
                    "Dirección": get_substring_safe(cabecera, 246, 286).strip(),
                    "Código Ciudad": get_substring_safe(cabecera, 286, 289).strip(),
                    "Código Dpto": get_substring_safe(cabecera, 289, 291).strip(),
                    "Teléfono": limpiar_zeros(get_substring_safe(cabecera, 294, 308).strip()),
                    "Correo": get_substring_safe(cabecera, 311, 371).strip(),
                    "Periodo de Pago": get_substring_safe(cabecera, 371, 378).strip(),
                    "Tipo de Planilla": get_substring_safe(cabecera, 378, 379),
                    "Fecha de Pago Planilla": get_substring_safe(cabecera, 379, 389).strip(),
                    "Fecha de Pago": get_substring_safe(cabecera, 389, 399).strip(),
                    "No. Planilla Asociada": get_substring_safe(cabecera, 399, 407).strip(),
                    "Número de Radicación": get_substring_safe(cabecera, 409, 419).strip(),
                    "Forma de Presentación": get_substring_safe(cabecera, 419, 420),
                    "Código Sucursal": get_substring_safe(cabecera, 420, 423).strip(),
                    "Nombre Sucursal": get_substring_safe(cabecera, 430, 465).strip(),
                    "Total Empleados": limpiar_zeros(get_substring_safe(cabecera, 470, 475)),
                    "Total Afiliados": limpiar_zeros(get_substring_safe(cabecera, 475, 480)),
                    "Código Operador": get_substring_safe(cabecera, 480, 482).strip(),
                    "Modalidad Planilla": get_substring_safe(cabecera, 482, 483),
                    "Días Mora": limpiar_zeros(get_substring_safe(cabecera, 483, 488)),
                    "Clase Aportante": get_substring_safe(cabecera, 488, 489),
                    "Naturaleza Jurídica": get_substring_safe(cabecera, 489, 490),
                    "Tipo Persona": get_substring_safe(cabecera, 490, 491),
                    "IBC": limpiar_zeros(get_substring_safe(aporte, 6, 19)),
                    "Aporte Obligatorio": limpiar_zeros(get_substring_safe(aporte, 19, 33)),
                    "Mora Aportes": limpiar_zeros(get_substring_safe(mora, 14, 23)),
                    "Total Aportes": limpiar_zeros(get_substring_safe(total, 6, 20))
                }
                registros.append(registro)
            except Exception as e:
                print(f"Error al extraer datos de {archivo_path.name}: {e}")
                
        df = pd.DataFrame(registros)

        # Convertir datetime con zona horaria a naive para evitar error al guardar en Excel
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.tz_localize(None)

        salida_excel = carpeta_proceso / f"planillas_generadas_{proceso_id}.xlsx"
        df.to_excel(salida_excel, index=False)

        # Retornar el archivo Excel generado
        return FileResponse(
            salida_excel, 
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"planillas_generadas.xlsx"
        )

    except ValueError as ve:
        print(f"Error de validación: {ve}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as ex:
        print(f"Error inesperado en el procesamiento: {ex}")
        raise HTTPException(status_code=500, detail="Error interno del servidor al procesar el archivo.")
    finally:
        # Limpiar la carpeta del proceso después de que todo haya terminado o fallado
        pass

# ============================================================================
# ENDPOINTS DEL MÓDULO CRUCE LOG (EXISTENTE)
# ============================================================================

@app.post("/cruce-log/validar-archivos/")
async def validar_archivos_cruce_log(
    archivo_log: UploadFile = File(...),
    archivos_txt: List[UploadFile] = File(...)
):
    """Valida los archivos antes del procesamiento de cruce LOG"""
    try:
        # Crear directorio temporal
        temp_dir = tempfile.mkdtemp()
        
        # Validar archivo LOG
        validacion_log = {
            "nombre": archivo_log.filename,
            "tamaño": 0,
            "lineas_estimadas": 0,
            "valido": False
        }
        
        # Guardar y analizar archivo LOG
        ruta_log = os.path.join(temp_dir, archivo_log.filename)
        with open(ruta_log, 'wb') as f:
            content = await archivo_log.read()
            f.write(content)
            validacion_log["tamaño"] = len(content)
        
        # Contar líneas del LOG
        try:
            with open(ruta_log, 'r', encoding='latin-1') as f:
                lineas = f.readlines()
                validacion_log["lineas_estimadas"] = len(lineas)
                validacion_log["valido"] = len(lineas) > 2
        except:
            validacion_log["valido"] = False
        
        # Validar archivos TXT/ZIP
        directorio_txt = os.path.join(temp_dir, "archivos_txt")
        os.makedirs(directorio_txt, exist_ok=True)
        
        archivos_validos = []
        archivos_invalidos = []
        total_archivos = len(archivos_txt)
        archivos_tipo_i = 0
        
        for archivo in archivos_txt:
            try:
                if archivo.filename.lower().endswith('.zip'):
                    # Es un ZIP, extraer contenido
                    ruta_zip = os.path.join(directorio_txt, archivo.filename)
                    with open(ruta_zip, 'wb') as f:
                        content = await archivo.read()
                        f.write(content)
                    
                    with zipfile.ZipFile(ruta_zip, 'r') as zip_ref:
                        archivos_en_zip = zip_ref.namelist()
                        zip_ref.extractall(directorio_txt)
                        
                        # Contar archivos tipo I en el ZIP
                        for nombre in archivos_en_zip:
                            if "_I_" in nombre and nombre.lower().endswith('.txt'):
                                archivos_tipo_i += 1
                                archivos_validos.append(nombre)
                    
                    os.remove(ruta_zip)
                    
                elif archivo.filename.lower().endswith('.txt') and "_I_" in archivo.filename:
                    # Es un archivo TXT tipo I directo
                    ruta_txt = os.path.join(directorio_txt, archivo.filename)
                    with open(ruta_txt, 'wb') as f:
                        content = await archivo.read()
                        f.write(content)
                    
                    archivos_tipo_i += 1
                    archivos_validos.append(archivo.filename)
                else:
                    archivos_invalidos.append(f"{archivo.filename} (no es tipo I o formato incorrecto)")
                    
            except Exception as e:
                archivos_invalidos.append(f"{archivo.filename} (error: {str(e)})")
        
        validacion_txt = {
            "total": total_archivos,
            "archivos_tipo_i": archivos_tipo_i,
            "archivos_validos": archivos_validos,
            "archivos_invalidos": archivos_invalidos
        }
        
        # Limpiar directorio temporal
        shutil.rmtree(temp_dir)
        
        return {
            "archivo_log": validacion_log,
            "archivos_txt": validacion_txt
        }
        
    except Exception as e:
        if 'temp_dir' in locals():
            shutil.rmtree(temp_dir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=f"Error en validación: {str(e)}")

@app.post("/cruce-log/procesar/")
async def procesar_cruce_log(
    archivo_log: UploadFile = File(...),
    archivos_txt: List[UploadFile] = File(...),
    meses_referencia: int = Form(2)
):
    """Procesa el cruce entre LOG y archivos tipo I"""
    try:
        # Crear directorio del proceso
        proceso_id = str(uuid.uuid4())
        directorio_proceso = os.path.join("procesos", proceso_id)
        os.makedirs(directorio_proceso, exist_ok=True)
        
        # Guardar archivo LOG
        ruta_log = os.path.join(directorio_proceso, archivo_log.filename)
        with open(ruta_log, 'wb') as f:
            content = await archivo_log.read()
            f.write(content)
        
        # Crear directorio para archivos TXT
        directorio_txt = os.path.join(directorio_proceso, "archivos_txt")
        os.makedirs(directorio_txt, exist_ok=True)
        
        # Procesar archivos TXT/ZIP
        for archivo in archivos_txt:
            if archivo.filename.lower().endswith('.zip'):
                # Extraer ZIP
                ruta_zip = os.path.join(directorio_txt, archivo.filename)
                with open(ruta_zip, 'wb') as f:
                    content = await archivo.read()
                    f.write(content)
                
                with zipfile.ZipFile(ruta_zip, 'r') as zip_ref:
                    zip_ref.extractall(directorio_txt)
                
                os.remove(ruta_zip)
            else:
                # Guardar archivo TXT directamente
                ruta_txt = os.path.join(directorio_txt, archivo.filename)
                with open(ruta_txt, 'wb') as f:
                    content = await archivo.read()
                    f.write(content)
        
        # Ruta del archivo ZIP de salida
        ruta_salida_zip = os.path.join(directorio_proceso, f"cruce_log_resultado_{proceso_id}.zip")
        
        # Procesar con la clase CruceLogProcessor
        procesador = CruceLogProcessor(ruta_log, directorio_txt, ruta_salida_zip, meses_referencia)
        resultados = procesador.procesar_archivos()
        procesador.guardar_y_comprimir_archivos(resultados)
        
        # Preparar estadísticas para headers
        stats = resultados["estadisticas"]
        
        # Verificar que el archivo ZIP existe
        if not os.path.exists(ruta_salida_zip):
            raise HTTPException(status_code=500, detail="Error al generar el archivo de resultados")
        
        # Retornar el archivo ZIP con estadísticas en headers
        return FileResponse(
            path=ruta_salida_zip,
            filename=f"cruce_log_resultado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
            media_type="application/zip",
            headers={
                "X-Matches-Encontrados": str(stats["matches_encontrados"]),
                "X-Capital-Actual": str(stats["capital_actual"]),
                "X-Capital-Anterior": str(stats["capital_anterior"]),
                "X-Interes-Actual": str(stats["interes_actual"]),
                "X-Interes-Anterior": str(stats["interes_anterior"]),
                "X-Total-Archivos-I": str(stats["total_archivos_i"]),
                "X-Errores": str(stats["errores"])
            }
        )
        
    except Exception as e:
        print(f"Error en procesamiento: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error en procesamiento: {str(e)}")

# ============================================================================
# PUNTO DE ENTRADA PARA DESARROLLO
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)