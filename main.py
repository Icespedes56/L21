from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import shutil
import os
import zipfile
from pathlib import Path
import chardet
import pandas as pd
import uuid
import tempfile
from datetime import datetime, timedelta
from typing import List, Optional
from control_aportantes_processor import obtener_nits_con_planillas_procesadas
import re

# Importación del módulo control aportantes
from control_aportantes_processor import (
    procesar_excel_aportantes,
    obtener_nits_unicos,
    obtener_detalle_por_nit,
    obtener_filtros_geograficos,
    obtener_municipios_por_departamento,
    filtrar_nits_por_geografia,
    obtener_estadisticas_geograficas,
    obtener_nits_con_planillas_procesadas,
    aportantes_sessions
)

# Importación del módulo base de datos
from database_config import (
    CruceLogDB, 
    ProcesAmientoPlanillasDB,
    PlanillasDB, 
    inicializar_base_datos, 
    extraer_fecha_de_archivos
)
from cruce_log_processor import CruceLogProcessor

# FORZAR RECARGA DEL MÓDULO - AGREGAR ESTAS LÍNEAS:
import importlib
import sys
if 'cruce_log_processor' in sys.modules:
    importlib.reload(sys.modules['cruce_log_processor'])
    from cruce_log_processor import CruceLogProcessor

app = FastAPI(title="Backend Planillas", description="API para procesamiento de planillas y cruce de LOG")
def extraer_estadisticas_de_archivos(ruta_salida_zip):
    """
    Extrae las estadísticas reales de los archivos generados en el ZIP
    """
    import zipfile
    import os
    
    estadisticas = {
        "matches_encontrados": 0,
        "capital_actual": 0,
        "capital_anterior": 0,
        "interes_actual": 0,
        "interes_anterior": 0,
        "total_archivos_i": 0,
        "errores": 0
    }
    
    try:
        if not os.path.exists(ruta_salida_zip):
            return estadisticas
        
        with zipfile.ZipFile(ruta_salida_zip, 'r') as zip_ref:
            # Extraer estadísticas de los archivos de capital e interés
            archivos_a_revisar = [
                ('Capital_Actual.txt', 'capital_actual'),
                ('Capital_Anterior.txt', 'capital_anterior'), 
                ('Interes_Actual.txt', 'interes_actual'),
                ('Interes_Anterior.txt', 'interes_anterior')
            ]
            
            total_matches = 0
            
            for nombre_archivo, tipo_stat in archivos_a_revisar:
                try:
                    with zip_ref.open(nombre_archivo) as file:
                        contenido = file.read().decode('latin-1')
                        lineas = contenido.splitlines()
                        
                        # Buscar la línea de control (empieza con 8)
                        for linea in lineas:
                            if linea.startswith('8'):
                                try:
                                    # Extraer número de líneas procesadas (posiciones 4-12)
                                    num_lineas = int(linea[4:12])
                                    
                                    # Extraer valor total (posiciones 19-34)
                                    valor_total = int(linea[19:34])
                                    
                                    estadisticas[tipo_stat] = valor_total
                                    total_matches += num_lineas
                                    
                                    print(f"[STATS] {nombre_archivo}: {num_lineas} líneas, valor {valor_total}")
                                    break
                                except:
                                    continue
                                    
                except Exception as e:
                    print(f"[ERROR] No se pudo leer {nombre_archivo}: {e}")
                    continue
            
            # Calcular matches encontrados (promedio de líneas procesadas)
            estadisticas["matches_encontrados"] = 0  # Se completará desde el procesador
            
            # Revisar archivo de errores
            try:
                with zip_ref.open('Errores.txt') as file:
                    contenido_errores = file.read().decode('latin-1')
                    lineas_error = contenido_errores.splitlines()
                    # Contar solo líneas no vacías que parezcan errores reales
                    errores_reales = [l for l in lineas_error if l.strip() and not l.startswith('===')]
                    estadisticas["errores"] = len(errores_reales)
            except:
                estadisticas["errores"] = 0
                
    except Exception as e:
        print(f"[ERROR] Error al extraer estadísticas del ZIP: {e}")
    
    print(f"[STATS FINAL] Estadísticas extraídas: {estadisticas}")
    return estadisticas
#hasta aqui 
@app.on_event("startup")
async def startup_event():
    """Inicializar la base de datos al arrancar la aplicación"""
    print("[STARTUP] Inicializando base de datos...")
    try:
        inicializar_base_datos()
        print("[STARTUP] Base de datos inicializada correctamente")
    except Exception as e:
        print(f"[STARTUP ERROR] Error al inicializar BD: {e}")

# Configuración CORS
origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:5173", 
    "http://127.0.0.1:5173",
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Matches-Encontrados", "X-Capital-Actual", "X-Capital-Anterior", "X-Interes-Actual", "X-Interes-Anterior", "X-Total-Archivos-I", "X-Errores", "X-Guardado-BD"]
)

BASE_PATH = Path("procesos")
BASE_PATH.mkdir(exist_ok=True)

# Diccionario de correcciones
correcciones = {
    "ï¿½": "Ñ", "Ã'": "Ñ", "Ã¡": "á", "Ã©": "é", "Ã­": "í",
    "Ã³": "ó", "Ãº": "ú", "Ã‰": "É", "Ã": "Ó", "Ãš": "Ú", "Ã¼": "ü",
    "Ã ": "Ñ",
    "´": "'",
    "`": "'"
}

def limpiar_zeros(valor):
    """Elimina ceros iniciales de una cadena si es una cadena y no vacía."""
    if isinstance(valor, str) and valor:
        return valor.lstrip("0")
    return valor

def get_substring_safe(text, start, end):
    """Extrae una subcadena de forma segura."""
    if not isinstance(text, str):
        return ""
    if start >= len(text):
        return ""
    if end > len(text):
        return text[start:]
    return text[start:end]

# ENDPOINTS PRINCIPALES
@app.get("/")
async def root():
    """Endpoint raíz para verificar que la API está funcionando"""
    return {
        "message": "Backend Planillas API",
        "version": "1.0.0",
        "status": "running",
        "modulos": ["procesamiento", "cruce-log", "control-aportantes", "base-datos"]
    }

@app.get("/health")
async def health_check():
    """Endpoint de salud para monitoreo"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

# NUEVOS ENDPOINTS PARA BASE DE DATOS
@app.get("/cruce-log/verificar-fecha/{fecha}")
async def verificar_fecha_cruce(fecha: str):
    """Verifica si existe un cruce LOG para una fecha específica (YYYY-MM-DD)"""
    try:
        fecha_obj = datetime.strptime(fecha, '%Y-%m-%d').date()
        existe_cruce = CruceLogDB.verificar_cruce_existe(fecha_obj)
        
        return {
            "fecha": fecha,
            "tiene_cruce_log": existe_cruce,
            "mensaje": "Cruce LOG encontrado" if existe_cruce else "No se encontró cruce LOG para esta fecha"
        }
        
    except ValueError:
        raise HTTPException(status_code=400, detail="Formato de fecha inválido. Use YYYY-MM-DD")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al verificar fecha: {str(e)}")

@app.get("/cruce-log/historial")
async def obtener_historial_cruces(limite: int = 50):
    """Obtiene el historial de cruces LOG realizados"""
    try:
        historial = CruceLogDB.obtener_historial_cruces(limite)
        
        for item in historial:
            if item.get('fecha_archivos'):
                item['fecha_archivos'] = item['fecha_archivos'].isoformat()
            if item.get('fecha_procesamiento'):
                item['fecha_procesamiento'] = item['fecha_procesamiento'].isoformat()
        
        return {"historial": historial}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener historial: {str(e)}")

@app.post("/procesamiento/verificar-fecha/")
async def verificar_fecha_antes_procesar(archivos_info: dict):
    """Verifica si una fecha de archivos tiene cruce LOG antes de procesar"""
    try:
        fecha_str = archivos_info.get('fecha_archivos')
        if not fecha_str:
            return {"tiene_cruce_log": False, "fecha_archivos": None}
        
        fecha_obj = datetime.strptime(fecha_str, '%Y-%m-%d').date()
        tiene_cruce = ProcesAmientoPlanillasDB.verificar_fecha_tiene_cruce(fecha_obj)
        
        return {
            "fecha_archivos": fecha_str,
            "tiene_cruce_log": tiene_cruce,
            "mensaje": "Cruce LOG encontrado" if tiene_cruce else "No se encontró cruce LOG para esta fecha"
        }
        
    except Exception as e:
        return {"error": str(e), "tiene_cruce_log": False}

# ENDPOINTS DEL MÓDULO CONTROL APORTANTES
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
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/aportantes_all_data/{session_id}")
def get_all_aportantes_data(session_id: str):
    """Obtiene todos los datos de aportantes para alimentar el mapa interactivo"""
    try:
        df = aportantes_sessions.get(session_id)
        if df is None:
            return JSONResponse(status_code=400, content={"error": "Sesión no encontrada"})
        
        data = df.to_dict(orient="records")
        total_registros = len(data)
        
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

# ENDPOINTS DEL MÓDULO DE PROCESAMIENTO

@app.post("/info-zip/")
async def obtener_info_zip(archivo: UploadFile = File(...)):
    try:
        contents = await archivo.read()
        
        # Usar tempfile en lugar de /tmp/ para compatibilidad con Windows
        import tempfile
        temp_zip_path = os.path.join(tempfile.gettempdir(), archivo.filename)
        
        with open(temp_zip_path, "wb") as temp_file:
            temp_file.write(contents)
        
        if not zipfile.is_zipfile(temp_zip_path):
            os.remove(temp_zip_path)
            raise ValueError("El archivo subido no es un archivo ZIP válido.")
        
        archivos_validos = 0
        archivos_tipo_i = []
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if (file_info.filename.endswith('.txt') and 
                    ('_I_' in file_info.filename or '_A_' in file_info.filename)):
                    archivos_validos += 1
                    if '_I_' in file_info.filename:
                        archivos_tipo_i.append(file_info.filename)
        
        # Extraer fecha de archivos para verificar cruce LOG
        fecha_archivos = None
        tiene_cruce_log = False
        
        try:
            fecha_archivos = extraer_fecha_de_archivos(archivos_tipo_i) if archivos_tipo_i else None
            if fecha_archivos:
                tiene_cruce_log = ProcesAmientoPlanillasDB.verificar_fecha_tiene_cruce(fecha_archivos)
        except Exception as e:
            print(f"[WARNING] Error al verificar fecha/cruce: {e}")
            # Continuar sin verificación
        
        # Limpiar archivo temporal
        os.remove(temp_zip_path)
        
        return JSONResponse({
            "total_archivos": archivos_validos,
            "peso_archivo": len(contents),
            "fecha_archivos": fecha_archivos.isoformat() if fecha_archivos else None,
            "tiene_cruce_log": tiene_cruce_log,
            "requiere_confirmacion": not tiene_cruce_log
        })
        
    except Exception as e:
        # Limpiar archivo temporal si existe
        if 'temp_zip_path' in locals() and os.path.exists(temp_zip_path):
            try:
                os.remove(temp_zip_path)
            except:
                pass
        
        print(f"[ERROR] Error en info-zip: {e}")
        raise HTTPException(status_code=400, detail=f"Error al analizar ZIP: {str(e)}")

@app.post("/procesar/")
async def procesar_archivo_con_bd(
    archivo: UploadFile = File(...),
    acepto_responsabilidad: bool = Form(False)
):
    """Procesa archivo con verificación de cruce LOG y guardado en BD"""
    proceso_id = uuid.uuid4().hex
    carpeta_proceso = BASE_PATH / proceso_id
    
    try:
        carpeta_proceso.mkdir(parents=True, exist_ok=True)

        carpeta_i = carpeta_proceso / "Archivos_I"
        carpeta_a = carpeta_proceso / "Archivos_A"
        carpeta_i.mkdir()
        carpeta_a.mkdir()

        zip_path = carpeta_proceso / archivo.filename
        with open(zip_path, "wb") as buffer:
            shutil.copyfileobj(archivo.file, buffer)
        
        if not zipfile.is_zipfile(zip_path):
            raise ValueError("El archivo subido no es un archivo ZIP válido.")

        archivos_tipo_i_encontrados = []
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(carpeta_proceso)
            
            for file_info in zip_ref.infolist():
                if "_I_" in file_info.filename and file_info.filename.lower().endswith('.txt'):
                    archivos_tipo_i_encontrados.append(file_info.filename)
        
        fecha_archivos = extraer_fecha_de_archivos(archivos_tipo_i_encontrados)
        
        tiene_cruce_log = False
        if fecha_archivos:
            tiene_cruce_log = ProcesAmientoPlanillasDB.verificar_fecha_tiene_cruce(fecha_archivos)
        
        if not tiene_cruce_log and not acepto_responsabilidad:
            fecha_str = fecha_archivos.isoformat() if fecha_archivos else "No identificada"
            return JSONResponse(
                status_code=422,
                content={
                    "requires_confirmation": True,
                    "fecha_archivos": fecha_str,
                    "tiene_cruce_log": False,
                    "mensaje": f"⚠️ ADVERTENCIA: No se encontró cruce LOG bancario para la fecha {fecha_str}.\n\nEsto significa que los datos de capital e interés no han sido validados con el sistema bancario. Si continúa, usted asume la responsabilidad de la información que se genere.\n\n¿Desea continuar bajo su responsabilidad?"
                }
            )
        
        os.remove(zip_path)

        extracted_files = list(carpeta_proceso.glob("*"))
        for extracted_file in extracted_files:
            if extracted_file.is_file():
                if "_I_" in extracted_file.name and extracted_file.suffix.lower() == '.txt':
                    shutil.move(str(extracted_file), carpeta_i / extracted_file.name)
                elif "_A_" in extracted_file.name and extracted_file.suffix.lower() == '.txt':
                    shutil.move(str(extracted_file), carpeta_a / extracted_file.name)
                else:
                    try:
                        extracted_file.unlink() 
                    except OSError as e:
                        print(f"No se pudo eliminar el archivo {extracted_file.name}: {e}")

        for archivo_path in carpeta_i.glob("*.txt"):
            try:
                with archivo_path.open("rb") as f:
                    raw_content = f.read()
                    
                result = chardet.detect(raw_content)
                encoding_to_use = result['encoding'] if result['encoding'] and result['confidence'] > 0.7 else "latin-1"
                
                decoded_text = raw_content.decode(encoding_to_use, errors="replace")
                
                for damaged, corrected in correcciones.items():
                    decoded_text = decoded_text.replace(damaged, corrected)
                
                lineas = decoded_text.splitlines()

                if len(lineas) >= 7:
                    reorganizadas = [lineas[0], lineas[2], lineas[4], lineas[6]] + lineas[7:]
                    final_text = "\n".join(reorganizadas)
                else:
                    final_text = "\n".join(lineas)

                with archivo_path.open("w", encoding="utf-8") as f:
                    f.write(final_text)

            except Exception as e:
                print(f"Error al corregir codificación o reorganizar {archivo_path.name}: {e}")

        registros = []
        for archivo_path in carpeta_i.glob("*.txt"):
            try:
                with archivo_path.open("r", encoding="utf-8") as f:
                    lineas = f.readlines()
                
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

        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.tz_localize(None)

        salida_excel = carpeta_proceso / f"planillas_generadas_{proceso_id}.xlsx"
        df.to_excel(salida_excel, index=False)

        archivo_info = {
            'nombre': archivo.filename,
            'tamaño': archivo.size if hasattr(archivo, 'size') else 0,
            'total_archivos': len(archivos_tipo_i_encontrados)
        }
        
        registros_generados = len(registros)
        
        try:
            db_id = ProcesAmientoPlanillasDB.guardar_procesamiento(
                archivo_info,
                registros_generados,
                fecha_archivos,
                acepto_responsabilidad,
                str(salida_excel)
            )
            print(f"[BD] Procesamiento guardado en BD con ID: {db_id}")
            
            # *** LÍNEAS NUEVAS - GUARDAR PLANILLAS INDIVIDUALES ***
            if db_id:
                print(f"[BD] Guardando {len(registros)} planillas individuales...")
                planillas_guardadas = ProcesAmientoPlanillasDB.guardar_planillas_procesadas(df, db_id)
                print(f"[BD] ✅ {planillas_guardadas} planillas individuales guardadas exitosamente")
                
                # Mostrar NITs únicos que se procesaron
                nits_unicos = df['No. Identificación Aportante'].dropna().unique()
                print(f"[BD] 🎯 NITs procesados: {len(nits_unicos)} únicos")
                
        except Exception as e:
            print(f"[BD ERROR] No se pudo guardar procesamiento: {e}")

        headers = {
            "X-Registros-Generados": str(registros_generados),
            "X-Fecha-Archivos": fecha_archivos.isoformat() if fecha_archivos else "No identificada",
            "X-Tiene-Cruce-Log": str(tiene_cruce_log),
            "X-Acepto-Responsabilidad": str(acepto_responsabilidad)
        }

        return FileResponse(
            salida_excel, 
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"planillas_generadas.xlsx",
            headers=headers
        )

    except ValueError as ve:
        print(f"Error de validación: {ve}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as ex:
        print(f"Error inesperado en el procesamiento: {ex}")
        raise HTTPException(status_code=500, detail="Error interno del servidor al procesar el archivo.")
    finally:
        pass

# ENDPOINTS DEL MÓDULO CRUCE LOG
@app.post("/cruce-log/validar-archivos/")
async def validar_archivos_cruce_log(
    archivo_log: UploadFile = File(...),
    archivos_txt: List[UploadFile] = File(...)
):
    """Valida los archivos antes del procesamiento de cruce LOG"""
    try:
        temp_dir = tempfile.mkdtemp()
        
        validacion_log = {
            "nombre": archivo_log.filename,
            "tamaño": 0,
            "lineas_estimadas": 0,
            "valido": False
        }
        
        ruta_log = os.path.join(temp_dir, archivo_log.filename)
        with open(ruta_log, 'wb') as f:
            content = await archivo_log.read()
            f.write(content)
            validacion_log["tamaño"] = len(content)
        
        try:
            with open(ruta_log, 'r', encoding='latin-1') as f:
                lineas = f.readlines()
                validacion_log["lineas_estimadas"] = len(lineas)
                validacion_log["valido"] = len(lineas) > 2
        except:
            validacion_log["valido"] = False
        
        directorio_txt = os.path.join(temp_dir, "archivos_txt")
        os.makedirs(directorio_txt, exist_ok=True)
        
        archivos_validos = []
        archivos_invalidos = []
        total_archivos = len(archivos_txt)
        archivos_tipo_i = 0
        
        for archivo in archivos_txt:
            try:
                if archivo.filename.lower().endswith('.zip'):
                    ruta_zip = os.path.join(directorio_txt, archivo.filename)
                    with open(ruta_zip, 'wb') as f:
                        content = await archivo.read()
                        f.write(content)
                    
                    with zipfile.ZipFile(ruta_zip, 'r') as zip_ref:
                        archivos_en_zip = zip_ref.namelist()
                        zip_ref.extractall(directorio_txt)
                        
                        for nombre in archivos_en_zip:
                            if "_I_" in nombre and nombre.lower().endswith('.txt'):
                                archivos_tipo_i += 1
                                archivos_validos.append(nombre)
                    
                    os.remove(ruta_zip)
                    
                elif archivo.filename.lower().endswith('.txt') and "_I_" in archivo.filename:
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
async def procesar_cruce_log_con_bd(
    archivo_log: UploadFile = File(...),
    archivos_txt: List[UploadFile] = File(...),
    meses_referencia: int = Form(2),
    guardar_en_bd: bool = Form(True)
):
    """Procesa el cruce entre LOG y archivos tipo I con guardado automático en BD"""
    try:
        proceso_id = str(uuid.uuid4())
        directorio_proceso = os.path.join("procesos", proceso_id)
        os.makedirs(directorio_proceso, exist_ok=True)
        
        ruta_log = os.path.join(directorio_proceso, archivo_log.filename)
        with open(ruta_log, 'wb') as f:
            content = await archivo_log.read()
            f.write(content)
        
        directorio_txt = os.path.join(directorio_proceso, "archivos_txt")
        os.makedirs(directorio_txt, exist_ok=True)
        
        archivos_tipo_i_paths = []
        archivo_log_info = {
            'nombre': archivo_log.filename,
            'tamaño': len(content)
        }
        
        for archivo in archivos_txt:
            if archivo.filename.lower().endswith('.zip'):
                ruta_zip = os.path.join(directorio_txt, archivo.filename)
                with open(ruta_zip, 'wb') as f:
                    zip_content = await archivo.read()
                    f.write(zip_content)
                
                with zipfile.ZipFile(ruta_zip, 'r') as zip_ref:
                    archivos_en_zip = zip_ref.namelist()
                    zip_ref.extractall(directorio_txt)
                    
                    for nombre_archivo in archivos_en_zip:
                        if "_I_" in nombre_archivo and nombre_archivo.lower().endswith('.txt'):
                            archivos_tipo_i_paths.append(os.path.join(directorio_txt, nombre_archivo))
                
                os.remove(ruta_zip)
            else:
                ruta_txt = os.path.join(directorio_txt, archivo.filename)
                with open(ruta_txt, 'wb') as f:
                    txt_content = await archivo.read()
                    f.write(txt_content)
                
                if "_I_" in archivo.filename:
                    archivos_tipo_i_paths.append(ruta_txt)
        
        ruta_salida_zip = os.path.join(directorio_proceso, f"cruce_log_resultado_{proceso_id}.zip")
        
        procesador = CruceLogProcessor(ruta_log, directorio_txt, ruta_salida_zip, meses_referencia)
        resultados = procesador.procesar_archivos()
        procesador.guardar_y_comprimir_archivos(resultados)
        
        stats_reales = extraer_estadisticas_de_archivos(ruta_salida_zip)
        stats_procesador = resultados.get("estadisticas", {})

        print(f"[DEBUG] Stats del procesador: {stats_procesador}")
        print(f"[DEBUG] Stats extraídas de archivos: {stats_reales}")        
       # Usar las stats más completas
        stats = stats_reales.copy()
        stats["matches_encontrados"] = stats_procesador.get("matches_encontrados", 0)  # Usar matches del procesador
        stats["total_archivos_i"] = stats_procesador.get("total_archivos_i", 0)
        
        print(f"[DEBUG] Stats finales a enviar: {stats}")
        
        # Intentar guardar en BD si hay matches
        db_id = None
        guardado_exitoso = False
        
        if guardar_en_bd and stats.get("matches_encontrados", 0) > 0:
            try:
                db_id = CruceLogDB.guardar_resultado_cruce(
                    resultados, 
                    archivos_tipo_i_paths, 
                    archivo_log_info, 
                    ruta_salida_zip
                )
                guardado_exitoso = True
                print(f"[BD] Cruce guardado en BD con ID: {db_id}")
            except Exception as e:
                print(f"[BD ERROR] No se pudo guardar en BD: {e}")
                guardado_exitoso = False
        
        if not os.path.exists(ruta_salida_zip):
            raise HTTPException(status_code=500, detail="Error al generar el archivo de resultados")
        
        # CORRECCIÓN: Enviar estadísticas reales en headers
        headers = {
            "X-Matches-Encontrados": str(stats.get("matches_encontrados", 0)),
            "X-Capital-Actual": str(stats.get("capital_actual", 0)),
            "X-Capital-Anterior": str(stats.get("capital_anterior", 0)),
            "X-Interes-Actual": str(stats.get("interes_actual", 0)),
            "X-Interes-Anterior": str(stats.get("interes_anterior", 0)),
            "X-Total-Archivos-I": str(stats.get("total_archivos_i", 0)),
            "X-Errores": str(stats.get("errores", 0)),
            "X-Guardado-BD": str(guardado_exitoso).lower()
        }
        
        if db_id:
            headers["X-DB-ID"] = str(db_id)
        
        print(f"[DEBUG] Headers que se enviarán:")
        for key, value in headers.items():
            print(f"  {key}: {value}")
        
        return FileResponse(
            path=ruta_salida_zip,
            filename=f"cruce_log_resultado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
            media_type="application/zip",
            headers=headers
        )
        
    except Exception as e:
        print(f"[ERROR] Error en procesamiento: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error en procesamiento: {str(e)}")

# NUEVOS ENDPOINTS para consultar planillas desde Control Aportantes

@app.get("/aportantes_planillas/{nit}")
def get_planillas_por_nit(nit: str, limite: int = 100):
    """Obtiene planillas procesadas para un NIT específico desde Control Aportantes"""
    try:
        planillas = PlanillasDB.obtener_planillas_por_nit(nit, limite)
        estadisticas = PlanillasDB.obtener_estadisticas_planillas_nit(nit)
        
        return {
            "nit": nit,
            "planillas": planillas,
            "estadisticas": estadisticas,
            "total_encontradas": len(planillas)
        }
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.get("/verificar_planillas_disponibles/{nit}")
def verificar_planillas_disponibles(nit: str):
    """Verifica si hay planillas procesadas disponibles para un NIT"""
    try:
        from database_config import get_db_connection
        with get_db_connection() as conn:
            cursor = conn.cursor()
            query = "SELECT COUNT(*) FROM planillas_procesadas WHERE nit = %s"
            cursor.execute(query, (str(nit),))
            count = cursor.fetchone()[0]
            
            return {
                "nit": nit,
                "tiene_planillas": count > 0,
                "total_planillas": count
            }
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    
@app.get("/aportantes_nits_with_planillas/{session_id}")
async def get_nits_with_planillas(session_id: str):
    """Endpoint para obtener NITs que tienen planillas procesadas en la BD"""
    try:
        print(f"[API] Consultando NITs con planillas para sesión: {session_id}")
        
        # Llamar a la función que consulta la BD
        nits_con_planillas = obtener_nits_con_planillas_procesadas(session_id)
        
        print(f"[API] Encontrados {len(nits_con_planillas)} NITs con planillas")
        
        return {
            "success": True,
            "nits_with_planillas": nits_con_planillas,
            "total_with_planillas": len(nits_con_planillas)
        }
        
    except Exception as e:
        print(f"[API ERROR] Error obteniendo NITs con planillas: {e}")
        return {
            "success": False,
            "nits_with_planillas": [],
            "total_with_planillas": 0,
            "error": str(e)
        }
    
        
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)