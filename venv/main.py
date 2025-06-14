from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import shutil, os, zipfile
from pathlib import Path
import chardet
import pandas as pd
import uuid
import asyncio
import json
from typing import Dict

app = FastAPI()

# --- Configuración CORS ---
origins = [
    "http://localhost",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_PATH = Path("procesos")
BASE_PATH.mkdir(exist_ok=True)

# Diccionario para almacenar el progreso de cada proceso
progreso_procesos: Dict[str, dict] = {}

# Diccionario de correcciones más específico para caracteres problemáticos
correcciones = {
    "ï¿½": "Ñ", "Ã'": "Ñ", "Ã¡": "á", "Ã©": "é", "Ã­": "í",
    "Ã³": "ó", "Ãº": "ú", "Ã‰": "É", "Ã"": "Ó", "Ãš": "Ú", "Ã¼": "ü",
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

def actualizar_progreso(proceso_id: str, archivo_actual: str, procesados: int, total: int):
    """Actualiza el progreso del proceso en el diccionario global"""
    if proceso_id not in progreso_procesos:
        progreso_procesos[proceso_id] = {}
    
    progreso_procesos[proceso_id].update({
        "archivo_actual": archivo_actual,
        "procesados": procesados,
        "total": total,
        "porcentaje": round((procesados / total) * 100, 1) if total > 0 else 0
    })

# Nuevo endpoint para obtener información del ZIP
@app.post("/info-zip/")
async def obtener_info_zip(archivo: UploadFile = File(...)):
    try:
        contents = await archivo.read()
        temp_zip_path = f"/tmp/{archivo.filename}"
        with open(temp_zip_path, "wb") as temp_file:
            temp_file.write(contents)
        
        if not zipfile.is_zipfile(temp_zip_path):
            os.remove(temp_zip_path)
            raise ValueError("El archivo subido no es un archivo ZIP válido.")
        
        archivos_validos = 0
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if (file_info.filename.endswith('.txt') and 
                    ('_I_' in file_info.filename or '_A_' in file_info.filename)):
                    archivos_validos += 1
        
        os.remove(temp_zip_path)
        
        return JSONResponse({
            "total_archivos": archivos_validos,
            "peso_archivo": len(contents)
        })
        
    except Exception as e:
        if 'temp_zip_path' in locals() and os.path.exists(temp_zip_path):
            os.remove(temp_zip_path)
        raise HTTPException(status_code=400, detail=f"Error al analizar ZIP: {str(e)}")

# Nuevo endpoint para obtener el progreso de un proceso
@app.get("/progreso/{proceso_id}")
async def obtener_progreso(proceso_id: str):
    if proceso_id not in progreso_procesos:
        raise HTTPException(status_code=404, detail="Proceso no encontrado")
    
    return JSONResponse(progreso_procesos[proceso_id])

@app.post("/procesar/")
async def procesar_archivo(archivo: UploadFile = File(...)):
    proceso_id = uuid.uuid4().hex
    carpeta_proceso = BASE_PATH / proceso_id
    
    try:
        carpeta_proceso.mkdir(parents=True, exist_ok=True)
        
        # Inicializar progreso
        progreso_procesos[proceso_id] = {
            "archivo_actual": "Iniciando...",
            "procesados": 0,
            "total": 0,
            "porcentaje": 0,
            "fase": "Extrayendo archivos"
        }

        carpeta_i = carpeta_proceso / "Archivos_I"
        carpeta_a = carpeta_proceso / "Archivos_A"
        carpeta_i.mkdir()
        carpeta_a.mkdir()

        # 1. Guardar y descomprimir archivo ZIP
        zip_path = carpeta_proceso / archivo.filename
        with open(zip_path, "wb") as buffer:
            shutil.copyfileobj(archivo.file, buffer)
        
        if not zipfile.is_zipfile(zip_path):
            raise ValueError("El archivo subido no es un archivo ZIP válido.")

        progreso_procesos[proceso_id]["fase"] = "Extrayendo archivos del ZIP"
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(carpeta_proceso)
        
        os.remove(zip_path)

        # 2. Clasificar archivos
        progreso_procesos[proceso_id]["fase"] = "Clasificando archivos"
        
        extracted_files = list(carpeta_proceso.glob("*"))
        archivos_validos = 0
        
        for extracted_file in extracted_files:
            if extracted_file.is_file():
                if "_I_" in extracted_file.name and extracted_file.suffix.lower() == '.txt':
                    shutil.move(str(extracted_file), carpeta_i / extracted_file.name)
                    archivos_validos += 1
                elif "_A_" in extracted_file.name and extracted_file.suffix.lower() == '.txt':
                    shutil.move(str(extracted_file), carpeta_a / extracted_file.name)
                else:
                    try:
                        extracted_file.unlink() 
                    except OSError as e:
                        print(f"No se pudo eliminar el archivo {extracted_file.name}: {e}")

        # Actualizar total de archivos a procesar
        progreso_procesos[proceso_id]["total"] = archivos_validos
        progreso_procesos[proceso_id]["fase"] = "Procesando archivos"

        # 3. Corregir codificación y reorganizar líneas
        archivos_i = list(carpeta_i.glob("*.txt"))
        
        for idx, archivo_path in enumerate(archivos_i):
            try:
                # Actualizar progreso
                actualizar_progreso(proceso_id, archivo_path.name, idx, len(archivos_i))
                progreso_procesos[proceso_id]["fase"] = f"Corrigiendo codificación: {archivo_path.name}"
                
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
                
                # Pequeña pausa para permitir que el frontend actualice
                await asyncio.sleep(0.01)

            except Exception as e:
                print(f"Error al corregir codificación o reorganizar {archivo_path.name}: {e}")

        # 4. Extraer información y generar Excel
        progreso_procesos[proceso_id]["fase"] = "Extrayendo datos y generando Excel"
        registros = []
        
        archivos_procesados = 0
        for archivo_path in carpeta_i.glob("*.txt"):
            try:
                # Actualizar progreso de extracción
                actualizar_progreso(proceso_id, f"Extrayendo: {archivo_path.name}", archivos_procesados, len(archivos_i))
                
                with archivo_path.open("r", encoding="utf-8") as f:
                    lineas = f.readlines()
                
                if len(lineas) < 4:
                    print(f"Advertencia: El archivo {archivo_path.name} tiene menos de 4 líneas esperadas.")
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
                archivos_procesados += 1
                
                # Pequeña pausa para permitir que el frontend actualice
                await asyncio.sleep(0.01)
                
            except Exception as e:
                print(f"Error al extraer datos de {archivo_path.name}: {e}")

        # Actualizar progreso final
        progreso_procesos[proceso_id].update({
            "fase": "Generando archivo Excel",
            "procesados": len(registros),
            "total": len(registros),
            "porcentaje": 100,
            "registros_generados": len(registros)
        })

        df = pd.DataFrame(registros)

        # Convertir datetime con zona horaria a naive para evitar error al guardar en Excel
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.tz_localize(None)

        salida_excel = carpeta_proceso / f"planillas_generadas_{proceso_id}.xlsx"
        df.to_excel(salida_excel, index=False)

        # Actualizar progreso completado
        progreso_procesos[proceso_id].update({
            "fase": "Completado",
            "archivo_generado": f"planillas_generadas_{proceso_id}.xlsx",
            "lineas_excel": len(registros)
        })

        return FileResponse(
            salida_excel, 
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"planillas_generadas.xlsx",
            headers={"X-Proceso-ID": proceso_id, "X-Registros-Generados": str(len(registros))}
        )

    except ValueError as ve:
        print(f"Error de validación: {ve}")
        if proceso_id in progreso_procesos:
            progreso_procesos[proceso_id]["error"] = str(ve)
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as ex:
        print(f"Error inesperado en el procesamiento: {ex}")
        if proceso_id in progreso_procesos:
            progreso_procesos[proceso_id]["error"] = str(ex)
        raise HTTPException(status_code=500, detail="Error interno del servidor al procesar el archivo.")
    finally:
        # Limpiar progreso después de un tiempo
        def limpiar_progreso():
            if proceso_id in progreso_procesos:
                del progreso_procesos[proceso_id]
        
        # Programar limpieza en 5 minutos
        import threading
        timer = threading.Timer(300.0, limpiar_progreso)
        timer.start()




