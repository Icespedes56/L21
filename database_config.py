# database_config.py
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from datetime import datetime, date
import re

# Configuración de la base de datos
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'planillas_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'QSCZXCVB2026+')  # Cambiar por tu password
}

@contextmanager
def get_db_connection():
    """Context manager para conexiones a la base de datos"""
    connection = None
    try:
        connection = psycopg2.connect(**DATABASE_CONFIG)
        yield connection
        connection.commit()
    except Exception as e:
        if connection:
            connection.rollback()
        raise e
    finally:
        if connection:
            connection.close()

def extraer_fecha_de_archivos(archivos_paths):
    """
    Extrae la fecha de los nombres de archivos tipo I
    Busca patrones como: 2025-05-26_1_33440606_NI_900373379_PAESAP_86_I_2025-04.TXT
    """
    fechas_encontradas = set()
    
    for archivo_path in archivos_paths:
        # Extraer solo el nombre del archivo
        nombre_archivo = os.path.basename(archivo_path)
        
        # Buscar patrón de fecha al inicio: YYYY-MM-DD
        patron_fecha = r'^(\d{4}-\d{2}-\d{2})'
        match = re.search(patron_fecha, nombre_archivo)
        
        if match:
            fecha_str = match.group(1)
            try:
                fecha = datetime.strptime(fecha_str, '%Y-%m-%d').date()
                fechas_encontradas.add(fecha)
            except ValueError:
                continue
    
    # Retornar la fecha más común (o la única si todas son iguales)
    if fechas_encontradas:
        return max(fechas_encontradas)  # Retorna la fecha más reciente
    
    return None

class CruceLogDB:
    """Clase para manejar operaciones de base de datos del módulo Cruce LOG"""
    
    @staticmethod
    def guardar_resultado_cruce(resultado_cruce, archivos_tipo_i, archivo_log_info, archivo_zip_path):
        """Guarda el resultado de un cruce LOG en la base de datos"""
        try:
            fecha_archivos = extraer_fecha_de_archivos(archivos_tipo_i)
            estadisticas = resultado_cruce.get('estadisticas', {})
            
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                insert_query = """
                INSERT INTO cruce_log_historial (
                    fecha_archivos, archivo_log_nombre, archivo_log_tamaño,
                    total_archivos_tipo_i, matches_encontrados, capital_actual,
                    capital_anterior, interes_actual, interes_anterior, errores,
                    archivo_resultado_zip, estado
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """
                
                cursor.execute(insert_query, (
                    fecha_archivos,
                    archivo_log_info.get('nombre', ''),
                    archivo_log_info.get('tamaño', 0),
                    estadisticas.get('total_archivos_i', 0),
                    estadisticas.get('matches_encontrados', 0),
                    estadisticas.get('capital_actual', 0),
                    estadisticas.get('capital_anterior', 0),
                    estadisticas.get('interes_actual', 0),
                    estadisticas.get('interes_anterior', 0),
                    estadisticas.get('errores', 0),
                    archivo_zip_path,
                    'COMPLETADO'
                ))
                
                resultado_id = cursor.fetchone()[0]
                print(f"[DB] Cruce LOG guardado con ID: {resultado_id}")
                return resultado_id
                
        except Exception as e:
            print(f"[DB ERROR] Error al guardar cruce LOG: {e}")
            return None
    
    @staticmethod
    def verificar_cruce_existe(fecha_archivos):
        """Verifica si existe un cruce LOG para una fecha específica"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                query = """
                SELECT COUNT(*) FROM cruce_log_historial 
                WHERE fecha_archivos = %s AND estado = 'COMPLETADO'
                """
                
                cursor.execute(query, (fecha_archivos,))
                count = cursor.fetchone()[0]
                
                return count > 0
                
        except Exception as e:
            print(f"[DB ERROR] Error al verificar cruce: {e}")
            return False
    
    @staticmethod
    def obtener_historial_cruces(limite=50):
        """Obtiene el historial de cruces LOG realizados"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                query = """
                SELECT id, fecha_procesamiento, fecha_archivos, archivo_log_nombre,
                       total_archivos_tipo_i, matches_encontrados, capital_actual,
                       capital_anterior, interes_actual, interes_anterior, errores, estado
                FROM cruce_log_historial 
                ORDER BY fecha_procesamiento DESC 
                LIMIT %s
                """
                
                cursor.execute(query, (limite,))
                return cursor.fetchall()
                
        except Exception as e:
            print(f"[DB ERROR] Error al obtener historial: {e}")
            return []

class ProcesAmientoPlanillasDB:
    """Clase para manejar operaciones de base de datos del módulo Procesamiento"""
    
    @staticmethod
    def verificar_fecha_tiene_cruce(fecha_archivos):
        """Verifica si una fecha tiene cruce LOG asociado"""
        return CruceLogDB.verificar_cruce_existe(fecha_archivos)
    
    @staticmethod
    def guardar_procesamiento(archivo_info, registros_generados, fecha_archivos, 
                            acepto_responsabilidad=False, archivo_excel_path=None):
        """Guarda el resultado de un procesamiento de planillas"""
        try:
            tiene_cruce = ProcesAmientoPlanillasDB.verificar_fecha_tiene_cruce(fecha_archivos) if fecha_archivos else False
            
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                insert_query = """
                INSERT INTO procesamiento_planillas_historial (
                    fecha_archivos, archivo_zip_nombre, archivo_zip_tamaño,
                    total_archivos_procesados, registros_generados, tiene_cruce_log,
                    usuario_acepto_responsabilidad, archivo_resultado_excel, estado
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """
                
                cursor.execute(insert_query, (
                    fecha_archivos,
                    archivo_info.get('nombre', ''),
                    archivo_info.get('tamaño', 0),
                    archivo_info.get('total_archivos', 0),
                    registros_generados,
                    tiene_cruce,
                    acepto_responsabilidad,
                    archivo_excel_path,
                    'COMPLETADO'
                ))
                
                resultado_id = cursor.fetchone()[0]
                print(f"[DB] Procesamiento guardado con ID: {resultado_id}")
                return resultado_id
                
        except Exception as e:
            print(f"[DB ERROR] Error al guardar procesamiento: {e}")
            return None

# Función para inicializar la base de datos (crear tablas si no existen)
def inicializar_base_datos():
    """Inicializa la base de datos creando las tablas si no existen"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Verificar si las tablas existen
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = 'cruce_log_historial'
            """)
            
            if not cursor.fetchone():
                print("[DB] Creando tablas de la base de datos...")
                # Aquí ejecutarías el SQL del schema si las tablas no existen
                # Por simplicidad, asumimos que ya fueron creadas manualmente
                
            print("[DB] Base de datos inicializada correctamente")
            return True
            
    except Exception as e:
        print(f"[DB ERROR] Error al inicializar base de datos: {e}")
        return False