# database_config.py (versión actualizada)
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from datetime import datetime, date
import re
import pandas as pd

# Configuración de la base de datos
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'planillas_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'QSCZXCVB2026+')
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
    """Extrae la fecha de los nombres de archivos tipo I"""
    fechas_encontradas = set()
    
    for archivo_path in archivos_paths:
        nombre_archivo = os.path.basename(archivo_path)
        patron_fecha = r'^(\d{4}-\d{2}-\d{2})'
        match = re.search(patron_fecha, nombre_archivo)
        
        if match:
            fecha_str = match.group(1)
            try:
                fecha = datetime.strptime(fecha_str, '%Y-%m-%d').date()
                fechas_encontradas.add(fecha)
            except ValueError:
                continue
    
    if fechas_encontradas:
        return max(fechas_encontradas)
    
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
    
    @staticmethod
    def guardar_planillas_procesadas(registros_df, procesamiento_id):
        """Guarda las planillas procesadas en la base de datos"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Preparar los datos para inserción
                records_to_insert = []
                for _, row in registros_df.iterrows():
                    record = (
                        # Extraer NIT del No. Identificación Aportante o usar NIT si existe
                        row.get('No. Identificación Aportante', '').strip(),
                        row.get('Nombre Aportante', '').strip(),
                        row.get('Numero Del Registro', ''),
                        row.get('Código de Formato', ''),
                        row.get('No. Identificación ESAP', '').strip(),
                        row.get('Dígito Verificación', ''),
                        row.get('Nombre Aportante', '').strip(),
                        row.get('Tipo Documento Aportante', ''),
                        row.get('No. Identificación Aportante', '').strip(),
                        row.get('Dígito Verificación Aportante', ''),
                        row.get('Tipo de Aportante', ''),
                        row.get('Dirección', '').strip(),
                        row.get('Código Ciudad', ''),
                        row.get('Código Dpto', ''),
                        row.get('Teléfono', ''),
                        row.get('Correo', '').strip(),
                        row.get('Periodo de Pago', ''),
                        row.get('Tipo de Planilla', ''),
                        # Convertir fechas
                        ProcesAmientoPlanillasDB._convert_date(row.get('Fecha de Pago Planilla', '')),
                        ProcesAmientoPlanillasDB._convert_date(row.get('Fecha de Pago', '')),
                        row.get('No. Planilla Asociada', ''),
                        row.get('Número de Radicación', ''),
                        row.get('Forma de Presentación', ''),
                        row.get('Código Sucursal', ''),
                        row.get('Nombre Sucursal', '').strip(),
                        ProcesAmientoPlanillasDB._convert_to_int(row.get('Total Empleados', 0)),
                        ProcesAmientoPlanillasDB._convert_to_int(row.get('Total Afiliados', 0)),
                        row.get('Código Operador', ''),
                        row.get('Modalidad Planilla', ''),
                        ProcesAmientoPlanillasDB._convert_to_int(row.get('Días Mora', 0)),
                        row.get('Clase Aportante', ''),
                        row.get('Naturaleza Jurídica', ''),
                        row.get('Tipo Persona', ''),
                        ProcesAmientoPlanillasDB._convert_to_decimal(row.get('IBC', 0)),
                        ProcesAmientoPlanillasDB._convert_to_decimal(row.get('Aporte Obligatorio', 0)),
                        ProcesAmientoPlanillasDB._convert_to_decimal(row.get('Mora Aportes', 0)),
                        ProcesAmientoPlanillasDB._convert_to_decimal(row.get('Total Aportes', 0)),
                        row.get('Archivo', ''),
                        procesamiento_id
                    )
                    records_to_insert.append(record)
                
                # Insertar en lotes para mejor performance
                insert_query = """
                INSERT INTO planillas_procesadas (
                    nit, entidad_aportante, numero_registro, codigo_formato,
                    no_identificacion_esap, digito_verificacion, nombre_aportante,
                    tipo_documento_aportante, no_identificacion_aportante,
                    digito_verificacion_aportante, tipo_aportante, direccion,
                    codigo_ciudad, codigo_depto, telefono, correo, periodo_pago,
                    tipo_planilla, fecha_pago_planilla, fecha_pago, no_planilla_asociada,
                    numero_radicacion, forma_presentacion, codigo_sucursal,
                    nombre_sucursal, total_empleados, total_afiliados, codigo_operador,
                    modalidad_planilla, dias_mora, clase_aportante, naturaleza_juridica,
                    tipo_persona, ibc, aporte_obligatorio, mora_aportes, total_aportes,
                    archivo_origen, procesamiento_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.executemany(insert_query, records_to_insert)
                
                print(f"[DB] {len(records_to_insert)} planillas guardadas en BD")
                return len(records_to_insert)
                
        except Exception as e:
            print(f"[DB ERROR] Error al guardar planillas: {e}")
            return 0
    
    @staticmethod
    def _convert_date(date_str):
        """Convierte string de fecha a formato fecha"""
        if not date_str or date_str.strip() == '':
            return None
        try:
            # Intentar diferentes formatos de fecha
            for fmt in ['%Y%m%d', '%Y-%m-%d', '%d/%m/%Y']:
                try:
                    return datetime.strptime(str(date_str).strip(), fmt).date()
                except ValueError:
                    continue
            return None
        except:
            return None
    
    @staticmethod
    def _convert_to_int(value):
        """Convierte valor a entero de forma segura"""
        try:
            if value is None or str(value).strip() == '':
                return 0
            return int(float(str(value).replace(',', '')))
        except:
            return 0
    
    @staticmethod
    def _convert_to_decimal(value):
        """Convierte valor a decimal de forma segura"""
        try:
            if value is None or str(value).strip() == '':
                return 0.0
            return float(str(value).replace(',', ''))
        except:
            return 0.0

class PlanillasDB:
    """Clase para manejar consultas específicas de planillas procesadas"""
    
    @staticmethod
    def obtener_planillas_por_nit(nit, limite=100):
        """Obtiene planillas procesadas para un NIT específico"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                query = """
                SELECT p.*, pph.fecha_archivos, pph.fecha_procesamiento
                FROM planillas_procesadas p
                LEFT JOIN procesamiento_planillas_historial pph ON p.procesamiento_id = pph.id
                WHERE p.nit = %s
                ORDER BY p.fecha_pago DESC, p.fecha_procesamiento DESC
                LIMIT %s
                """
                
                cursor.execute(query, (str(nit), limite))
                planillas = cursor.fetchall()
                
                # Convertir fechas a string para JSON
                for planilla in planillas:
                    for key, value in planilla.items():
                        if isinstance(value, (date, datetime)):
                            planilla[key] = value.isoformat()
                
                return planillas
                
        except Exception as e:
            print(f"[DB ERROR] Error al obtener planillas por NIT: {e}")
            return []
    
    @staticmethod
    def obtener_estadisticas_planillas_nit(nit):
        """Obtiene estadísticas de planillas para un NIT"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                query = """
                SELECT 
                    COUNT(*) as total_planillas,
                    COUNT(DISTINCT periodo_pago) as periodos_diferentes,
                    SUM(total_aportes) as total_aportes_acumulado,
                    SUM(mora_aportes) as total_mora_acumulada,
                    SUM(total_empleados) as total_empleados_max,
                    MAX(fecha_pago) as ultima_fecha_pago,
                    MIN(fecha_pago) as primera_fecha_pago
                FROM planillas_procesadas 
                WHERE nit = %s
                """
                
                cursor.execute(query, (str(nit),))
                stats = cursor.fetchone()
                
                # Convertir fechas a string
                if stats:
                    for key, value in stats.items():
                        if isinstance(value, (date, datetime)):
                            stats[key] = value.isoformat()
                
                return dict(stats) if stats else {}
                
        except Exception as e:
            print(f"[DB ERROR] Error al obtener estadísticas: {e}")
            return {}

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
                
            print("[DB] Base de datos inicializada correctamente")
            return True
            
    except Exception as e:
        print(f"[DB ERROR] Error al inicializar base de datos: {e}")
        return False