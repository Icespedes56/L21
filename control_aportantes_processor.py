import pandas as pd
import uuid
from typing import Dict, List, Optional
from database_config import get_db_connection


# Almacenamiento temporal en memoria (diccionario de sesiones)
aportantes_sessions: Dict[str, pd.DataFrame] = {}

def procesar_excel_aportantes(file) -> str:
    df = pd.read_excel(file, engine="openpyxl")
    
    # Validación mínima
    if 'NIT' not in df.columns:
        raise ValueError("El archivo no contiene la columna 'NIT'")
    
    # Debug básico: imprimir columnas encontradas
    print(f"[DEBUG] Archivo procesado con {len(df.columns)} columnas:")
    for i, col in enumerate(df.columns):
        print(f"  {i}: '{col}'")
    
    # Generamos un ID de sesión para este archivo
    session_id = str(uuid.uuid4())
    aportantes_sessions[session_id] = df
    return session_id

def obtener_nits_unicos(session_id: str):
    df = aportantes_sessions.get(session_id)
    if df is None:
        raise ValueError("Sesión no encontrada")
    
    nits = df['NIT'].dropna().unique()
    return sorted(nits.tolist())

def obtener_detalle_por_nit(session_id: str, nit: int):
    df = aportantes_sessions.get(session_id)
    if df is None:
        raise ValueError("Sesión no encontrada")
    
    df_filtrado = df[df['NIT'] == nit]
    
    # NUEVO: Enriquecer con información de planillas procesadas
    detalle_records = df_filtrado.to_dict(orient="records")
    
    # Verificar si hay planillas procesadas para este NIT
    try:
        planillas_info = verificar_planillas_disponibles_nit(nit)
        # Agregar información de planillas a cada registro
        for record in detalle_records:
            record['_planillas_info'] = planillas_info
    except Exception as e:
        print(f"[WARNING] No se pudo obtener info de planillas para NIT {nit}: {e}")
        for record in detalle_records:
            record['_planillas_info'] = {'tiene_planillas': False, 'total_planillas': 0}
    
    return detalle_records

# NUEVA FUNCIÓN PRINCIPAL PARA EL FRONTEND
def obtener_nits_con_planillas_procesadas(session_id: str):
    """
    Obtiene NITs de la sesión actual que tienen planillas procesadas en la base de datos
    Esta es la función principal que necesita el frontend
    """
    try:
        # 1. Obtener todos los NITs de la sesión actual
        df = aportantes_sessions.get(session_id)
        if df is None:
            print(f"[DEBUG] Sesión {session_id} no encontrada")
            return []
        
        nits_sesion = df['NIT'].dropna().unique().tolist()
        print(f"[DEBUG] NITs en sesión: {len(nits_sesion)}")
        
        # 2. Verificar cuáles tienen planillas en la BD
        nits_con_planillas = []
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Consulta optimizada para verificar múltiples NITs de una vez
            if nits_sesion:
                # Convertir NITs a strings para la consulta
                nits_str = [str(nit) for nit in nits_sesion]
                
                # Crear placeholders para la consulta IN
                placeholders = ','.join(['%s'] * len(nits_str))
                
                query = f"""
                SELECT DISTINCT nit 
                FROM planillas_procesadas 
                WHERE nit IN ({placeholders})
                """
                
                cursor.execute(query, nits_str)
                resultados = cursor.fetchall()
                
                # Extraer los NITs que tienen planillas y convertir a int
                for row in resultados:
                    try:
                        nit_int = int(row[0]) if row[0].isdigit() else None
                        if nit_int:
                            nits_con_planillas.append(nit_int)
                    except (ValueError, TypeError):
                        continue
                
                print(f"[DEBUG] NITs con planillas encontrados: {len(nits_con_planillas)}")
                if nits_con_planillas:
                    print(f"[DEBUG] Primeros NITs con planillas: {nits_con_planillas[:10]}")
        
        return nits_con_planillas
        
    except Exception as e:
        print(f"[DB ERROR] Error obteniendo NITs con planillas: {e}")
        return []

def verificar_planillas_disponibles_nit(nit: int):
    """Verifica si hay planillas procesadas disponibles para un NIT específico"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Consulta para verificar existencia y obtener estadísticas básicas
            query = """
            SELECT 
                COUNT(*) as total_planillas,
                COUNT(DISTINCT periodo_pago) as periodos_diferentes,
                MAX(fecha_pago) as ultima_fecha_pago,
                SUM(total_aportes) as total_aportes_acumulado,
                SUM(mora_aportes) as total_mora_acumulada
            FROM planillas_procesadas 
            WHERE nit = %s
            """
            
            cursor.execute(query, (str(nit),))
            result = cursor.fetchone()
            
            if result and result[0] > 0:
                return {
                    'tiene_planillas': True,
                    'total_planillas': result[0],
                    'periodos_diferentes': result[1],
                    'ultima_fecha_pago': result[2].isoformat() if result[2] else None,
                    'total_aportes_acumulado': float(result[3]) if result[3] else 0,
                    'total_mora_acumulada': float(result[4]) if result[4] else 0
                }
            else:
                return {
                    'tiene_planillas': False,
                    'total_planillas': 0,
                    'periodos_diferentes': 0,
                    'ultima_fecha_pago': None,
                    'total_aportes_acumulado': 0,
                    'total_mora_acumulada': 0
                }
                
    except Exception as e:
        print(f"[DB ERROR] Error al verificar planillas para NIT {nit}: {e}")
        return {
            'tiene_planillas': False,
            'total_planillas': 0,
            'error': str(e)
        }

def encontrar_columna_municipio(df):
    """Función SÚPER ROBUSTA para encontrar la columna de municipio"""
    
    print(f"[DEBUG] Buscando columna de municipio...")
    
    # Primero: buscar por nombres exactos
    nombres_exactos = [
        'MUNICIPIO / ISLA',
        'MUNICIPIO /ISLA', 
        'MUNICIPIO/ ISLA',
        'MUNICIPIO/ISLA',
        'MUNICIPIO',
        'Municipio',
        'municipio'
    ]
    
    for nombre in nombres_exactos:
        if nombre in df.columns and not df[nombre].dropna().empty:
            print(f"[DEBUG] Municipio encontrado por nombre exacto: '{nombre}'")
            return nombre
    
    # Segundo: buscar por cualquier columna que contenga "MUNICIPIO"
    for col in df.columns:
        if 'MUNICIPIO' in str(col).upper():
            if not df[col].dropna().empty:
                print(f"[DEBUG] Municipio encontrado por coincidencia: '{col}'")
                return col
    
    # Tercero: buscar por posición y contenido
    # Basándome en tu estructura: ID, NIT, SUFIJO, ENTIDAD, TERRITORIAL, DEPARTAMENTO, COD_DEPTO, MUNICIPIO, COD_MUNICIPIO
    if len(df.columns) >= 8:
        # La columna 7 (índice 7) debería ser municipio según tu estructura
        col_candidata = df.columns[7]
        muestra = df[col_candidata].dropna().head(5)
        # Verificar si los valores parecen municipios
        for valor in muestra:
            if isinstance(valor, str) and len(valor) > 3 and not valor.isdigit():
                print(f"[DEBUG] Municipio encontrado por posición: '{col_candidata}'")
                return col_candidata
    
    print(f"[DEBUG] No se pudo encontrar columna de municipio")
    return None

def encontrar_columna_departamento(df):
    """Función SÚPER ROBUSTA para encontrar la columna de departamento"""
    
    print(f"[DEBUG] Buscando columna de departamento...")
    
    # Primero: buscar por nombres exactos
    nombres_exactos = [
        'DEPARTAMENTO',
        'Departamento', 
        'departamento'
    ]
    
    for nombre in nombres_exactos:
        if nombre in df.columns and not df[nombre].dropna().empty:
            print(f"[DEBUG] Departamento encontrado por nombre exacto: '{nombre}'")
            return nombre
    
    # Segundo: buscar por cualquier columna que contenga "DEPARTAMENTO"
    for col in df.columns:
        if 'DEPARTAMENTO' in str(col).upper():
            if not df[col].dropna().empty:
                print(f"[DEBUG] Departamento encontrado por coincidencia: '{col}'")
                return col
    
    # Tercero: buscar por posición y contenido típico
    if len(df.columns) >= 6:
        col_candidata = df.columns[5]  # Posición 5 según tu estructura
        muestra = df[col_candidata].dropna().head(5)
        # Verificar si los valores parecen departamentos colombianos
        for valor in muestra:
            if isinstance(valor, str) and any(dept in str(valor).upper() for dept in 
                ['SANTANDER', 'CUNDINAMARCA', 'ANTIOQUIA', 'VALLE', 'ATLANTICO', 'BOLIVAR', 'MAGDALENA', 'AMAZONAS']):
                print(f"[DEBUG] Departamento encontrado por posición: '{col_candidata}'")
                return col_candidata
    
    print(f"[DEBUG] No se pudo encontrar columna de departamento")
    return None

# FUNCIONES PARA FILTROS GEOGRÁFICOS (CORREGIDAS)

def obtener_filtros_geograficos(session_id: str):
    """Obtiene listas únicas de departamentos y municipios"""
    df = aportantes_sessions.get(session_id)
    if df is None:
        raise ValueError("Sesión no encontrada")
    
    print(f"[DEBUG] Obteniendo filtros geográficos para sesión: {session_id}")
    
    # Buscar columnas usando las funciones helper
    departamento_col = encontrar_columna_departamento(df)
    municipio_col = encontrar_columna_municipio(df)
    
    filtros = {
        'departamentos': [],
        'municipios': []
    }
    
    if departamento_col:
        departamentos = df[departamento_col].dropna().unique().tolist()
        filtros['departamentos'] = sorted(departamentos)
        print(f"[DEBUG] {len(departamentos)} departamentos encontrados")
    
    if municipio_col:
        municipios = df[municipio_col].dropna().unique().tolist()
        filtros['municipios'] = sorted(municipios)
        print(f"[DEBUG] {len(municipios)} municipios encontrados")
        if len(municipios) > 0:
            print(f"[DEBUG] Primeros municipios: {municipios[:5]}")
    
    return filtros

def obtener_municipios_por_departamento(session_id: str, departamento: str):
    """Obtiene municipios de un departamento específico"""
    df = aportantes_sessions.get(session_id)
    if df is None:
        raise ValueError("Sesión no encontrada")
    
    print(f"[DEBUG] Buscando municipios para departamento: '{departamento}'")
    
    # Buscar columnas usando las funciones helper
    departamento_col = encontrar_columna_departamento(df)
    municipio_col = encontrar_columna_municipio(df)
    
    if not departamento_col or not municipio_col:
        print(f"[DEBUG] No se encontraron las columnas necesarias - Depto: {departamento_col}, Municipio: {municipio_col}")
        return []
    
    # Filtrar por departamento y obtener municipios únicos
    df_filtrado = df[df[departamento_col] == departamento]
    print(f"[DEBUG] Registros filtrados por departamento: {len(df_filtrado)}")
    
    if len(df_filtrado) == 0:
        print(f"[DEBUG] No se encontraron registros para el departamento: '{departamento}'")
        print(f"[DEBUG] Departamentos disponibles: {df[departamento_col].unique().tolist()}")
        return []
    
    municipios = df_filtrado[municipio_col].dropna().unique().tolist()
    print(f"[DEBUG] Municipios encontrados: {len(municipios)}")
    if len(municipios) > 0:
        print(f"[DEBUG] Primeros municipios: {municipios[:5]}")
    
    return sorted(municipios)

def filtrar_nits_por_geografia(session_id: str, departamento: Optional[str] = None, municipio: Optional[str] = None):
    """Filtra NITs por criterios geográficos"""
    df = aportantes_sessions.get(session_id)
    if df is None:
        raise ValueError("Sesión no encontrada")
    
    print(f"[DEBUG] Filtrando por - Depto: '{departamento}', Municipio: '{municipio}'")
    
    # Buscar columnas usando las funciones helper
    departamento_col = encontrar_columna_departamento(df)
    municipio_col = encontrar_columna_municipio(df)
    
    df_filtrado = df.copy()
    print(f"[DEBUG] Registros iniciales: {len(df_filtrado)}")
    
    # Aplicar filtros
    if departamento and departamento_col:
        print(f"[DEBUG] Aplicando filtro de departamento: '{departamento}' en columna '{departamento_col}'")
        df_filtrado = df_filtrado[df_filtrado[departamento_col] == departamento]
        print(f"[DEBUG] Después de filtrar por departamento: {len(df_filtrado)}")
    
    if municipio and municipio_col:
        print(f"[DEBUG] Aplicando filtro de municipio: '{municipio}' en columna '{municipio_col}'")
        df_filtrado = df_filtrado[df_filtrado[municipio_col] == municipio]
        print(f"[DEBUG] Después de filtrar por municipio: {len(df_filtrado)}")
    
    # Obtener NITs únicos del resultado filtrado
    nits_filtrados = df_filtrado['NIT'].dropna().unique()
    print(f"[DEBUG] NITs únicos filtrados: {len(nits_filtrados)}")
    
    return sorted(nits_filtrados.tolist())

def obtener_estadisticas_geograficas(session_id: str):
    """Obtiene estadísticas por departamento y municipio"""
    df = aportantes_sessions.get(session_id)
    if df is None:
        raise ValueError("Sesión no encontrada")
    
    # Buscar columnas usando las funciones helper
    departamento_col = encontrar_columna_departamento(df)
    municipio_col = encontrar_columna_municipio(df)
    
    estadisticas = {}
    
    if departamento_col:
        # Contar NITs únicos por departamento
        dept_stats = df.groupby(departamento_col)['NIT'].nunique().to_dict()
        estadisticas['por_departamento'] = dept_stats
    
    if municipio_col and departamento_col:
        # Contar NITs únicos por municipio
        mun_stats = df.groupby([departamento_col, municipio_col])['NIT'].nunique().reset_index()
        mun_stats['ubicacion'] = mun_stats[departamento_col] + ' - ' + mun_stats[municipio_col]
        estadisticas['por_municipio'] = mun_stats[['ubicacion', 'NIT']].set_index('ubicacion')['NIT'].to_dict()
    
    return estadisticas

def obtener_todos_los_datos_aportantes(session_id: str):
    """Obtiene todos los datos de aportantes para el mapa y análisis"""
    df = aportantes_sessions.get(session_id)
    if df is None:
        raise ValueError("Sesión no encontrada")
    
    try:
        # Buscar columnas
        departamento_col = encontrar_columna_departamento(df)
        municipio_col = encontrar_columna_municipio(df)
        
        # Preparar datos para el mapa
        datos_mapa = []
        
        if departamento_col and municipio_col:
            # Agrupar por departamento y municipio
            agrupados = df.groupby([departamento_col, municipio_col]).agg({
                'NIT': 'nunique',
                'ENTIDAD APORTANTE': 'count'
            }).reset_index()
            
            for _, row in agrupados.iterrows():
                datos_mapa.append({
                    'departamento': row[departamento_col],
                    'municipio': row[municipio_col],
                    'nits_unicos': int(row['NIT']),
                    'total_entidades': int(row['ENTIDAD APORTANTE'])
                })
        
        return datos_mapa
        
    except Exception as e:
        print(f"[ERROR] Error obteniendo datos para mapa: {e}")
        return []

def analizar_estructura_completa(session_id: str):
    """Función para analizar completamente la estructura del DataFrame"""
    df = aportantes_sessions.get(session_id)
    if df is None:
        return "Sesión no encontrada"
    
    print(f"\n{'='*80}")
    print(f"ANÁLISIS COMPLETO DEL ARCHIVO")
    print(f"{'='*80}")
    print(f"Dimensiones: {df.shape[0]} filas x {df.shape[1]} columnas")
    print(f"\nPrimeras 3 filas del DataFrame:")
    print(df.head(3).to_string())
    
    print(f"\nEstructura de columnas:")
    for i, col in enumerate(df.columns):
        tipo_datos = df[col].dtype
        valores_unicos = df[col].nunique()
        primer_valor = df[col].dropna().iloc[0] if not df[col].dropna().empty else "N/A"
        print(f"  {i:2}: '{col}' | Tipo: {tipo_datos} | Únicos: {valores_unicos} | Ejemplo: '{primer_valor}'")
    
    print(f"{'='*80}")
    return "Análisis completado"
