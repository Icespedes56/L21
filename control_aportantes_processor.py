import pandas as pd
import uuid
from typing import Dict, List, Optional

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
    return df_filtrado.to_dict(orient="records")

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
