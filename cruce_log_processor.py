# cruce_log_processor.py
import os
import zipfile
from datetime import datetime, timedelta
import re

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
                # Formato: 2025-05-27_1_33267335_NI_840000594_PAESAP_86_I_2025-04.TXT
                # Necesitamos el número después de cualquier número + "_"
                patron = r'_(\d+)_(\d+)_'
                match = re.search(patron, archivo)
                if match:
                    numero_clave = match.group(2)  # El segundo número es el que necesitamos
                    print(f"[DEBUG] Número clave extraído: {numero_clave}")
                    
                    with open(ruta_archivo, 'r', encoding='latin-1') as f:
                        lineas = f.readlines()
                        
                        if len(lineas) >= 4:
                            # Línea 2 (índice 1): Capital 
                            # Línea 3 (índice 2): Interés
                            linea2 = lineas[1].strip()  # Segunda línea (índice 1)
                            linea3 = lineas[2].strip()  # Tercera línea (índice 2)
                            
                            print(f"[DEBUG] Línea 2: {linea2}")
                            print(f"[DEBUG] Línea 3: {linea3}")
                            
                            # Extraer capital de la línea 2
                            # Formato: 00031300000170948250000000085500
                            # Los últimos dígitos son el capital
                            valor_capital = 0
                            if linea2.isdigit() and len(linea2) >= 5:
                                # Tomar los últimos 5-10 dígitos para el capital
                                valor_capital = int(linea2[-10:]) if len(linea2) >= 10 else int(linea2[-5:])
                            
                            # Extraer interés de la línea 3
                            # Formato: 0003630000000000000000
                            valor_interes = 0
                            if linea3.isdigit() and len(linea3) >= 3:
                                # Buscar un valor no cero en la línea
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
                    print(f"[ERROR] No se pudo extraer número clave de {archivo} - patrón no encontrado")
                            
            except Exception as e:
                archivos_con_error.append(f"{archivo} (error: {str(e)})")
                print(f"[ERROR] No se pudo procesar el archivo {archivo}: {e}")
        
        print(f"[DEBUG] Archivos procesados exitosamente: {len(datos)}")
        print(f"[DEBUG] Archivos con errores: {len(archivos_con_error)}")
        if archivos_con_error:
            print(f"[DEBUG] Errores: {archivos_con_error}")
        
        return datos, archivos_procesados, archivos_con_error

    def modificar_linea_para_capital(self, linea, valor_capital):
        """
        Modifica la línea del LOG para reemplazar el valor existente en las posiciones 74-88
        con el valor de capital encontrado en los archivos tipo I
        """
        if len(linea) >= 88:  # Asegurarse de que la línea sea lo suficientemente larga
            # Convertir línea a lista para poder modificarla
            linea_lista = list(linea)
            
            # Las posiciones 74-88 corresponden a índices 73-87 (0-indexed)
            # Esto son 15 caracteres donde está el valor actual
            
            # Obtener el valor actual para analizarlo
            valor_actual_str = linea[73:88]  # Extraer posiciones 74-88
            print(f"[DEBUG CAPITAL] Valor actual en posiciones 74-88: '{valor_actual_str}'")
            
            # Encontrar el número actual dentro del string
            match = re.search(r'0*(\d+)', valor_actual_str)
            
            if match:
                numero_actual = int(match.group(1))
                print(f"[DEBUG CAPITAL] Número actual encontrado: {numero_actual}")
                
                # Crear el nuevo valor manteniendo el formato
                nuevo_valor_str = str(valor_capital)
                
                # Mantener la misma cantidad de dígitos que el formato original
                # Calcular cuántos ceros necesitamos
                longitud_total = 15  # Siempre 15 caracteres en posiciones 74-88
                
                if len(nuevo_valor_str) <= longitud_total:
                    # Rellenar con ceros a la izquierda
                    nuevo_segmento = nuevo_valor_str.zfill(longitud_total)
                else:
                    # Si es muy largo, truncar (caso raro)
                    nuevo_segmento = nuevo_valor_str[-longitud_total:]
                
                print(f"[DEBUG CAPITAL] Nuevo segmento: '{nuevo_segmento}'")
                
                # Reemplazar en la línea
                for i, caracter in enumerate(nuevo_segmento):
                    if 73 + i < len(linea_lista):
                        linea_lista[73 + i] = caracter
            else:
                # Si no se puede parsear, reemplazar todo con el nuevo valor
                nuevo_valor_formateado = str(valor_capital).zfill(15)
                print(f"[DEBUG CAPITAL] Reemplazo directo: '{nuevo_valor_formateado}'")
                for i, caracter in enumerate(nuevo_valor_formateado):
                    if 73 + i < len(linea_lista):
                        linea_lista[73 + i] = caracter
            
            return ''.join(linea_lista)
        
        return linea
    
    def modificar_linea_para_interes(self, linea, valor_interes):
        """
        Modifica la línea del LOG para reemplazar el valor existente en las posiciones 74-88
        con el valor de interés encontrado en los archivos tipo I
        """
        if len(linea) >= 88:  # Asegurarse de que la línea sea lo suficientemente larga
            # Convertir línea a lista para poder modificarla
            linea_lista = list(linea)
            
            # Las posiciones 74-88 corresponden a índices 73-87 (0-indexed)
            # Esto son 15 caracteres donde está el valor actual
            
            # Obtener el valor actual para analizarlo
            valor_actual_str = linea[73:88]  # Extraer posiciones 74-88
            print(f"[DEBUG INTERES] Valor actual en posiciones 74-88: '{valor_actual_str}'")
            
            # Encontrar el número actual dentro del string
            match = re.search(r'0*(\d+)', valor_actual_str)
            
            if match:
                numero_actual = int(match.group(1))
                print(f"[DEBUG INTERES] Número actual encontrado: {numero_actual}")
                
                # Crear el nuevo valor manteniendo el formato
                nuevo_valor_str = str(valor_interes)
                
                # Mantener la misma cantidad de dígitos que el formato original
                # Calcular cuántos ceros necesitamos
                longitud_total = 15  # Siempre 15 caracteres en posiciones 74-88
                
                if len(nuevo_valor_str) <= longitud_total:
                    # Rellenar con ceros a la izquierda
                    nuevo_segmento = nuevo_valor_str.zfill(longitud_total)
                else:
                    # Si es muy largo, truncar (caso raro)
                    nuevo_segmento = nuevo_valor_str[-longitud_total:]
                
                print(f"[DEBUG INTERES] Nuevo segmento: '{nuevo_segmento}'")
                
                # Reemplazar en la línea
                for i, caracter in enumerate(nuevo_segmento):
                    if 73 + i < len(linea_lista):
                        linea_lista[73 + i] = caracter
            else:
                # Si no se puede parsear, reemplazar todo con el nuevo valor
                nuevo_valor_formateado = str(valor_interes).zfill(15)
                print(f"[DEBUG INTERES] Reemplazo directo: '{nuevo_valor_formateado}'")
                for i, caracter in enumerate(nuevo_valor_formateado):
                    if 73 + i < len(linea_lista):
                        linea_lista[73 + i] = caracter
            
            return ''.join(linea_lista)
        
        return linea

    def procesar_archivos(self):
        """
        Procesa el cruce entre el archivo LOG y los archivos tipo I
        """
        datos_i, archivos_procesados, archivos_con_error = self.leer_archivos_tipo_I()
        print(f"\n[DEBUG] Se leyeron {len(datos_i)} registros del tipo I")
        
        # Mostrar algunos números clave para debugging
        if datos_i:
            numeros_muestra = list(datos_i.keys())[:10]
            print(f"[DEBUG] Números clave encontrados (muestra): {numeros_muestra}")

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
            print(f"\n[DEBUG] Se leyeron {len(lineas_log)} líneas del archivo LOG (sin contar header)")

            for i, linea in enumerate(lineas_log, start=3):  # Empezar desde línea 3
                # Extraer el número de las columnas 42-52 (posiciones 41-51 en Python, 0-indexed)
                if len(linea) >= 52:
                    numero_en_log = linea[41:51].strip()  # Columnas 42-51 (ajustado para 0-indexed)
                    
                    # Debug: mostrar algunos números del LOG
                    if i <= 12:  # Solo las primeras 10 líneas para debugging
                        print(f"[DEBUG LOG] Línea {i}, posiciones 41-51: '{numero_en_log}'")
                    
                    if numero_en_log in datos_i:
                        matches_encontrados += 1
                        datos = datos_i[numero_en_log]
                        
                        print(f"[MATCH #{matches_encontrados}] Línea {i}: Número {numero_en_log}")
                        print(f"  -> Archivo: {datos['archivo']}")
                        print(f"  -> Capital: {datos['capital']}, Interés: {datos['interes']}")
                        
                        # Extraer fecha de la línea del LOG (columnas 57-65, posiciones 56-64)
                        try:
                            if len(linea) >= 65:
                                fecha_str = linea[56:64]  # Columnas 57-64 (ajustado para 0-indexed)
                                print(f"  -> Fecha extraída: '{fecha_str}'")
                                fecha_log = datetime.strptime(fecha_str, "%Y%m%d")
                                es_actual = fecha_log >= fecha_referencia
                                
                                periodo = "Actual" if es_actual else "Anterior"
                                print(f"  -> Fecha: {fecha_log.strftime('%Y-%m-%d')}, Período: {periodo}")
                                
                                # Clasificar según las condiciones
                                # Si capital > 0 → Va a Capital
                                if datos["capital"] > 0:
                                    linea_capital = self.modificar_linea_para_capital(linea, datos["capital"])
                                    if es_actual:
                                        capital_actual.append(linea_capital)
                                        print(f"  -> Agregado a Capital Actual (capital: {datos['capital']})")
                                    else:
                                        capital_anterior.append(linea_capital)
                                        print(f"  -> Agregado a Capital Anterior (capital: {datos['capital']})")
                                
                                # Si interés > 0 → Va a Interés
                                if datos["interes"] > 0:
                                    linea_interes = self.modificar_linea_para_interes(linea, datos["interes"])
                                    if es_actual:
                                        interes_actual.append(linea_interes)
                                        print(f"  -> Agregado a Interés Actual (interés: {datos['interes']})")
                                    else:
                                        interes_anterior.append(linea_interes)
                                        print(f"  -> Agregado a Interés Anterior (interés: {datos['interes']})")
                                
                                # Si ninguno es > 0, va a errores
                                if datos["capital"] == 0 and datos["interes"] == 0:
                                    errores.append(f"Línea {i}: {numero_en_log} - Capital y interés son 0\n")
                                    print(f"  -> ERROR: Capital y interés son 0")
                            else:
                                errores.append(f"Línea {i}: {numero_en_log} - Línea muy corta para extraer fecha\n")
                                print(f"  -> ERROR: Línea muy corta para fecha")
                                
                        except ValueError as e:
                            errores.append(f"Línea {i}: {numero_en_log} - Error en fecha '{fecha_str}': {str(e)}\n")
                            print(f"  -> ERROR FECHA: '{fecha_str}' - {e}")
                    else:
                        # Solo registrar algunos errores para no saturar
                        if len(lineas_sin_match) < 10:
                            lineas_sin_match.append(f"Línea {i}: '{numero_en_log}' no encontrado")
                            if len(lineas_sin_match) <= 5:  # Solo mostrar los primeros 5 en debug
                                print(f"[NO MATCH] Línea {i}: '{numero_en_log}' no encontrado en archivos tipo I")
                else:
                    errores.append(f"Línea {i}: Línea muy corta (menos de 52 caracteres)\n")

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

        print(f"\n[RESUMEN DE MATCHES]")
        print(f"Total de matches encontrados: {matches_encontrados}")
        print(f"Líneas sin match: {len(lineas_sin_match)}")

        estadisticas = {
            "matches_encontrados": matches_encontrados,
            "capital_actual": len(capital_actual),
            "capital_anterior": len(capital_anterior),
            "interes_actual": len(interes_actual),
            "interes_anterior": len(interes_anterior),
            "total_archivos_i": archivos_procesados,
            "errores": len(errores)
        }

        print("\n[RESUMEN FINAL]")
        print("Capital Actual:", len(capital_actual))
        print("Capital Anterior:", len(capital_anterior))
        print("Interés Actual:", len(interes_actual))
        print("Interés Anterior:", len(interes_anterior))
        print("Errores registrados:", len(errores))

        return {
            "Capital_Actual.txt": capital_actual,
            "Capital_Anterior.txt": capital_anterior,
            "Interes_Actual.txt": interes_actual,
            "Interes_Anterior.txt": interes_anterior,
            "Errores.txt": errores,
            "estadisticas": estadisticas
        }

    def guardar_y_comprimir_archivos(self, resultados):
        """
        Guarda los resultados en archivos TXT y los comprime en un ZIP
        """
        ruta_temp = os.path.dirname(self.ruta_salida_zip)
        archivos_txt = []

        for nombre_archivo, contenido in resultados.items():
            if nombre_archivo == "estadisticas":
                continue
                
            if contenido:  # Solo crear archivos que tengan contenido
                ruta_completa = os.path.join(ruta_temp, nombre_archivo)
                
                with open(ruta_completa, 'w', encoding='latin-1') as f:
                    if nombre_archivo == "Errores.txt":
                        # Para errores, escribir como texto simple
                        f.writelines(contenido)
                    else:
                        # Para los demás, escribir las líneas del LOG
                        f.writelines(contenido)
                
                archivos_txt.append(ruta_completa)
                print(f"[ARCHIVO CREADO] {nombre_archivo} con {len(contenido)} líneas/registros")
            else:
                print(f"[ARCHIVO VACÍO] {nombre_archivo} - No se creará")

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
            
            print(f"\n✅ Archivo ZIP generado correctamente en:\n{self.ruta_salida_zip}")
            print(f"Archivos incluidos: {[os.path.basename(a) for a in archivos_txt]}")
        else:
            print("\n⚠️ No se generó ZIP porque no hay archivos para incluir")

    def ejecutar(self):
        """Ejecuta todo el flujo de procesamiento"""
        print("=== INICIANDO PROCESAMIENTO ===")
        print(f"Ruta LOG: {self.ruta_log}")
        print(f"Ruta TXT: {self.ruta_txt}")
        print(f"Ruta ZIP: {self.ruta_salida_zip}")
        
        resultados = self.procesar_archivos()
        self.guardar_y_comprimir_archivos(resultados)
        print("=== PROCESAMIENTO COMPLETADO ===")
