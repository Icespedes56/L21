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
                partes = archivo.split('_')
                if len(partes) >= 3:
                    numero_clave = partes[2]  # El tercer elemento: 7972154738
                    print(f"[DEBUG] Número clave extraído: {numero_clave}")
                    
                    with open(ruta_archivo, 'r', encoding='latin-1') as f:
                        lineas = f.readlines()
                        
                        if len(lineas) >= 4:
                            # CORRECCIÓN: Extraer valores de posiciones específicas
                            linea2 = lineas[1].strip()  # Línea 2: capital
                            linea3 = lineas[2].strip()  # Línea 3: interés
                            linea4 = lineas[3].strip()  # Línea 4: total
                            
                            print(f"[DEBUG] Línea 2 (Capital): {linea2}")
                            print(f"[DEBUG] Línea 3 (Interés): {linea3}")
                            print(f"[DEBUG] Línea 4 (Total): {linea4}")
                            
                            # CORRECCIÓN: Extraer capital de posiciones 23-32 (índices 22-32)
                            capital_str = ""
                            if len(linea2) >= 32:
                                capital_str = linea2[22:32].lstrip('0')
                            valor_capital = int(capital_str) if capital_str else 0
                            
                            # CORRECCIÓN: Extraer interés de posiciones 14-22 (índices 13-22)
                            interes_str = ""
                            if len(linea3) >= 22:
                                interes_str = linea3[13:22].lstrip('0')
                            valor_interes = int(interes_str) if interes_str else 0
                            
                            # CORRECCIÓN: Extraer total de posiciones 9-19 (índices 8-19)
                            total_str = ""
                            if len(linea4) >= 19:
                                total_str = linea4[8:19].lstrip('0')
                            valor_total = int(total_str) if total_str else 0
                            
                            print(f"[DEBUG] Capital extraído: {valor_capital}")
                            print(f"[DEBUG] Interés extraído: {valor_interes}")
                            print(f"[DEBUG] Total extraído: {valor_total}")
                            
                            # Validación: capital + interés debería = total
                            suma_calculada = valor_capital + valor_interes
                            if suma_calculada == valor_total:
                                print(f"[DEBUG] ✅ Validación correcta: {valor_capital} + {valor_interes} = {valor_total}")
                            else:
                                print(f"[DEBUG] ⚠️ Discrepancia: {valor_capital} + {valor_interes} = {suma_calculada}, pero total es {valor_total}")
                            
                            datos[numero_clave] = {
                                "capital": valor_capital,
                                "interes": valor_interes,
                                "total": valor_total,
                                "archivo": archivo
                            }
                            
                        else:
                            archivos_con_error.append(f"{archivo} (pocas líneas: {len(lineas)})")
                            print(f"[ERROR] Archivo {archivo} tiene solo {len(lineas)} líneas")
                else:
                    archivos_con_error.append(f"{archivo} (formato de nombre incorrecto)")
                    print(f"[ERROR] No se pudo extraer número clave de {archivo}")
                            
            except Exception as e:
                archivos_con_error.append(f"{archivo} (error: {str(e)})")
                print(f"[ERROR] No se pudo procesar el archivo {archivo}: {e}")
        
        print(f"[DEBUG] Archivos procesados exitosamente: {len(datos)}")
        print(f"[DEBUG] Archivos con errores: {len(archivos_con_error)}")
        
        return datos, archivos_procesados, archivos_con_error

    def modificar_linea_para_capital(self, linea, valor_capital):
        """
        Modifica la línea del LOG para reemplazar el valor con el capital
        SIN agregar número secuencial al final
        """
        if len(linea) >= 88:
            linea_lista = list(linea.strip())
            
            # Reemplazar el valor en las posiciones 73-87 (15 caracteres) con el capital
            nuevo_valor_str = str(valor_capital).zfill(15)
            
            for i, caracter in enumerate(nuevo_valor_str):
                if 73 + i < len(linea_lista):
                    linea_lista[73 + i] = caracter
            
            return ''.join(linea_lista) + '\n'
        
        return linea
    
    def modificar_linea_para_interes(self, linea, valor_interes):
        """
        Modifica la línea del LOG para reemplazar el valor con el interés
        SIN agregar número secuencial al final
        """
        if len(linea) >= 88:
            linea_lista = list(linea.strip())
            
            # Reemplazar el valor en las posiciones 73-87 (15 caracteres) con el interés
            nuevo_valor_str = str(valor_interes).zfill(15)
            
            for i, caracter in enumerate(nuevo_valor_str):
                if 73 + i < len(linea_lista):
                    linea_lista[73 + i] = caracter
            
            return ''.join(linea_lista) + '\n'
        
        return linea

    def generar_lineas_control(self, lineas_procesadas):
        """
        Genera las líneas de control y total para cada archivo
        """
        if not lineas_procesadas:
            return []
        
        # Calcular suma total de los valores y contador de líneas
        suma_total = 0
        contador_lineas = len(lineas_procesadas)
        
        for linea in lineas_procesadas:
            # Extraer el valor de las posiciones 73-87
            if len(linea) >= 88:
                try:
                    valor = int(linea[73:88])
                    suma_total += valor
                except:
                    pass
        
        lineas_control = []
        
        # Generar líneas de control con el formato del archivo original
        contador_str = str(contador_lineas).zfill(8)
        suma_str = str(suma_total).zfill(15)
        
        # Línea de control principal
        linea_control = f"8000{contador_str}2000000{suma_str}0000"
        linea_control = linea_control.ljust(120) + '\n'
        lineas_control.append(linea_control)
        
        # Línea final
        linea_final = f"9000{contador_str}05000000000000{suma_str}000001"
        linea_final = linea_final.ljust(120) + '\n'
        lineas_control.append(linea_final)
        
        return lineas_control

    def procesar_archivos(self):
        """
        Procesa el cruce entre el archivo LOG y los archivos tipo I
        """
        datos_i, archivos_procesados, archivos_con_error = self.leer_archivos_tipo_I()
        print(f"\n[DEBUG] Se leyeron {len(datos_i)} registros del tipo I")
        
        if datos_i:
            numeros_muestra = list(datos_i.keys())[:5]
            print(f"[DEBUG] Números clave encontrados (muestra): {numeros_muestra}")

        capital_actual, capital_anterior = [], []
        interes_actual, interes_anterior = [], []
        errores = []
        
        matches_encontrados = 0
        lineas_sin_match = []

        if not os.path.exists(self.ruta_log):
            print(f"[ERROR] El archivo LOG no existe: {self.ruta_log}")
            return self._crear_resultado_vacio(archivos_procesados, archivos_con_error)

        # Fecha de referencia basada en meses hacia atrás desde HOY
        fecha_hoy = datetime.now()
        fecha_referencia = fecha_hoy - timedelta(days=self.meses_referencia * 30)
        print(f"[DEBUG] Fecha actual: {fecha_hoy.strftime('%Y-%m-%d')}")
        print(f"[DEBUG] Fecha de referencia ({self.meses_referencia} meses atrás): {fecha_referencia.strftime('%Y-%m-%d')}")
        print(f"[DEBUG] Fechas >= {fecha_referencia.strftime('%Y-%m-%d')} van a ACTUAL")
        print(f"[DEBUG] Fechas < {fecha_referencia.strftime('%Y-%m-%d')} van a ANTERIOR")

        with open(self.ruta_log, 'r', encoding='latin-1') as f:
            todas_las_lineas = f.readlines()
            # Mantener las primeras 2 líneas de header para los resultados
            header_lineas = todas_las_lineas[:2]
            lineas_log = todas_las_lineas[2:]
            print(f"\n[DEBUG] Se leyeron {len(lineas_log)} líneas del archivo LOG")

            for i, linea in enumerate(lineas_log, start=3):
                # Ignorar líneas de control/cierre que empiecen con 8 o 9
                if len(linea) >= 1 and linea[0] in ['8', '9']:
                    continue
                
                # Buscar el número clave en toda la línea
                match_encontrado = False
                datos_match = None
                numero_clave_usado = None
                
                for numero_archivo_i in datos_i.keys():
                    if numero_archivo_i in linea:
                        match_encontrado = True
                        datos_match = datos_i[numero_archivo_i]
                        numero_clave_usado = numero_archivo_i
                        break
                
                if match_encontrado:
                    matches_encontrados += 1
                    
                    print(f"\n[MATCH #{matches_encontrados}] Línea {i}: Encontrado '{numero_clave_usado}'")
                    print(f"  -> Archivo: {datos_match['archivo']}")
                    print(f"  -> Capital: {datos_match['capital']}, Interés: {datos_match['interes']}, Total: {datos_match['total']}")
                    
                    # VALIDACIÓN: Verificar que el total del LOG coincida con el total del archivo I
                    if len(linea) >= 88:
                        try:
                            valor_log = int(linea[73:88])
                            if valor_log == datos_match['total']:
                                print(f"  -> ✅ Validación LOG: {valor_log} coincide con total archivo I: {datos_match['total']}")
                            else:
                                print(f"  -> ⚠️ Discrepancia LOG: {valor_log} vs archivo I: {datos_match['total']} - PROCESANDO DE TODAS FORMAS")
                                errores.append(f"Línea {i}: {numero_clave_usado} - Discrepancia: LOG={valor_log}, Archivo I={datos_match['total']}\n")
                                # NO hacer continue - procesar de todas formas
                        except:
                            print(f"  -> ❌ Error al extraer valor del LOG - PROCESANDO DE TODAS FORMAS")
                            errores.append(f"Línea {i}: {numero_clave_usado} - Error al extraer valor del LOG\n")
                            # NO hacer continue - procesar de todas formas
                    
                    # Extraer fecha de la línea del LOG en posición fija
                    try:
                        # La fecha está en posición fija 56-64 (YYYYMMDD)
                        if len(linea) >= 64:
                            fecha_str = linea[56:64]
                            print(f"  -> Fecha extraída del LOG: '{fecha_str}'")
                            fecha_log = datetime.strptime(fecha_str, "%Y%m%d")
                            es_actual = fecha_log >= fecha_referencia
                            
                            periodo = "ACTUAL" if es_actual else "ANTERIOR"
                            print(f"  -> Fecha: {fecha_log.strftime('%Y-%m-%d')}, Período: {periodo}")
                            
                            # Procesar capital (siempre que sea > 0)
                            if datos_match["capital"] > 0:
                                linea_capital = self.modificar_linea_para_capital(linea, datos_match["capital"])
                                if es_actual:
                                    capital_actual.append(linea_capital)
                                    print(f"  -> Capital {datos_match['capital']} → Capital_Actual.txt")
                                else:
                                    capital_anterior.append(linea_capital)
                                    print(f"  -> Capital {datos_match['capital']} → Capital_Anterior.txt")
                            
                            # Procesar interés (siempre que sea > 0)
                            if datos_match["interes"] > 0:
                                linea_interes = self.modificar_linea_para_interes(linea, datos_match["interes"])
                                if es_actual:
                                    interes_actual.append(linea_interes)
                                    print(f"  -> Interés {datos_match['interes']} → Interes_Actual.txt")
                                else:
                                    interes_anterior.append(linea_interes)
                                    print(f"  -> Interés {datos_match['interes']} → Interes_Anterior.txt")
                            
                            # Si ambos son 0, reportar error
                            if datos_match["capital"] == 0 and datos_match["interes"] == 0:
                                errores.append(f"Línea {i}: {numero_clave_usado} - Capital y interés son 0\n")
                                
                        else:
                            errores.append(f"Línea {i}: {numero_clave_usado} - Línea muy corta para extraer fecha\n")
                            
                    except ValueError as e:
                        errores.append(f"Línea {i}: {numero_clave_usado} - Error en fecha '{fecha_str}': {str(e)}\n")
                else:
                    # Registrar TODAS las líneas sin match con su contenido completo
                    linea_contenido = linea.strip()
                    lineas_sin_match.append(f"Línea {i}: {linea_contenido}")

        # Agregar headers y líneas de control a cada archivo
        capital_actual_final = header_lineas + capital_actual + self.generar_lineas_control(capital_actual)
        capital_anterior_final = header_lineas + capital_anterior + self.generar_lineas_control(capital_anterior)
        interes_actual_final = header_lineas + interes_actual + self.generar_lineas_control(interes_actual)
        interes_anterior_final = header_lineas + interes_anterior + self.generar_lineas_control(interes_anterior)

        # Agregar errores de líneas sin match
        if lineas_sin_match:
            errores.append("\n=== LÍNEAS SIN MATCH EN ARCHIVOS TIPO I ===\n")
            for error in lineas_sin_match:
                errores.append(f"{error}\n")

        # Agregar errores de archivos con problemas
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
        print(f"Capital Actual: {len(capital_actual)} líneas")
        print(f"Capital Anterior: {len(capital_anterior)} líneas")
        print(f"Interés Actual: {len(interes_actual)} líneas")
        print(f"Interés Anterior: {len(interes_anterior)} líneas")
        print(f"Total errores: {len(errores)}")

        return {
            "Capital_Actual.txt": capital_actual_final,
            "Capital_Anterior.txt": capital_anterior_final,
            "Interes_Actual.txt": interes_actual_final,
            "Interes_Anterior.txt": interes_anterior_final,
            "Errores.txt": errores,
            "estadisticas": estadisticas
        }

    def _crear_resultado_vacio(self, archivos_procesados, archivos_con_error):
        """Crea un resultado vacío cuando no hay archivo LOG"""
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

    def guardar_y_comprimir_archivos(self, resultados):
        """
        Guarda los resultados en archivos TXT y los comprime en un ZIP
        """
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
            
            for archivo in archivos_txt:
                try:
                    os.remove(archivo)
                except:
                    pass
            
            print(f"\n✅ Archivo ZIP generado: {self.ruta_salida_zip}")
        else:
            print("\n⚠️ No se generó ZIP porque no hay archivos para incluir")

        return archivos_txt
