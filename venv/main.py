from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse
from zipfile import ZipFile
import os
import tempfile

app = FastAPI()

@app.post("/cargar-archivo/")
async def cargar_archivo(file: UploadFile = File(...)):
    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_path = os.path.join(tmp_dir, file.filename)

        with open(zip_path, "wb") as f:
            f.write(await file.read())

        try:
            with ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(tmp_dir)
        except Exception as e:
            return JSONResponse(status_code=400, content={"error": f"No se pudo descomprimir el archivo ZIP: {str(e)}"})

        # Listar todos los archivos extraídos con ruta relativa
        archivos = []
        for root, _, files in os.walk(tmp_dir):
            for nombre in files:
                ruta_relativa = os.path.relpath(os.path.join(root, nombre), tmp_dir)
                archivos.append(ruta_relativa)

        return {
            "nombre": file.filename,
            "tamaño": os.path.getsize(zip_path),
            "archivos_encontrados": archivos
        }
