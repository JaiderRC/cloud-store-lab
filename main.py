"""
Aplicación FastAPI para evaluación de computación en la nube.
Integra: Cloud SQL (PostgreSQL), Cloud Storage, Datastore, App Engine.
"""

import os
import uuid
import logging
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, UploadFile, File
from pydantic import BaseModel
from google.cloud import storage, datastore
from google.cloud.exceptions import GoogleCloudError
from dotenv import load_dotenv
load_dotenv()

# ── Registro de eventos ───────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Aplicación ────────────────────────────────────────────────────────────────
app = FastAPI(title="API de Evaluación — Computación en la Nube")

# ── Variables de entorno ──────────────────────────────────────────────────────
ENTORNO             = os.getenv("APP_ENV", "desarrollo")
PROYECTO_GCP        = os.getenv("GOOGLE_CLOUD_PROJECT", "")
NOMBRE_BUCKET       = os.getenv("GCS_BUCKET_NAME", "")
COLECCION_AUDITORIA = os.getenv("FIRESTORE_COLLECTION_AUDIT_EVENTS", "eventos_auditoria")

DB_HOST       = os.getenv("DB_HOST", "")
DB_PUERTO     = int(os.getenv("DB_PORT", "5432"))
DB_NOMBRE     = os.getenv("DB_NAME", "")
DB_USUARIO    = os.getenv("DB_USER", "")
DB_CONTRASENA = os.getenv("DB_PASSWORD", "")


# ── Helpers de base de datos ──────────────────────────────────────────────────

def obtener_conexion_db():
    """
    Retorna una conexión psycopg2.
    En App Engine, DB_HOST debe ser la ruta del socket Unix de Cloud SQL:
        /cloudsql/<PROYECTO>:<REGION>:<INSTANCIA>
    En local puede ser 127.0.0.1 usando el Cloud SQL Auth Proxy.
    """
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PUERTO,
        dbname=DB_NOMBRE,
        user=DB_USUARIO,
        password=DB_CONTRASENA,
    )


def inicializar_db():
    """Crea las tablas si aún no existen."""
    ddl = """
    CREATE TABLE IF NOT EXISTS productos (
        id          SERIAL PRIMARY KEY,
        nombre      VARCHAR(255) NOT NULL,
        descripcion TEXT,
        precio      NUMERIC(10,2) NOT NULL,
        url_imagen  TEXT,
        creado_en   TIMESTAMPTZ DEFAULT NOW()
    );
    """
    try:
        conn = obtener_conexion_db()
        with conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
        conn.close()
        logger.info("Base de datos inicializada correctamente.")
    except Exception as exc:
        logger.warning("Inicialización de DB omitida (sin Cloud SQL disponible): %s", exc)


@app.on_event("startup")
def al_iniciar():
    inicializar_db()


# ── Helpers de Cloud Storage ──────────────────────────────────────────────────

def obtener_cliente_gcs() -> storage.Client:
    return storage.Client(project=PROYECTO_GCP)


def subir_a_gcs(contenido: bytes, nombre_objeto: str, tipo_contenido: str) -> str:
    """
    Sube bytes a Cloud Storage y retorna la URL pública del objeto.
    Compatible con buckets que tienen 'uniform bucket-level access' activado.
    El acceso público se gestiona a nivel de bucket (allUsers:objectViewer).
    """
    cliente = obtener_cliente_gcs()
    bucket  = cliente.bucket(NOMBRE_BUCKET)
    blob    = bucket.blob(nombre_objeto)
    blob.upload_from_string(contenido, content_type=tipo_contenido)
    return f"https://storage.googleapis.com/{NOMBRE_BUCKET}/{nombre_objeto}"


# ── Helpers de Datastore ──────────────────────────────────────────────────────

def obtener_cliente_datastore() -> datastore.Client:
    return datastore.Client(project=PROYECTO_GCP)


def registrar_evento_auditoria(tipo_evento: str, detalles: dict):
    """Escribe un documento de auditoría en Datastore."""
    try:
        cliente = obtener_cliente_datastore()
        entidad = datastore.Entity(cliente.key(COLECCION_AUDITORIA))
        entidad.update({
            "tipo_evento":  tipo_evento,
            "detalles":     str(detalles),
            "marca_tiempo": datetime.now(timezone.utc),
        })
        cliente.put(entidad)
    except Exception as exc:
        logger.error("Error al escribir evento de auditoría: %s", exc)


# ── Modelos Pydantic ──────────────────────────────────────────────────────────

class CrearProducto(BaseModel):
    nombre: str
    descripcion: str | None = None
    precio: float


class CrearComentario(BaseModel):
    autor: str
    texto: str


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def verificar_salud():
    """
    Health check — verifica la conectividad con Cloud SQL,
    Cloud Storage y Datastore.
    """
    estado: dict = {"estado": "ok", "entorno": ENTORNO, "servicios": {}}

    # Cloud SQL
    try:
        conn = obtener_conexion_db()
        conn.close()
        estado["servicios"]["cloud_sql"] = "ok"
    except Exception as exc:
        estado["servicios"]["cloud_sql"] = f"error: {exc}"
        estado["estado"] = "degradado"

    # Cloud Storage
    try:
        cliente = obtener_cliente_gcs()
        cliente.get_bucket(NOMBRE_BUCKET)
        estado["servicios"]["cloud_storage"] = "ok"
    except Exception as exc:
        estado["servicios"]["cloud_storage"] = f"error: {exc}"
        estado["estado"] = "degradado"

    # Datastore
    try:
        cliente = obtener_cliente_datastore()
        consulta = cliente.query(kind=COLECCION_AUDITORIA)
        list(consulta.fetch(limit=1))
        estado["servicios"]["datastore"] = "ok"
    except Exception as exc:
        estado["servicios"]["datastore"] = f"error: {exc}"
        estado["estado"] = "degradado"

    return estado


@app.post("/products", status_code=201)
def crear_producto(datos: CrearProducto):
    """Crea un producto en Cloud SQL (PostgreSQL)."""
    sql = """
        INSERT INTO productos (nombre, descripcion, precio)
        VALUES (%s, %s, %s)
        RETURNING id, nombre, descripcion, precio, creado_en;
    """
    try:
        conn = obtener_conexion_db()
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (datos.nombre, datos.descripcion, datos.precio))
                fila = dict(cur.fetchone())
        conn.close()
    except psycopg2.Error as exc:
        raise HTTPException(status_code=500, detail=f"Error en la base de datos: {exc}") from exc

    fila["creado_en"] = fila["creado_en"].isoformat()
    registrar_evento_auditoria("producto_creado", {"id_producto": fila["id"], "nombre": datos.nombre})
    return fila


@app.get("/products")
def listar_productos():
    """Retorna todos los productos almacenados en Cloud SQL."""
    sql = "SELECT id, nombre, descripcion, precio, url_imagen, creado_en FROM productos ORDER BY id;"
    try:
        conn = obtener_conexion_db()
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                filas = [dict(f) for f in cur.fetchall()]
        conn.close()
    except psycopg2.Error as exc:
        raise HTTPException(status_code=500, detail=f"Error en la base de datos: {exc}") from exc

    for f in filas:
        f["creado_en"] = f["creado_en"].isoformat() if f["creado_en"] else None

    return {"productos": filas, "total": len(filas)}


@app.post("/products/{id_producto}/image")
def subir_imagen_producto(id_producto: int, archivo: UploadFile = File(...)):
    """
    Sube una imagen a Cloud Storage y guarda la URL pública
    en la tabla de productos de Cloud SQL.
    """
    # Verificar que el producto existe
    try:
        conn = obtener_conexion_db()
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM productos WHERE id = %s;", (id_producto,))
                if cur.fetchone() is None:
                    raise HTTPException(status_code=404, detail="Producto no encontrado")
        conn.close()
    except psycopg2.Error as exc:
        raise HTTPException(status_code=500, detail=f"Error en la base de datos: {exc}") from exc

    # Validar tipo de archivo
    tipos_permitidos = {"image/jpeg", "image/png", "image/webp", "image/gif"}
    if archivo.content_type not in tipos_permitidos:
        raise HTTPException(
            status_code=400,
            detail="Solo se aceptan imágenes en formato jpeg, png, webp o gif."
        )

    extension     = archivo.filename.rsplit(".", 1)[-1] if "." in archivo.filename else "bin"
    nombre_objeto = f"productos/{id_producto}/{uuid.uuid4()}.{extension}"
    contenido     = archivo.file.read()

    # Subir a Cloud Storage
    try:
        url_publica = subir_a_gcs(contenido, nombre_objeto, archivo.content_type)
    except GoogleCloudError as exc:
        raise HTTPException(status_code=500, detail=f"Error en Cloud Storage: {exc}") from exc

    # Guardar URL en Cloud SQL
    try:
        conn = obtener_conexion_db()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE productos SET url_imagen = %s WHERE id = %s;",
                    (url_publica, id_producto),
                )
        conn.close()
    except psycopg2.Error as exc:
        raise HTTPException(status_code=500, detail=f"Error en la base de datos: {exc}") from exc

    registrar_evento_auditoria("imagen_subida", {"id_producto": id_producto, "url": url_publica})
    return {"id_producto": id_producto, "url_imagen": url_publica}


@app.post("/products/{id_producto}/comments", status_code=201)
def agregar_comentario(id_producto: int, datos: CrearComentario):
    """Escribe un comentario en Datastore y registra el evento en auditoría."""

    # Verificar que el producto existe en Cloud SQL
    try:
        conn = obtener_conexion_db()
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM productos WHERE id = %s;", (id_producto,))
                if cur.fetchone() is None:
                    raise HTTPException(status_code=404, detail="Producto no encontrado")
        conn.close()
    except psycopg2.Error as exc:
        raise HTTPException(status_code=500, detail=f"Error en la base de datos: {exc}") from exc

    # Guardar comentario en Datastore
    try:
        cliente = obtener_cliente_datastore()
        entidad = datastore.Entity(cliente.key("comentarios_productos"))
        entidad.update({
            "id_producto": id_producto,
            "autor":       datos.autor,
            "texto":       datos.texto,
            "creado_en":   datetime.now(timezone.utc),
        })
        cliente.put(entidad)
        id_comentario = entidad.key.id
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error en Datastore: {exc}") from exc

    registrar_evento_auditoria(
        "comentario_agregado",
        {"id_producto": id_producto, "autor": datos.autor}
    )
    return {"id_comentario": id_comentario, "id_producto": id_producto}


@app.get("/audit/events")
def obtener_eventos_auditoria(limite: int = 50):
    """
    Retorna los eventos de auditoría almacenados en Datastore,
    ordenados por marca de tiempo de forma descendente.
    """
    try:
        cliente  = obtener_cliente_datastore()
        consulta = cliente.query(kind=COLECCION_AUDITORIA)
        consulta.order = ["-marca_tiempo"]
        eventos  = []
        for entidad in consulta.fetch(limit=limite):
            datos        = dict(entidad)
            datos["id"]  = entidad.key.id
            if "marca_tiempo" in datos and hasattr(datos["marca_tiempo"], "isoformat"):
                datos["marca_tiempo"] = datos["marca_tiempo"].isoformat()
            eventos.append(datos)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error en Datastore: {exc}") from exc

    return {"eventos": eventos, "total": len(eventos)}