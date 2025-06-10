# Contenido del archivo: database.py
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Configuración de la URL de la base de datos.
# Usamos SQLite para simplificar, se creará un archivo 'app_data.db' en el mismo directorio.
DATABASE_URL = "sqlite:///./app_data.db"

def get_engine():
    """
    Crea y devuelve el motor de SQLAlchemy. Este motor es el punto de conexión
    con la base de datos.
    """
    return create_engine(DATABASE_URL)

def get_session(engine):
    """
    Crea y devuelve una sesión de SQLAlchemy. Las sesiones son unidades de trabajo
    a través de las cuales se interactúa con la base de datos (consultas, inserciones, etc.).
    """
    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return Session()

def initialize_database(engine):
    """
    Inicializa la base de datos. Para este enfoque dinámico, las tablas se crean
    cuando los DataFrames se guardan. Sin embargo, esta función puede usarse para
    verificar que la conexión a la base de datos es exitosa.
    """
    try:
        with engine.connect() as connection:
            # Ejecuta una consulta simple para verificar la conectividad
            connection.execute(text("SELECT 1"))
        print(f"Conexión a la base de datos '{DATABASE_URL}' exitosa.")
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        # En un entorno de producción, considerar un manejo de errores más robusto