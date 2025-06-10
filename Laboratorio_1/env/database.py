import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "sqlite:///./app_data.db"

def get_engine():
 
    return create_engine(DATABASE_URL)

def get_session(engine):

    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return Session()

def initialize_database(engine):

    try:
        with engine.connect() as connection:
            # Verifica la conectividad
            connection.execute(text("SELECT 1"))
        print(f"Conexión a la base de datos '{DATABASE_URL}' exitosa.")
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")