import os
from database import get_engine, initialize_database
from data_handler import FILE_READERS, DataValidator, DataPersister, VALIDATION_CONFIG

FILES_DIR = 'files'

def get_file_type(filename: str) -> str:
    return os.path.splitext(filename)[1].lower()

def get_table_name(filename: str) -> str:
    return os.path.splitext(filename)[0].lower()

def main():
    print("Iniciando la aplicación de carga de datos...")

    # 1.Se inicializa el motor de la base de datos y se verifica la conexión
    engine = get_engine()
    initialize_database(engine) 
    data_persister = DataPersister(engine)

    # 2.Se escanea el directorio de archivos
    if not os.path.exists(FILES_DIR):
        print(f"Error: El directorio '{FILES_DIR}' no existe. Por favor, créelo y coloque sus archivos allí.")
        return

    # Se obtiene lista de todos los archivos en el directorio 'files/'
    files_to_process = [f for f in os.listdir(FILES_DIR) if os.path.isfile(os.path.join(FILES_DIR, f))]

    if not files_to_process:
        print(f"No se encontraron archivos en el directorio '{FILES_DIR}'.")
        return

    print(f"\nSe encontraron {len(files_to_process)} archivos en '{FILES_DIR}':")
    for f in files_to_process:
        print(f"  - {f}")

    processed_successfully = [] # Lista para archivos cargados con éxito
    processed_failed = []      # Lista para archivos que fallaron al cargar

    print("\n--- Procesando Archivos ---")
    for filename in files_to_process:
        filepath = os.path.join(FILES_DIR, filename)
        file_type = get_file_type(filename)
        table_name = get_table_name(filename)

        print(f"\nProcesando '{filename}' (Tipo: {file_type}, Tabla: {table_name})...")

        # Verifica si el tipo de archivo es soportado por nuestros lectores
        if file_type not in FILE_READERS:
            print(f"  - Error: Tipo de archivo '{file_type}' no soportado.")
            processed_failed.append(filename)
            continue

        try:
            # Obtenemos el lector de archivos adecuado
            file_reader = FILE_READERS[file_type]
            df = file_reader.read_file(filepath)

            if df.empty:
                print(f"  - El archivo '{filename}' está vacío o no contiene datos válidos después de la lectura.")
                processed_failed.append(filename)
                continue

            # Configuración de validación específica para este archivo. Se usa el nombre base del archivo (ej. 'users' para 'users.csv').
            file_base_name = os.path.splitext(filename)[0].lower()
            validation_settings = VALIDATION_CONFIG.get(file_base_name, {})
            required_fields = validation_settings.get('required_fields', [])
            data_types = validation_settings.get('data_types', {})

            # Se validan y limpian los datos utilizando la clase DataValidator
            data_validator = DataValidator(required_fields=required_fields, data_types=data_types)
            cleaned_df = data_validator.validate_and_clean(df)

            if cleaned_df.empty:
                print(f"  - Después de la validación y limpieza, el DataFrame para '{filename}' está vacío. No se guardará nada.")
                processed_failed.append(filename)
                continue

            # Crea tabla dinámicamente y guardar los datos
            data_persister.create_table_from_dataframe(cleaned_df, table_name)
            data_persister.save_dataframe_to_db(cleaned_df, table_name)

            processed_successfully.append(filename) # Añadir a la lista de éxitos

        except Exception as e:
            # Captura cualquier excepción durante el procesamiento del archivo
            print(f"  - Falló el procesamiento de '{filename}': {e}")
            processed_failed.append(filename) # Se agrega lista de fallos

    print("\n--- Resumen de Carga ---")
    if processed_successfully:
        print("Archivos cargados exitosamente:")
        for f in processed_successfully:
            print(f"  - {f}")
    else:
        print("No se cargaron archivos exitosamente.")

    if processed_failed:
        print("\nArchivos que fallaron al cargar:")
        for f in processed_failed:
            print(f"  - {f}")
    else:
        print("Todos los archivos se cargaron o procesaron sin errores.")

    print("\nAplicación finalizada.")

if __name__ == "__main__":
    main()