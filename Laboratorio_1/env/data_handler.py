# Contenido del archivo: data_handler.py
import pandas as pd
from abc import ABC, abstractmethod
import os
from sqlalchemy import Table, Column, MetaData, Integer, String, Float, DateTime, Boolean, Date, inspect
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

# --- Abstracción: Clases Base y Manejadores ---

class AbstractFileReader(ABC):
    """
    Clase base abstracta para la lectura de diferentes formatos de archivo.
    Define la interfaz común para el método `read_file`, garantizando que
    todas las subclases implementen esta funcionalidad. Este es un ejemplo
    claro de **Abstracción**.
    """
    @abstractmethod
    def read_file(self, filepath: str) -> pd.DataFrame:
        """
        Lee un archivo desde la ruta especificada y devuelve un DataFrame de pandas.
        Este método debe ser implementado por todas las subclases concretas.
        """
        pass

class DataValidator:
    """
    Clase para la validación y limpieza de datos.
    Encapsula la lógica de validación de datos, como la eliminación de duplicados,
    la validación de campos obligatorios (nulos) y la verificación de tipos de datos.
    Este es un ejemplo de **Encapsulamiento**, ya que la lógica de validación
    está contenida dentro de esta clase.
    """
    def __init__(self, required_fields: list = None, data_types: dict = None):
        """
        Inicializa el validador con una lista de campos que no pueden ser nulos
        y un diccionario de tipos de datos esperados para ciertas columnas.
        """
        self.required_fields = required_fields if required_fields is not None else []
        self.data_types = data_types if data_types is not None else {}

    def validate_and_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Realiza una serie de validaciones y operaciones de limpieza en el DataFrame.
        El orden de las operaciones es importante: primero se eliminan duplicados,
        luego nulos en campos obligatorios y finalmente se validan/ajustan los tipos.
        """
        initial_rows = len(df)
        print(f"  - Iniciando validación para {initial_rows} filas.")

        # 1. Eliminar duplicados
        df_cleaned = self.remove_duplicates(df.copy())
        duplicates_removed = initial_rows - len(df_cleaned)
        if duplicates_removed > 0:
            print(f"    - Se eliminaron {duplicates_removed} filas duplicadas.")
        else:
            print("    - No se encontraron filas duplicadas.")

        # 2. Validar campos obligatorios y eliminar filas con nulos en ellos
        if self.required_fields:
            rows_before_null_check = len(df_cleaned)
            # Asegurarse de que los campos obligatorios realmente existan en el DataFrame
            actual_required_fields = [f for f in self.required_fields if f in df_cleaned.columns]
            if actual_required_fields:
                df_cleaned.dropna(subset=actual_required_fields, inplace=True)
                nulls_removed = rows_before_null_check - len(df_cleaned)
                if nulls_removed > 0:
                    print(f"    - Se eliminaron {nulls_removed} filas con valores nulos en campos obligatorios.")
                else:
                    print("    - No se encontraron valores nulos en campos obligatorios.")
            else:
                print("    - Ninguno de los campos obligatorios especificados fue encontrado en el archivo.")
        else:
            print("    - No se especificaron campos obligatorios para validación de nulos.")


        # 3. Validar tipos de datos y forzar la conversión (coerción)
        rows_before_type_check = len(df_cleaned)
        for col, expected_type in self.data_types.items():
            if col not in df_cleaned.columns:
                print(f"    - Advertencia: Campo '{col}' con tipo esperado no encontrado en los datos.")
                continue
            try:
                if expected_type == int:
                    # Usar pd.Int64Dtype() para permitir nulos en enteros,
                    # errors='coerce' convierte valores no válidos a NaN.
                    df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce').astype(pd.Int64Dtype())
                elif expected_type == float:
                    df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
                elif expected_type == datetime:
                    # pd.to_datetime convierte a NaT (Not a Time) si no es una fecha válida.
                    df_cleaned[col] = pd.to_datetime(df_cleaned[col], errors='coerce')
                elif expected_type == str:
                    df_cleaned[col] = df_cleaned[col].astype(str)
                elif expected_type == bool:
                    df_cleaned[col] = df_cleaned[col].astype(bool)
            except Exception as e:
                print(f"    - Error de conversión de tipo para la columna '{col}': {e}. Se intentará eliminar filas con valores no válidos.")
                # Si hay un error general en la conversión, se eliminarán las filas con NaNs resultantes.

        # Después de la coerción, eliminar las filas que tienen valores NaN
        # en las columnas donde se esperaban tipos específicos (debido a errores de conversión).
        initial_rows_after_coercion = len(df_cleaned)
        columns_to_check_for_nan_after_coerce = [col for col in self.data_types.keys() if col in df_cleaned.columns]
        if columns_to_check_for_nan_after_coerce:
            df_cleaned.dropna(subset=columns_to_check_for_nan_after_coerce, inplace=True)
            invalid_type_rows_removed = initial_rows_after_coercion - len(df_cleaned)
            if invalid_type_rows_removed > 0:
                print(f"    - Se eliminaron {invalid_type_rows_removed} filas con tipos de datos inválidos después de la conversión.")
            else:
                print("    - No se encontraron errores de tipo de datos que requirieran la eliminación de filas.")
        else:
            print("    - No se especificaron tipos de datos para validar o columnas no encontradas.")


        print(f"  - Validación finalizada. Filas restantes después de la limpieza: {len(df_cleaned)}")
        return df_cleaned

    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Elimina filas duplicadas del DataFrame.
        """
        return df.drop_duplicates()

    def remove_nulls(self, df: pd.DataFrame, subset: list = None) -> pd.DataFrame:
        """
        Elimina filas que contienen valores nulos.
        Si se especifica `subset`, solo considera esos campos para la eliminación.
        Este método es genérico y es llamado por `validate_and_clean` con `required_fields`.
        """
        return df.dropna(subset=subset)


class DataPersister:
    """
    Clase para la persistencia de datos en la base de datos usando SQLAlchemy.
    Gestiona la creación dinámica de tablas y el guardado de DataFrames.
    """
    def __init__(self, engine):
        """
        Inicializa el persistidor con el motor de SQLAlchemy.
        `_engine` es un atributo protegido, mostrando **Encapsulamiento**.
        """
        self._engine = engine
        self._metadata = MetaData() # Metadata para la creación dinámica de tablas

    def _get_sqlalchemy_type(self, pandas_type):
        """
        Mapea los tipos de datos de pandas a los tipos de columnas de SQLAlchemy.
        Esta es una función auxiliar interna.
        """
        if pd.api.types.is_integer_dtype(pandas_type):
            return Integer
        elif pd.api.types.is_float_dtype(pandas_type):
            return Float
        elif pd.api.types.is_datetime64_any_dtype(pandas_type):
            return DateTime
        elif pd.api.types.is_bool_dtype(pandas_type):
            return Boolean
        # Por defecto, si el tipo no se puede mapear específicamente, se usa String
        return String

    def create_table_from_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Crea dinámicamente una tabla en la base de datos basándose en las columnas
        y sus tipos inferidos del DataFrame de pandas.
        Si la tabla ya existe, no la recrea. Esto satisface el requisito
        "una tabla por archivo cargado".
        """
        inspector = inspect(self._engine)
        # Verificar si la tabla ya existe en la base de datos
        if inspector.has_table(table_name):
            print(f"  - La tabla '{table_name}' ya existe. No se recreará.")
            return

        columns = []
        for col_name, dtype in df.dtypes.items():
            sqlalchemy_type = self._get_sqlalchemy_type(dtype)
            columns.append(Column(col_name, sqlalchemy_type, nullable=True)) # Todas las columnas son anulables por defecto

        # Se podría añadir una columna de clave primaria autoincremental si fuera necesario,
        # pero para mantener la flexibilidad y no alterar el esquema original del archivo,
        # lo omitimos a menos que el requisito lo especifique explícitamente.
        # Si no hay ninguna columna en el DataFrame que sea una PK natural,
        # SQLAlchemy asignará una PK implícita o se puede definir una manualmente.

        # Definir la tabla utilizando MetaData
        table = Table(table_name, self._metadata, *columns)
        # Crea la tabla en la base de datos (si no existe)
        self._metadata.create_all(self._engine)
        print(f"  - Tabla '{table_name}' creada exitosamente.")


    def save_dataframe_to_db(self, df: pd.DataFrame, table_name: str):
        """
        Guarda los datos del DataFrame en la tabla especificada en la base de datos.
        Utiliza el método `to_sql` de pandas para una inserción eficiente.
        """
        try:
            # `if_exists='append'` añade nuevas filas a la tabla existente.
            # `index=False` evita que el índice del DataFrame se guarde como una columna.
            df.to_sql(table_name, self._engine, if_exists='append', index=False)
            print(f"  - Datos guardados exitosamente en la tabla '{table_name}'. Total de {len(df)} filas.")
        except SQLAlchemyError as e:
            print(f"  - Error al guardar datos en la tabla '{table_name}': {e}")
            raise # Relanza la excepción para que el proceso principal pueda manejarla


# --- Herencia y Polimorfismo: Implementaciones concretas de lectores ---

class CSVFileReader(AbstractFileReader):
    """
    Implementación concreta para leer archivos CSV.
    Demuestra **Herencia** de `AbstractFileReader` y **Polimorfismo**
    al implementar `read_file` de forma específica para CSV.
    """
    def read_file(self, filepath: str) -> pd.DataFrame:
        print(f"  - Leyendo archivo CSV: {os.path.basename(filepath)}")
        return pd.read_csv(filepath)

class ExcelFileReader(AbstractFileReader):
    """
    Implementación concreta para leer archivos XLSX.
    Demuestra **Herencia** de `AbstractFileReader` y **Polimorfismo**
    al implementar `read_file` de forma específica para XLSX.
    """
    def read_file(self, filepath: str) -> pd.DataFrame:
        print(f"  - Leyendo archivo XLSX: {os.path.basename(filepath)}")
        return pd.read_excel(filepath)

class JSONFileReader(AbstractFileReader):
    """
    Implementación concreta para leer archivos JSON.
    Demuestra **Herencia** de `AbstractFileReader` y **Polimorfismo**
    al implementar `read_file` de forma específica para JSON.
    """
    def read_file(self, filepath: str) -> pd.DataFrame:
        print(f"  - Leyendo archivo JSON: {os.path.basename(filepath)}")
        return pd.read_json(filepath)

class TextFileReader(AbstractFileReader):
    """
    Implementación concreta para leer archivos TXT.
    Demuestra **Herencia** de `AbstractFileReader` y **Polimorfismo**
    al implementar `read_file` de forma específica para TXT.
    Intenta leer el archivo TXT como un CSV con un delimitador (por defecto coma),
    si no es posible (por ejemplo, si el archivo es texto plano no estructurado),
    lo lee línea por línea y lo carga en una columna 'content'.
    """
    def __init__(self, delimiter: str = ','):
        """
        Inicializa el lector de TXT con un delimitador, útil si el TXT es semi-estructurado.
        """
        self.delimiter = delimiter

    def read_file(self, filepath: str) -> pd.DataFrame:
        print(f"  - Leyendo archivo TXT: {os.path.basename(filepath)}")
        try:
            # Intentar leer el archivo TXT como un CSV con el delimitador especificado
            df = pd.read_csv(filepath, sep=self.delimiter)
            # Si se pudo leer con el delimitador y tiene más de una columna, o si la primera columna
            # parece ser contenido estructurado, se devuelve el DataFrame.
            if len(df.columns) > 1 or (len(df.columns) == 1 and self.delimiter != ',' and df.columns[0].strip() != ''):
                 return df
            else:
                # Si el archivo no parece ser un CSV con el delimitador, leer línea por línea
                with open(filepath, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                # Eliminar saltos de línea y crear un DataFrame con una única columna 'content'
                return pd.DataFrame({'content': [line.strip() for line in lines]})
        except Exception:
            # Si falla la lectura con pd.read_csv por cualquier motivo, se usa el fallback
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            return pd.DataFrame({'content': [line.strip() for line in lines]})


# Diccionario que mapea extensiones de archivo a las instancias de las clases de lector.
# Esto es fundamental para el **Polimorfismo**, permitiendo seleccionar el lector
# adecuado en tiempo de ejecución sin lógica condicional explícada.
FILE_READERS = {
    '.csv': CSVFileReader(),
    '.xlsx': ExcelFileReader(),
    '.json': JSONFileReader(),
    '.txt': TextFileReader(delimiter=','), # Se puede ajustar el delimitador si los TXT tienen otro formato
}

# Configuración de validación por tipo de archivo (nombre base del archivo sin extensión).
# En una aplicación más grande, esto podría cargarse desde un archivo de configuración (YAML, JSON).
VALIDATION_CONFIG = {
    'users': {
        'required_fields': ['id', 'name', 'email'],
        'data_types': {'id': int, 'name': str, 'email': str, 'age': int, 'join_date': datetime}
    },
    'products': {
        'required_fields': ['product_id', 'name', 'price'],
        'data_types': {'product_id': str, 'name': str, 'price': float, 'stock': int}
    },
    'orders': {
        'required_fields': ['order_id', 'user_id', 'product_id', 'quantity', 'order_date'],
        'data_types': {'order_id': str, 'user_id': int, 'product_id': str, 'quantity': int, 'order_date': datetime}
    },
    'notes': { # Para archivos TXT que se leen en una columna 'content'
        'required_fields': ['content'],
        'data_types': {'content': str}
    }
}