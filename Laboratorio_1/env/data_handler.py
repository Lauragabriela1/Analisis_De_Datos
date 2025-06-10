import pandas as pd
from abc import ABC, abstractmethod
import os
from sqlalchemy import Table, Column, MetaData, Integer, String, Float, DateTime, Boolean, Date, inspect
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

class AbstractFileReader(ABC):
   
    @abstractmethod
    def read_file(self, filepath: str) -> pd.DataFrame:
        pass

class DataValidator:
    
    def __init__(self, required_fields: list = None, data_types: dict = None):
        
        self.required_fields = required_fields if required_fields is not None else []
        self.data_types = data_types if data_types is not None else {}

    def validate_and_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        
        initial_rows = len(df)
        print(f"  # Iniciando validación para {initial_rows} filas.")

        # 1. Elimina duplicados
        df_cleaned = self.remove_duplicates(df.copy())
        duplicates_removed = initial_rows - len(df_cleaned)
        if duplicates_removed > 0:
            print(f"    - Se eliminaron {duplicates_removed} filas duplicadas.")
        else:
            print("    - No se encontraron filas duplicadas.")

        # 2. Valida los campos obligatorios y se eliminan filas con valores nulos
        if self.required_fields:
            rows_before_null_check = len(df_cleaned)
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


        # 3. Valida los tipos de datos
        rows_before_type_check = len(df_cleaned)
        for col, expected_type in self.data_types.items():
            if col not in df_cleaned.columns:
                print(f"    - Advertencia: Campo '{col}' con tipo esperado no encontrado en los datos.")
                continue
            try:
                if expected_type == int:
                    df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce').astype(pd.Int64Dtype())
                elif expected_type == float:
                    df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
                elif expected_type == datetime:
                    df_cleaned[col] = pd.to_datetime(df_cleaned[col], errors='coerce')
                elif expected_type == str:
                    df_cleaned[col] = df_cleaned[col].astype(str)
                elif expected_type == bool:
                    df_cleaned[col] = df_cleaned[col].astype(bool)
            except Exception as e:
                print(f"    - Error de conversión de tipo para la columna '{col}': {e}. Se intentará eliminar filas con valores no válidos.")

        # Elimina las filas que tienen valores NaN
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
        #Elimina filas duplicadas
        return df.drop_duplicates()

    def remove_nulls(self, df: pd.DataFrame, subset: list = None) -> pd.DataFrame:
        #Elimina filas que tienen valores nulos.
        return df.dropna(subset=subset)

class DataPersister:
    
    def __init__(self, engine):
        self._engine = engine
        self._metadata = MetaData()

    def _get_sqlalchemy_type(self, pandas_type):
        if pd.api.types.is_integer_dtype(pandas_type):
            return Integer
        elif pd.api.types.is_float_dtype(pandas_type):
            return Float
        elif pd.api.types.is_datetime64_any_dtype(pandas_type):
            return DateTime
        elif pd.api.types.is_bool_dtype(pandas_type):
            return Boolean
        return String

    def create_table_from_dataframe(self, df: pd.DataFrame, table_name: str):
        
        inspector = inspect(self._engine)
        # Se verificar si la tabla ya existe en la base de datos
        if inspector.has_table(table_name):
            print(f"  - La tabla '{table_name}' ya existe. No se recreará.")
            return

        columns = []
        for col_name, dtype in df.dtypes.items():
            sqlalchemy_type = self._get_sqlalchemy_type(dtype)
            columns.append(Column(col_name, sqlalchemy_type, nullable=True))

        # Se define la tabla
        table = Table(table_name, self._metadata, *columns)
        # Se crea la tabla si no existe
        self._metadata.create_all(self._engine)
        print(f"  - Tabla '{table_name}' creada exitosamente.")

    def save_dataframe_to_db(self, df: pd.DataFrame, table_name: str):
        
        try:
            df.to_sql(table_name, self._engine, if_exists='append', index=False)
            print(f"  - Datos guardados exitosamente en la tabla '{table_name}'. Total de {len(df)} filas.")
        except SQLAlchemyError as e:
            print(f"  - Error al guardar datos en la tabla '{table_name}': {e}")
            raise

# Herencia y Polimorfismo

class CSVFileReader(AbstractFileReader):
    
    def read_file(self, filepath: str) -> pd.DataFrame:
        print(f"  - Leyendo archivo CSV: {os.path.basename(filepath)}")
        return pd.read_csv(filepath)

class ExcelFileReader(AbstractFileReader):
      
    def read_file(self, filepath: str) -> pd.DataFrame:
        print(f"  - Leyendo archivo XLSX: {os.path.basename(filepath)}")
        return pd.read_excel(filepath)

class JSONFileReader(AbstractFileReader):
    
    def read_file(self, filepath: str) -> pd.DataFrame:
        print(f"  - Leyendo archivo JSON: {os.path.basename(filepath)}")
        return pd.read_json(filepath)

class TextFileReader(AbstractFileReader):

    def __init__(self, delimiter: str = ','):
        
        self.delimiter = delimiter

    def read_file(self, filepath: str) -> pd.DataFrame:
        print(f"  - Leyendo archivo TXT: {os.path.basename(filepath)}")
        try:
            df = pd.read_csv(filepath, sep=self.delimiter)
            if len(df.columns) > 1 or (len(df.columns) == 1 and self.delimiter != ',' and df.columns[0].strip() != ''):
                 return df
            else:
                with open(filepath, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                return pd.DataFrame({'content': [line.strip() for line in lines]})
        except Exception:
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            return pd.DataFrame({'content': [line.strip() for line in lines]})

# Mapea extensiones de archivo.
FILE_READERS = {
    '.csv': CSVFileReader(),
    '.xlsx': ExcelFileReader(),
    '.json': JSONFileReader(),
    '.txt': TextFileReader(delimiter=','),
}

# Validación
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
    'notes': {
        'required_fields': ['content'],
        'data_types': {'content': str}
    }
}