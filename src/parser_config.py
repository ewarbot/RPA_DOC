"""
Configuración de parsers para diferentes formatos de archivos TXT
"""

# Ejemplo de configuración para diferentes tipos de archivos
PARSERS_CONFIG = {
    "type1": {
        "delimiter": "|",
        "encoding": "utf-8",
        "fields": ["id", "nombre", "fecha", "monto"],
        "date_format": "%Y-%m-%d",
        "skip_lines": 1
    },
    "type2": {
        "delimiter": ";",
        "encoding": "latin-1",
        "fields": ["codigo", "descripcion", "cantidad"],
        "decimal_separator": ",",
        "header": False
    },
    "type3": {
        "fixed_width": [
            (0, 10),    # Campo 1
            (10, 30),   # Campo 2
            (30, 45)    # Campo 3
        ],
        "field_names": ["referencia", "cliente", "ubicacion"],
        "strip_spaces": True
    }
}

# Expresiones regulares para detectar el tipo de archivo
FILE_PATTERNS = {
    "type1": r"ventas_.*\.txt",
    "type2": r"inventario_.*\.txt",
    "type3": r"clientes_.*\.txt"
}

# Funciones personalizadas de transformación
def custom_currency_parser(value):
    """Ejemplo: Convertir $1,234.56 a 1234.56"""
    return float(value.replace('$', '').replace(',', ''))

def custom_date_parser(date_str):
    from datetime import datetime
    return datetime.strptime(date_str, "%d/%m/%Y").isoformat()

CUSTOM_PARSERS = {
    "monto": custom_currency_parser,
    "fecha": custom_date_parser
}

# Mapeo de tipos de datos
DATA_TYPE_MAP = {
    "id": int,
    "cantidad": int,
    "monto": float,
    "fecha": "datetime"
}