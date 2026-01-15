"""
DB 타입을 DataType으로 매핑하는 모듈
"""

from typing import Literal
import re

DataType = Literal[
    "integer",
    "number",
    "float",
    "decimal",
    "varchar",
    "date",
    "datetime",
    "array",
    "map",
    "boolean"
]


def map_db_type_to_data_type(db_type: str) -> DataType:
    """
    DB 타입 문자열을 DataType으로 매핑합니다.
    
    매핑 규칙:
    - integer 타입: 
      * PostgreSQL: integer, int, int4, int8, smallint, bigint
      * MySQL: tinyint, smallint, mediumint, int, integer, bigint
      * Oracle: number(p) (소수점 없음)
      * BigQuery: int64, int32
      * SQL Server: tinyint, smallint, int, bigint
      * DuckDB: tinyint, smallint, integer, bigint, hugeint
    - number 타입 (소수점 포함):
      * PostgreSQL: numeric(p,s), decimal(p,s), float4, float8, float, double precision, real
      * MySQL: decimal, numeric, float, double, real
      * Oracle: number(p,s), float, binary_float, binary_double
      * BigQuery: numeric, float64, float32, bignumeric
      * SQL Server: decimal, numeric, float, real, money, smallmoney
      * Snowflake: number, decimal, numeric, float, float4, float8, double, real
      * DuckDB: decimal, numeric, real, double, float
    - varchar 타입: varchar, bpchar, char, text, character varying
    - date 타입: date
    - datetime 타입: timestamp, timestamptz, timestamp without time zone, timestamp with time zone
    - boolean 타입: boolean, bool
    - 기본값: varchar
    """
    # 공백 제거 및 소문자 변환
    normalized = db_type.strip().lower()
    
    # numeric(p,s), decimal(p,s), number(p,s) 형식 처리
    if normalized.startswith('numeric') or normalized.startswith('decimal') or normalized.startswith('number'):
        # numeric(15) 또는 numeric(15,2) 같은 경우
        if '(' in normalized:
            # 소수점이 있으면 number, 없으면 integer로 판단
            if ',' in normalized:
                return "number"
            else:
                # numeric(15) 같은 경우는 integer로 처리
                return "integer"
        else:
            # numeric만 있는 경우는 number
            return "number"
    
    # 정확한 매칭
    type_map = {
        # Integer types (PostgreSQL, MySQL, Oracle, SQL Server, DuckDB 등)
        "integer": "integer",
        "int": "integer",
        "int4": "integer",  # PostgreSQL
        "int8": "integer",  # PostgreSQL
        "int32": "integer",  # BigQuery
        "int64": "integer",  # BigQuery
        "smallint": "integer",
        "bigint": "integer",
        "tinyint": "integer",  # MySQL, SQL Server, DuckDB
        "mediumint": "integer",  # MySQL
        "hugeint": "integer",  # DuckDB
        
        # Floating point types
        "float": "number",
        "float4": "number",  # PostgreSQL, Snowflake
        "float8": "number",  # PostgreSQL, Snowflake
        "float32": "number",  # BigQuery
        "float64": "number",  # BigQuery
        "double": "number",
        "double precision": "number",  # PostgreSQL
        "real": "number",
        "binary_float": "number",  # Oracle
        "binary_double": "number",  # Oracle
        
        # Decimal/Numeric types (이미 위에서 처리되지만 명시적으로)
        "bignumeric": "number",  # BigQuery
        "money": "number",  # SQL Server
        "smallmoney": "number",  # SQL Server
        
        # String types
        "varchar": "varchar",
        "bpchar": "varchar",
        "char": "varchar",
        "text": "varchar",
        "character varying": "varchar",
        "character": "varchar",
        
        # Date types
        "date": "date",
        
        # DateTime types
        "timestamp": "datetime",
        "timestamptz": "datetime",
        "timestamp without time zone": "datetime",
        "timestamp with time zone": "datetime",
        
        # Boolean types
        "boolean": "boolean",
        "bool": "boolean",
    }
    
    if normalized in type_map:
        return type_map[normalized]
    
    # 타입명 포함 검사 (fallback)
    # 주의: 이 검사는 명시적으로 매핑되지 않은 타입에 대해서만 사용
    # "int"가 포함된 경우 (tinyint, mediumint, hugeint, int64 등)
    if "int" in normalized and "point" not in normalized:  # "point"는 geometry 타입
        return "integer"
    # "float", "double", "real"이 포함된 경우
    if "float" in normalized or "double" in normalized or "real" in normalized:
        return "number"
    # "numeric", "decimal", "number"가 포함된 경우 (이미 위에서 처리되지만 fallback)
    if "numeric" in normalized or "decimal" in normalized or "number" in normalized:
        return "number"
    if "char" in normalized or "text" in normalized or "varchar" in normalized:
        return "varchar"
    if "timestamp" in normalized:
        return "datetime"
    if "bool" in normalized:
        return "boolean"
    
    # 기본값
    return "varchar"


def is_numeric_type(data_type: DataType) -> bool:
    """
    DataType이 숫자 타입인지 확인합니다.
    숫자 타입: integer, number, float, decimal
    """
    return data_type in ("integer", "number", "float", "decimal")
