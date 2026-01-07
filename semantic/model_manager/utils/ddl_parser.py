"""
다중 DBMS DDL 파싱 모듈
DDL SQL 파일을 파싱하여 테이블 정보를 추출합니다.
지원하는 dialect: mysql, postgres, sqlite, oracle, tsql, bigquery, snowflake, duckdb

**역할**: 정교한 DDL 파싱
- DBMS별 전용 파서 사용
- 테이블, 컬럼, 타입, 제약조건, 주석 등 상세 정보 추출
- draft_generator.py와 semantic_linter.py에서 사용
"""

import re
from typing import Dict, Optional, Set, Tuple
from pathlib import Path
from utils.logger import setup_logger

from semantic.model_manager.utils.ddl_types import TableInfo
from semantic.model_manager.utils.ddl_parsers import (
    PostgreSQLParser,
    BigQueryParser,
    MySQLParser,
    OracleParser,
    MsSQLParser
)

logger = setup_logger("ddl_parser")

# 지원하는 dialect 목록
SUPPORTED_DIALECTS = {
    'mysql', 'postgres', 'postgresql', 'sqlite', 'oracle', 
    'tsql', 'mssql', 'bigquery', 'snowflake', 'duckdb'
}


class DDLDialectError(Exception):
    """DDL 파일에 dialect 주석이 없거나 잘못된 경우 발생하는 예외"""
    pass


def parse_dialect_comment(ddl_text: str) -> str:
    """
    DDL 파일 첫머리에서 dialect 주석을 파싱합니다.
    
    형식: -- mysql 또는 -- postgres 등
    
    Args:
        ddl_text: DDL SQL 텍스트
        
    Returns:
        dialect 문자열 (예: 'mysql', 'postgres')
        
    Raises:
        DDLDialectError: dialect 주석이 없거나 지원하지 않는 dialect인 경우
    """
    # 첫 10줄 내에서 dialect 주석 찾기
    lines = ddl_text.split('\n')[:10]
    
    for line in lines:
        stripped = line.strip()
        # -- dialect 형식의 주석 찾기
        if stripped.startswith('--'):
            # -- 다음의 dialect 추출
            dialect_part = stripped[2:].strip()
            if dialect_part:
                dialect = dialect_part.lower()
                # 지원하는 dialect인지 확인
                if dialect in SUPPORTED_DIALECTS:
                    logger.info(f"DDL dialect 감지: {dialect}")
                    return dialect
                else:
                    raise DDLDialectError(
                        f"지원하지 않는 dialect: {dialect}. "
                        f"지원하는 dialect: {', '.join(sorted(SUPPORTED_DIALECTS))}"
                    )
    
    # dialect 주석이 없는 경우
    raise DDLDialectError(
        f"DDL 파일 첫머리에 dialect 주석이 없습니다. "
        f"다음 형식으로 dialect를 명시해주세요: -- mysql\n"
        f"지원하는 dialect: {', '.join(sorted(SUPPORTED_DIALECTS))}"
    )


def parse_ddl(ddl_path: str, dbms: Optional[str] = None) -> Dict[str, TableInfo]:
    """
    DDL SQL 파일을 파싱하여 테이블 정보를 추출합니다.
    
    Args:
        ddl_path: DDL SQL 파일 경로
        dbms: DBMS 타입 (사용하지 않음, DDL 파일 첫머리의 주석에서 자동 감지)
        
    Returns:
        테이블명을 키로 하는 TableInfo 딕셔너리
        
    Raises:
        FileNotFoundError: DDL 파일이 없는 경우
        DDLDialectError: dialect 주석이 없거나 지원하지 않는 dialect인 경우
    """
    if not Path(ddl_path).exists():
        raise FileNotFoundError(f"DDL file not found: {ddl_path}")
    
    logger.info(f"DDL 파일 발견: {ddl_path}")
    
    with open(ddl_path, 'r', encoding='utf-8') as f:
        ddl_text = f.read()
    
    # DDL 첫머리에서 dialect 주석 파싱
    dialect = parse_dialect_comment(ddl_text)
    
    # DDL 형태가 같은 dialect들은 같은 파서를 공유
    parser_map = {
        # PostgreSQL 계열
        'postgres': PostgreSQLParser,
        'postgresql': PostgreSQLParser,
        'duckdb': PostgreSQLParser,
        'sqlite': PostgreSQLParser,
        
        # BigQuery 계열
        'bigquery': BigQueryParser,
        'snowflake': BigQueryParser,
        
        # MySQL 계열
        'mysql': MySQLParser,
        
        # Oracle 계열
        'oracle': OracleParser,
        
        # SQL Server
        'mssql': MsSQLParser,
        'tsql': MsSQLParser,
    }
    
    parser_class = parser_map.get(dialect)
    if not parser_class:
        if dialect in parser_map:
            raise DDLDialectError(f"dialect '{dialect}'에 대한 파서가 아직 구현되지 않았습니다.")
        else:
            raise DDLDialectError(
                f"dialect '{dialect}'에 대한 파서가 구현되지 않았습니다. "
                f"현재 지원하는 dialect: {', '.join(sorted(k for k, v in parser_map.items() if v is not None))}"
            )
    
    logger.info(f"파서 선택: {parser_class.__name__} (dialect: {dialect})")
    
    # 파서 인스턴스 생성 및 파싱 실행
    parser = parser_class(ddl_text)
    tables = parser.parse_all_tables()
    
    return tables


def parse_ddl_tables(
    ddl_path: str,
) -> Tuple[Dict[Tuple[str, str, str], Set[str]], Dict[str, Set[str]]]:
    """
    DDL SQL 파일을 파싱하여 테이블별 컬럼 목록을 반환합니다.
    
    **역할**: 린터용 DDL 파싱 (parse_ddl()의 결과를 린터 형식으로 변환)
    - parse_ddl()을 사용하여 정교하게 파싱한 후 컬럼 목록만 추출
    - semantic_linter.py에서 사용
    
    Returns:
        (fully_qualified_map, short_name_map)
        - fully_qualified_map[(database, schema, table)] = {column_name, ...}
        - short_name_map[table] = {column_name, ...}
    """
    # parse_ddl()을 사용하여 정교하게 파싱
    tables = parse_ddl(ddl_path)
    
    fully_qualified_map: Dict[Tuple[str, str, str], Set[str]] = {}
    short_name_map: Dict[str, Set[str]] = {}
    
    for table_name, table_info in tables.items():
        # 컬럼 이름만 추출
        column_names = {col.name for col in table_info.columns}
        
        # fully_qualified_map에 추가
        db = table_info.database or ""
        schema = table_info.schema or ""
        key = (db, schema, table_name)
        fully_qualified_map[key] = column_names
        
        # short_name_map에 추가
        short_name_map.setdefault(table_name, set()).update(column_names)
    
    return fully_qualified_map, short_name_map
