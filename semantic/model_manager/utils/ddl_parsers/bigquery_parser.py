"""
BigQuery DDL 파서
"""

import re
from typing import Dict, List, Optional, Tuple
from semantic.model_manager.utils.ddl_parsers.base_parser import BaseDDLParser
from semantic.model_manager.utils.ddl_types import TableInfo, ColumnInfo, ForeignKeyInfo


class BigQueryParser(BaseDDLParser):
    """BigQuery DDL 파서"""
    
    def get_table_pattern(self) -> re.Pattern:
        """BigQuery CREATE TABLE 패턴"""
        # BigQuery: CREATE TABLE `project.dataset.table` 또는 CREATE TABLE `dataset.table`
        return re.compile(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+`([^`]+)`\s*\(',
            re.IGNORECASE | re.MULTILINE
        )
    
    def parse_table_name(self, table_match: re.Match) -> Tuple[Optional[str], Optional[str], str]:
        """
        BigQuery 테이블명 파싱
        형식: `project.dataset.table` 또는 `dataset.table` 또는 `table`
        """
        full_name = table_match.group(1).strip('`')
        parts = full_name.split('.')
        
        if len(parts) == 3:
            # project.dataset.table
            return parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            # dataset.table
            return None, parts[0], parts[1]
        else:
            # table만
            return None, None, parts[0]
    
    def parse_keys(self, table_block: str, table_info: TableInfo) -> None:
        """PRIMARY KEY 파싱 (BigQuery는 FOREIGN KEY를 지원하지 않음)"""
        lines = table_block.split('\n')
        
        # PRIMARY KEY 패턴: PRIMARY KEY (`col1`, `col2`, ...) NOT ENFORCED
        pk_pattern = re.compile(
            r'PRIMARY\s+KEY\s*\(([^)]+)\)',
            re.IGNORECASE
        )
        
        for line in lines:
            stripped_line = line.strip()
            if not stripped_line or stripped_line.startswith('--'):
                continue
            
            pk_match = pk_pattern.search(stripped_line)
            if pk_match:
                pk_cols = [col.strip().strip('`').strip('"') for col in pk_match.group(1).split(',')]
                table_info.primary_keys.extend(pk_cols)
    
    def parse_columns(self, table_block: str, table_info: TableInfo) -> None:
        """컬럼 정의 파싱"""
        lines = table_block.split('\n')
        
        # BigQuery 컬럼 패턴: `column_name` TYPE [NOT NULL] [OPTIONS(description="...")]
        # 백틱으로 감싼 컬럼명을 찾고, 그 다음 공백 이후부터 쉼표, NOT NULL, OPTIONS 전까지를 타입으로 추출
        column_name_pattern = re.compile(
            r'^[\s\t]*`([^`]+)`',
            re.IGNORECASE
        )
        
        for line in lines:
            stripped_line = line.strip()
            if not stripped_line or stripped_line.startswith('--'):
                continue
            
            # PRIMARY KEY 라인은 스킵
            if 'PRIMARY KEY' in stripped_line.upper():
                continue
            
            # OPTIONS 라인은 스킵
            if stripped_line.startswith('OPTIONS('):
                continue
            
            col_name_match = column_name_pattern.match(stripped_line)
            if col_name_match:
                col_name = col_name_match.group(1).strip()
                
                # 컬럼명 이후 부분에서 타입 추출
                # `column_name` 다음 부분을 찾음
                after_name = stripped_line[col_name_match.end():].strip()
                
                # 타입은 공백 이후부터 쉼표, NOT NULL, OPTIONS 전까지
                # 타입 패턴: 알파벳, 숫자, 언더스코어, 괄호, 공백(타입에 공백이 있을 수 있음)
                # 쉼표, NOT NULL, OPTIONS 전까지 추출
                type_match = re.match(
                    r'^([A-Za-z0-9_()]+(?:\s+[A-Za-z0-9_()]+)*?)(?:\s*,\s*|\s+(?:NOT\s+NULL|OPTIONS)|$)',
                    after_name,
                    re.IGNORECASE
                )
                
                if type_match:
                    col_type = type_match.group(1).strip()
                    
                    # NULL/NOT NULL 확인
                    nullable = 'NOT NULL' not in stripped_line.upper()
                    
                    # OPTIONS에서 description 추출
                    col_comment = None
                    options_match = re.search(
                        r'OPTIONS\s*\(\s*description\s*=\s*["\']([^"\']+)["\']',
                        stripped_line,
                        re.IGNORECASE
                    )
                    if options_match:
                        col_comment = options_match.group(1)
                    
                    table_info.columns.append(ColumnInfo(
                        name=col_name,
                        type=col_type,
                        nullable=nullable,
                        comment=col_comment
                    ))
    
    def parse_comments(self, ddl_text: str, table_name: str, table_info: TableInfo) -> None:
        """BigQuery는 OPTIONS에서 주석을 처리하므로 여기서는 추가 작업 없음"""
        pass
