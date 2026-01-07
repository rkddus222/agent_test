"""
MySQL/MariaDB DDL 파서
"""

import re
from typing import Dict, List, Optional, Tuple
from semantic.model_manager.utils.ddl_parsers.base_parser import BaseDDLParser
from semantic.model_manager.utils.ddl_types import TableInfo, ColumnInfo, ForeignKeyInfo


class MySQLParser(BaseDDLParser):
    """MySQL/MariaDB DDL 파서"""
    
    def get_table_pattern(self) -> re.Pattern:
        """MySQL CREATE TABLE 패턴"""
        # MySQL: CREATE TABLE `table` 또는 CREATE TABLE table
        return re.compile(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:`([\w]+)`|([\w]+))\s*\(',
            re.IGNORECASE | re.MULTILINE
        )
    
    def parse_table_name(self, table_match: re.Match) -> Tuple[Optional[str], Optional[str], str]:
        """
        MySQL 테이블명 파싱
        형식: `table` 또는 table
        """
        # group(1)은 백틱이 있는 경우, group(2)는 없는 경우
        table_name = table_match.group(1) or table_match.group(2)
        return None, None, table_name.strip('`')
    
    def parse_keys(self, table_block: str, table_info: TableInfo) -> None:
        """PRIMARY KEY 파싱"""
        lines = table_block.split('\n')
        
        # PRIMARY KEY 패턴: PRIMARY KEY (`col1`, `col2`, ...)
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
        
        # MySQL 컬럼 패턴: `column_name` TYPE [CHARACTER SET ...] [COLLATE ...] [NOT NULL] [DEFAULT ...] [COMMENT '...']
        # 한글 컬럼명 처리를 위해 컬럼명과 타입을 분리하여 추출
        column_name_pattern = re.compile(
            r'^[\s\t]*`([^`]+)`',
            re.IGNORECASE
        )
        
        for line in lines:
            stripped_line = line.strip()
            if not stripped_line or stripped_line.startswith('--'):
                continue
            
            # PRIMARY KEY, ENGINE, DEFAULT CHARSET 등은 스킵
            if any(keyword in stripped_line.upper() for keyword in ['PRIMARY KEY', 'ENGINE=', 'DEFAULT CHARSET', 'COLLATE=']):
                continue
            
            # 컬럼명 추출
            col_name_match = column_name_pattern.match(stripped_line)
            if col_name_match:
                col_name = col_name_match.group(1).strip('`')
                
                # 컬럼명 이후 부분에서 타입 추출
                after_name = stripped_line[col_name_match.end():].strip()
                
                # 타입은 공백 이후부터 쉼표, NOT NULL, DEFAULT, COMMENT, CHARACTER SET, COLLATE 전까지
                type_match = re.match(
                    r'^([A-Za-z0-9_()]+(?:\s+[A-Za-z0-9_()]+)*?)(?:\s*,\s*|\s+(?:NOT\s+NULL|DEFAULT|COMMENT|CHARACTER\s+SET|COLLATE)|$)',
                    after_name,
                    re.IGNORECASE
                )
                
                if type_match:
                    col_type = type_match.group(1).strip()
                    
                    # 타입에서 CHARACTER SET, COLLATE, DEFAULT, COMMENT 제거 (혹시 모를 경우)
                    col_type = re.sub(
                        r'\s+(?:CHARACTER\s+SET|COLLATE|DEFAULT|COMMENT).*$',
                        '',
                        col_type,
                        flags=re.IGNORECASE
                    )
                    col_type = col_type.rstrip(',').strip()
                
                    # NULL/NOT NULL 확인
                    nullable = 'NOT NULL' not in stripped_line.upper()
                    
                    # COMMENT 추출
                    col_comment = None
                    comment_match = re.search(
                        r"COMMENT\s+['\"]([^'\"]+)['\"]",
                        stripped_line,
                        re.IGNORECASE
                    )
                    if comment_match:
                        col_comment = comment_match.group(1)
                    
                    table_info.columns.append(ColumnInfo(
                        name=col_name,
                        type=col_type,
                        nullable=nullable,
                        comment=col_comment
                    ))
    
    def parse_comments(self, ddl_text: str, table_name: str, table_info: TableInfo) -> None:
        """MySQL은 컬럼 정의 내 COMMENT를 사용하므로 여기서는 추가 작업 없음"""
        pass
