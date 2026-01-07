"""
Oracle DDL 파서
"""

import re
from typing import Dict, List, Optional, Tuple
from semantic.model_manager.utils.ddl_parsers.base_parser import BaseDDLParser
from semantic.model_manager.utils.ddl_types import TableInfo, ColumnInfo, ForeignKeyInfo


class OracleParser(BaseDDLParser):
    """Oracle DDL 파서 (PostgreSQL과 유사하지만 COMMENT ON COLUMN 사용)"""
    
    def parse_table_name(self, table_match: re.Match) -> Tuple[Optional[str], Optional[str], str]:
        """
        Oracle 테이블명 파싱
        형식: CREATE TABLE schema.table 또는 CREATE TABLE table
        """
        group1 = table_match.group(1)
        group2 = table_match.group(2)
        group3 = table_match.group(3)
        
        if not group3:
            return None, None, ""
        
        if group1 and group2:
            # 3개 부분: database.schema.table (드물지만 가능)
            return group1, group2, group3
        elif group1 and not group2:
            # 2개 부분: schema.table
            return None, group1, group3
        else:
            # 1개 부분: table만
            return None, None, group3
    
    def parse_keys(self, table_block: str, table_info: TableInfo) -> None:
        """PRIMARY KEY와 FOREIGN KEY 파싱"""
        lines = table_block.split('\n')
        
        # PRIMARY KEY 패턴: CONSTRAINT name PRIMARY KEY (col1, col2, ...)
        pk_pattern = re.compile(
            r'CONSTRAINT\s+[\w_]+\s+PRIMARY\s+KEY\s*\(([^)]+)\)',
            re.IGNORECASE
        )
        
        # FOREIGN KEY 패턴: CONSTRAINT name FOREIGN KEY (col) REFERENCES table(col)
        fk_pattern = re.compile(
            r'CONSTRAINT\s+[\w]+\s+FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+([\w]+)\s*\(([^)]+)\)',
            re.IGNORECASE
        )
        
        for line in lines:
            stripped_line = line.strip()
            if not stripped_line or stripped_line.startswith('--'):
                continue
            
            # PRIMARY KEY 확인
            pk_match = pk_pattern.search(stripped_line)
            if pk_match:
                pk_cols = [col.strip().strip('"') for col in pk_match.group(1).split(',')]
                table_info.primary_keys.extend(pk_cols)
                continue
            
            # FOREIGN KEY 확인
            fk_match = fk_pattern.search(stripped_line)
            if fk_match:
                fk_col = fk_match.group(1).strip().strip('"')
                ref_table = fk_match.group(2).strip()
                table_info.foreign_keys.append(ForeignKeyInfo(
                    column=fk_col,
                    references_table=ref_table
                ))
                continue
    
    def parse_columns(self, table_block: str, table_info: TableInfo) -> None:
        """컬럼 정의 파싱"""
        lines = table_block.split('\n')
        
        # Oracle 컬럼 패턴: column_name type [NULL|NOT NULL] [GENERATED ...]
        # 한글 컬럼명 처리를 위해 컬럼명과 타입을 분리하여 추출
        # 컬럼명은 따옴표로 감싸져 있을 수 있음: "column_name" 또는 column_name
        column_name_pattern = re.compile(
            r'^[\s\t]*"([^"]+)"|^[\s\t]*([^\s]+)',
            re.IGNORECASE
        )
        
        # CONSTRAINT 패턴 (스킵용)
        constraint_pattern = re.compile(
            r'CONSTRAINT',
            re.IGNORECASE
        )
        
        for line in lines:
            stripped_line = line.strip()
            if not stripped_line or stripped_line.startswith('--'):
                continue
            
            # CONSTRAINT가 포함된 라인은 스킵
            if constraint_pattern.search(stripped_line):
                continue
            
            # 컬럼명 추출
            col_name_match = column_name_pattern.match(stripped_line)
            if col_name_match:
                # 따옴표로 감싸진 경우와 아닌 경우 모두 처리
                col_name = col_name_match.group(1) or col_name_match.group(2)
                if not col_name:
                    continue
                col_name = col_name.strip().strip('"')
                
                # 컬럼명 이후 부분에서 타입 추출
                after_name = stripped_line[col_name_match.end():].strip()
                
                # 타입은 공백 이후부터 쉼표, NULL, NOT NULL, GENERATED 전까지
                type_match = re.match(
                    r'^([A-Za-z0-9_()]+(?:\s+[A-Za-z0-9_()]+)*?)(?:\s*,\s*|\s+(?:NULL|NOT\s+NULL|GENERATED)|$)',
                    after_name,
                    re.IGNORECASE
                )
                
                if type_match:
                    col_type = type_match.group(1).strip()
                    
                    # 타입에서 뒤에 오는 NULL/NOT NULL, GENERATED 제거 (혹시 모를 경우)
                    col_type = re.sub(
                        r'\s+(?:NULL|NOT\s+NULL|GENERATED).*$',
                        '',
                        col_type,
                        flags=re.IGNORECASE
                    )
                    col_type = col_type.rstrip(',').strip()
                
                    # NULL/NOT NULL 확인
                    nullable = 'NOT NULL' not in stripped_line.upper()
                    
                    # 컬럼 주석 확인 (같은 라인)
                    col_comment = None
                    if '--' in stripped_line:
                        comment_part = stripped_line.split('--', 1)[1].strip()
                        if comment_part:
                            col_comment = comment_part
                    
                    table_info.columns.append(ColumnInfo(
                        name=col_name,
                        type=col_type,
                        nullable=nullable,
                        comment=col_comment
                    ))
    
    def parse_comments(self, ddl_text: str, table_name: str, table_info: TableInfo) -> None:
        """테이블 및 컬럼 주석 파싱 (COMMENT ON COLUMN 사용)"""
        # 테이블 주석 패턴: -- [테이블 설명]: ...
        table_comment_pattern = re.compile(
            r'--\s*\[테이블\s*설명\]:\s*(.+)',
            re.IGNORECASE
        )
        
        # COMMENT ON COLUMN 패턴: COMMENT ON COLUMN schema.table.column IS '...'
        column_comment_pattern = re.compile(
            rf"COMMENT\s+ON\s+COLUMN\s+[\w\.]*\.?{re.escape(table_name)}\.([\w]+)\s+IS\s+['\"]([^'\"]+)['\"]",
            re.IGNORECASE
        )
        
        # 테이블 주석 찾기 (CREATE TABLE 전)
        table_match = re.search(
            rf"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+[\w\.]*\.?{re.escape(table_name)}",
            ddl_text,
            re.IGNORECASE
        )
        
        if table_match:
            comment_search_start = max(0, table_match.start() - 500)
            comment_text = ddl_text[comment_search_start:table_match.start()]
            comment_match = table_comment_pattern.search(comment_text)
            if comment_match:
                table_info.comment = comment_match.group(1).strip()
        
        # 컬럼 주석 찾기 (COMMENT ON COLUMN)
        for col_comment_match in column_comment_pattern.finditer(ddl_text):
            col_name = col_comment_match.group(1)
            col_comment = col_comment_match.group(2)
            
            # 해당 컬럼 찾아서 주석 업데이트
            for col in table_info.columns:
                if col.name == col_name:
                    col.comment = col_comment
                    break
