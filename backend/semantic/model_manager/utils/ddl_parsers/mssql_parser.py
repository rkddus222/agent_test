"""
Microsoft SQL Server (T-SQL) DDL 파서
"""

import re
from typing import Dict, List, Optional, Tuple
from backend.semantic.model_manager.utils.ddl_parsers.base_parser import BaseDDLParser
from backend.semantic.model_manager.utils.ddl_types import TableInfo, ColumnInfo, ForeignKeyInfo


class MsSQLParser(BaseDDLParser):
    """Microsoft SQL Server (T-SQL) DDL 파서"""
    
    def get_table_pattern(self) -> re.Pattern:
        """T-SQL CREATE TABLE 패턴"""
        # T-SQL: CREATE TABLE [database].[schema].[table] 또는 CREATE TABLE [schema].[table] 또는 CREATE TABLE [table]
        # 대괄호 또는 쌍따옴표로 식별자 구분
        return re.compile(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+'
            r'(?:\[([\w]+)\]|"([\w]+)")?\.?'
            r'(?:\[([\w]+)\]|"([\w]+)")?\.?'
            r'(?:\[([\w]+)\]|"([\w]+)"|([\w]+))\s*\(',
            re.IGNORECASE | re.MULTILINE
        )
    
    def parse_table_name(self, table_match: re.Match) -> Tuple[Optional[str], Optional[str], str]:
        """
        T-SQL 테이블명 파싱
        형식: [database].[schema].[table] 또는 [schema].[table] 또는 [table]
        """
        # group(1) 또는 group(2): database (대괄호 또는 쌍따옴표)
        # group(3) 또는 group(4): schema (대괄호 또는 쌍따옴표)
        # group(5), group(6), group(7): table (대괄호, 쌍따옴표, 또는 일반)
        
        database = table_match.group(1) or table_match.group(2)
        schema = table_match.group(3) or table_match.group(4)
        table = table_match.group(5) or table_match.group(6) or table_match.group(7)
        
        if not table:
            return None, None, ""
        
        # 대괄호나 쌍따옴표 제거
        database = database.strip('[]"') if database else None
        schema = schema.strip('[]"') if schema else None
        table = table.strip('[]"')
        
        return database, schema, table
    
    def parse_keys(self, table_block: str, table_info: TableInfo) -> None:
        """PRIMARY KEY와 FOREIGN KEY 파싱"""
        lines = table_block.split('\n')
        
        # PRIMARY KEY 패턴: CONSTRAINT name PRIMARY KEY (col1, col2, ...)
        pk_constraint_pattern = re.compile(
            r'CONSTRAINT\s+[\w_]+\s+PRIMARY\s+KEY\s*\(([^)]+)\)',
            re.IGNORECASE
        )
        
        # PRIMARY KEY 패턴: PRIMARY KEY (col1, col2, ...) - 인라인 정의
        pk_inline_pattern = re.compile(
            r'PRIMARY\s+KEY\s*\(([^)]+)\)',
            re.IGNORECASE
        )
        
        # FOREIGN KEY 패턴: CONSTRAINT name FOREIGN KEY (col) REFERENCES table(col)
        fk_pattern = re.compile(
            r'CONSTRAINT\s+[\w]+\s+FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+(?:\[([\w]+)\]|"([\w]+)"|([\w]+))\s*\(([^)]+)\)',
            re.IGNORECASE
        )
        
        for line in lines:
            stripped_line = line.strip()
            if not stripped_line or stripped_line.startswith('--'):
                continue
            
            # PRIMARY KEY 확인 (CONSTRAINT 형식 우선)
            pk_match = pk_constraint_pattern.search(stripped_line)
            if not pk_match:
                pk_match = pk_inline_pattern.search(stripped_line)
            
            if pk_match:
                # 대괄호나 쌍따옴표 제거
                pk_cols = [col.strip().strip('[]"') for col in pk_match.group(1).split(',')]
                table_info.primary_keys.extend(pk_cols)
                continue
            
            # FOREIGN KEY 확인
            fk_match = fk_pattern.search(stripped_line)
            if fk_match:
                fk_col = fk_match.group(1).strip().strip('[]"')
                ref_table = (fk_match.group(2) or fk_match.group(3) or fk_match.group(4)).strip().strip('[]"')
                table_info.foreign_keys.append(ForeignKeyInfo(
                    column=fk_col,
                    references_table=ref_table
                ))
                continue
    
    def parse_columns(self, table_block: str, table_info: TableInfo) -> None:
        """컬럼 정의 파싱"""
        lines = table_block.split('\n')
        
        # 컬럼 정의 패턴: [column_name] type [NULL|NOT NULL] [IDENTITY(1,1)] [DEFAULT ...]
        # 한글 컬럼명 처리를 위해 컬럼명과 타입을 분리하여 추출
        # 컬럼명은 대괄호, 쌍따옴표, 또는 일반 형식일 수 있음
        column_name_pattern = re.compile(
            r'^[\s\t]*\[([^\]]+)\]|^[\s\t]*"([^"]+)"|^[\s\t]*([^\s]+)',
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
            
            # CONSTRAINT가 포함된 라인은 스킵 (이미 parse_keys에서 처리)
            if constraint_pattern.search(stripped_line):
                continue
            
            # 컬럼명 추출
            col_name_match = column_name_pattern.match(stripped_line)
            if col_name_match:
                # 대괄호, 쌍따옴표, 또는 일반 형식 모두 처리
                col_name = col_name_match.group(1) or col_name_match.group(2) or col_name_match.group(3)
                if not col_name:
                    continue
                col_name = col_name.strip().strip('[]"')
                
                # 컬럼명 이후 부분에서 타입 추출
                after_name = stripped_line[col_name_match.end():].strip()
                
                # 타입은 공백 이후부터 쉼표, NULL, NOT NULL, IDENTITY, DEFAULT, CONSTRAINT 전까지
                type_match = re.match(
                    r'^([A-Za-z0-9_()]+(?:\s+[A-Za-z0-9_()]+)*?)(?:\s*,\s*|\s+(?:NULL|NOT\s+NULL|IDENTITY|DEFAULT|CONSTRAINT)|$)',
                    after_name,
                    re.IGNORECASE
                )
                
                if type_match:
                    col_type = type_match.group(1).strip()
                    
                    # 타입에서 IDENTITY, DEFAULT, NULL/NOT NULL 제거 (혹시 모를 경우)
                    col_type = re.sub(
                        r'\s+(?:IDENTITY\s*\([^)]+\)|DEFAULT|NULL|NOT\s+NULL).*$',
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
        """테이블 및 컬럼 주석 파싱"""
        # 테이블 주석 패턴: -- [테이블 설명]: ...
        table_comment_pattern = re.compile(
            r'--\s*\[테이블\s*설명\]:\s*(.+)',
            re.IGNORECASE
        )
        
        # T-SQL 주석 패턴: EXEC sp_addextendedproperty 'MS_Description', '...', 'SCHEMA', 'schema', 'TABLE', 'table'
        # 또는 간단한 형식: -- column comment
        column_comment_pattern = re.compile(
            rf"EXEC\s+sp_addextendedproperty\s+'MS_Description',\s+'([^']+)',\s+'SCHEMA',\s+'[\w]+',\s+'TABLE',\s+['\"]?{re.escape(table_name)}['\"]?,\s+'COLUMN',\s+['\"]?([\w]+)['\"]?",
            re.IGNORECASE
        )
        
        # 테이블 주석 찾기 (CREATE TABLE 전)
        table_match = re.search(
            rf"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+.*?{re.escape(table_name)}",
            ddl_text,
            re.IGNORECASE
        )
        
        if table_match:
            comment_search_start = max(0, table_match.start() - 500)
            comment_text = ddl_text[comment_search_start:table_match.start()]
            comment_match = table_comment_pattern.search(comment_text)
            if comment_match:
                table_info.comment = comment_match.group(1).strip()
        
        # 컬럼 주석 찾기 (sp_addextendedproperty)
        for col_comment_match in column_comment_pattern.finditer(ddl_text):
            col_name = col_comment_match.group(2)
            col_comment = col_comment_match.group(1)
            
            # 해당 컬럼 찾아서 주석 업데이트
            for col in table_info.columns:
                if col.name == col_name:
                    col.comment = col_comment
                    break
