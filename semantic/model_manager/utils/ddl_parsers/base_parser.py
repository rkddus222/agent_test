"""
DDL 파서 기본 클래스
각 DBMS별 파서가 상속받아 구현해야 하는 인터페이스를 정의합니다.
"""

import re
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple
from semantic.model_manager.utils.ddl_types import TableInfo, ColumnInfo, ForeignKeyInfo
from utils.logger import setup_logger

logger = setup_logger("ddl_parser")


class BaseDDLParser(ABC):
    """DDL 파서 기본 클래스"""
    
    def __init__(self, ddl_text: str):
        self.ddl_text = ddl_text
    
    @abstractmethod
    def parse_table_name(self, table_match: re.Match) -> Tuple[Optional[str], Optional[str], str]:
        """
        테이블명을 파싱하여 (database, schema, table_name) 튜플을 반환합니다.
        
        Args:
            table_match: CREATE TABLE 패턴 매치 객체
            
        Returns:
            (database, schema, table_name) 튜플
        """
        pass
    
    @abstractmethod
    def parse_keys(self, table_block: str, table_info: TableInfo) -> None:
        """
        PRIMARY KEY와 FOREIGN KEY를 파싱하여 table_info에 추가합니다.
        
        Args:
            table_block: 테이블 정의 블록 (CREATE TABLE ... ( ... ) 부분)
            table_info: 업데이트할 TableInfo 객체
        """
        pass
    
    @abstractmethod
    def parse_columns(self, table_block: str, table_info: TableInfo) -> None:
        """
        컬럼 정의를 파싱하여 table_info에 추가합니다.
        
        Args:
            table_block: 테이블 정의 블록
            table_info: 업데이트할 TableInfo 객체
        """
        pass
    
    @abstractmethod
    def parse_comments(self, ddl_text: str, table_name: str, table_info: TableInfo) -> None:
        """
        테이블 및 컬럼 주석을 파싱하여 table_info에 추가합니다.
        
        Args:
            ddl_text: 전체 DDL 텍스트
            table_name: 테이블명
            table_info: 업데이트할 TableInfo 객체
        """
        pass
    
    def find_table_block(self, table_start: int) -> Tuple[str, int]:
        """
        테이블 정의 블록을 찾아 반환합니다.
        
        Args:
            table_start: CREATE TABLE ... ( 이후 시작 위치
            
        Returns:
            (table_block, block_end) 튜플
        """
        paren_count = 1
        block_end = len(self.ddl_text)
        
        for i in range(table_start, len(self.ddl_text)):
            if self.ddl_text[i] == '(':
                paren_count += 1
            elif self.ddl_text[i] == ')':
                paren_count -= 1
                if paren_count == 0:
                    block_end = i
                    break
        
        table_block = self.ddl_text[table_start:block_end]
        return table_block, block_end
    
    def get_table_pattern(self) -> re.Pattern:
        """
        CREATE TABLE 패턴을 반환합니다.
        각 DBMS별로 오버라이드할 수 있습니다.
        """
        return re.compile(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+'
            r'(?:([\w]+)\.)?(?:([\w]+)\.)?([\w]+)\s*\(',
            re.IGNORECASE | re.MULTILINE
        )
    
    def parse_all_tables(self) -> Dict[str, TableInfo]:
        """
        DDL 텍스트에서 모든 테이블을 파싱합니다.
        
        Returns:
            테이블명을 키로 하는 TableInfo 딕셔너리
        """
        tables: Dict[str, TableInfo] = {}
        table_pattern = self.get_table_pattern()
        
        pos = 0
        while True:
            table_match = table_pattern.search(self.ddl_text, pos)
            if not table_match:
                break
            
            # 테이블명 파싱
            database, schema, table_name = self.parse_table_name(table_match)
            if not table_name:
                pos = table_match.end()
                continue
            
            # 테이블 정의 블록 찾기
            table_start = table_match.end()
            table_block, block_end = self.find_table_block(table_start)
            
            # TableInfo 생성
            table_info = TableInfo(
                name=table_name,
                database=database,
                schema=schema
            )
            
            # 키 파싱
            self.parse_keys(table_block, table_info)
            
            # 컬럼 파싱
            self.parse_columns(table_block, table_info)
            
            # 주석 파싱
            self.parse_comments(self.ddl_text, table_name, table_info)
            
            tables[table_name] = table_info
            pos = block_end + 1
        
        logger.info(f"테이블 {len(tables)}개 파싱 완료")
        return tables
