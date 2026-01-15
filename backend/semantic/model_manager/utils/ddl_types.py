"""
DDL 파싱에 사용되는 데이터 타입 정의
"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class ColumnInfo:
    """컬럼 정보를 담는 데이터클래스"""
    name: str
    type: str  # 원본 DB 타입
    nullable: bool
    comment: Optional[str] = None


@dataclass
class ForeignKeyInfo:
    """Foreign Key 정보를 담는 데이터클래스"""
    column: str
    references_table: Optional[str] = None  # 초기 버전에서는 None 가능


@dataclass
class TableInfo:
    """테이블 정보를 담는 데이터클래스"""
    name: str  # 테이블명
    database: Optional[str] = None
    schema: Optional[str] = None
    columns: List[ColumnInfo] = None
    primary_keys: List[str] = None
    foreign_keys: List[ForeignKeyInfo] = None
    comment: Optional[str] = None
    
    def __post_init__(self):
        if self.columns is None:
            self.columns = []
        if self.primary_keys is None:
            self.primary_keys = []
        if self.foreign_keys is None:
            self.foreign_keys = []
