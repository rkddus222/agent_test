"""
Semantic Model DTO 정의
"""
from typing import List, Optional
from dataclasses import dataclass
from pydantic import BaseModel

@dataclass
class Range:
    """파일 편집 범위"""
    start_line: int
    end_line: int
    start_column: int
    end_column: int

@dataclass
class Edit:
    """파일 편집 내용"""
    old_text: str
    new_text: str
    range: Optional[Range] = None

@dataclass
class Proposal:
    """파일 편집 제안"""
    file: str
    edits: List[Edit]

@dataclass
class DraftResponse:
    """Draft 생성 응답"""
    path: str = ""
    generatedAt: str = ""
    proposals: List[Proposal] = None
    
    def __post_init__(self):
        if self.proposals is None:
            self.proposals = []


class SemanticModelPathRequest(BaseModel):
    """Semantic Model 경로 요청"""
    path: str