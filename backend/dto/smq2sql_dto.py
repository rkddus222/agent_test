"""
SMQ to SQL DTO 정의
"""
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel


class SmqRequest(BaseModel):
    """SMQ 요청 내부 구조"""
    metrics: List[str]
    group_by: Optional[List[str]] = None
    filters: Optional[List[str]] = None
    order_by: Optional[List[str]] = None
    limit: Optional[int] = None
    joins: Optional[List[Dict[str, Any]]] = None


class ColumnMetadata(BaseModel):
    """컬럼 메타데이터"""
    name: str
    type: Optional[str] = None
    description: Optional[str] = None


class QueryResult(BaseModel):
    """쿼리 결과"""
    query: str
    metadata: List[ColumnMetadata]


class SmqToSqlRequest(BaseModel):
    """SMQ to SQL 변환 요청"""
    smq_request: SmqRequest
    manifest_content: Union[str, Dict[str, Any]]
    dialect: str = "bigquery"
    cte: bool = True


class SmqToSqlResponse(BaseModel):
    """SMQ to SQL 변환 응답"""
    success: bool
    results: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    queries: Optional[List[QueryResult]] = None
