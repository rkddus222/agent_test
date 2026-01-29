"""
v2 버전 semantic_router
v3와의 차이점: semantic_parse_service가 v2 버전(파일 저장, bool 반환)을 사용
"""
from fastapi import APIRouter, HTTPException
import json
from backend.dto.smq2sql_dto import (
    SmqToSqlRequest,
    SmqToSqlResponse,
    ColumnMetadata,
    QueryResult,
)
from backend.dto.semantic_model_dto import SemanticModelPathRequest, DraftResponse

# v2 버전 서비스 사용 (현재는 v1과 동일하게 사용)
from backend.semantic.services.semantic_model_service import (
    semantic_parse_service,
    semantic_lint_service,
    draft_service,
)
from backend.semantic.services.smq2sql_service import prepare_smq_to_sql
from backend.utils.logger import setup_logger


logger = setup_logger("semantic_router_v2")
router = APIRouter(prefix="/semantic_model", tags=["semantic_model"])


@router.post("/parse")
async def semantic_parse_api(request: SemanticModelPathRequest):
    """
    semantic model 파싱 엔드포인트입니다.
    v2: 파일 시스템에 manifest를 저장하고 성공 여부를 반환
    """
    try:
        result = await semantic_parse_service(request.path)
        logger.info("Successfully parsed semantic model using semantic_parser")
        return {"message": "semantic 파싱 성공", "success": result}
    except Exception as e:
        logger.error("Failed to parse semantic layer: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/lint")
async def semantic_lint_api(request: SemanticModelPathRequest):
    """
    semantic model 정적 검사를 수행하는 엔드포인트입니다.
    """
    try:
        result = await semantic_lint_service(request.path)
        return result
    except Exception as e:
        logger.error("Failed to lint semantic model: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/draft")
async def draft_api(request: SemanticModelPathRequest):
    """
    DDL SQL 파일을 기반으로 sources.yml과 semantic_models 파일들의 초안을 생성하는 엔드포인트입니다.
    """
    try:
        result = await draft_service(request.path)
        logger.info("Successfully generated draft for path: %s", request.path)
        # dataclass를 dict로 변환하여 반환
        return {
            "path": result.path,
            "generatedAt": result.generatedAt,
            "proposals": [
                {
                    "file": p.file,
                    "edits": [
                        {
                            "old_text": e.old_text,
                            "new_text": e.new_text,
                            "range": {
                                "start_line": e.range.start_line,
                                "end_line": e.range.end_line,
                                "start_column": e.range.start_column,
                                "end_column": e.range.end_column,
                            } if e.range else None
                        }
                        for e in p.edits
                    ]
                }
                for p in result.proposals
            ]
        }
    except Exception as e:
        logger.error("Failed to generate draft: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/smq2sql")
async def smq_to_sql_api(request: SmqToSqlRequest) -> SmqToSqlResponse:
    """
    SQL 쿼리를 생성하는 엔드포인트입니다.
    """

    try:
        # DTO 객체를 딕셔너리로 변환
        smq_dict = {
            "metrics": request.smq_request.metrics,
            "group_by": request.smq_request.group_by,
            "filters": request.smq_request.filters,
            "order_by": request.smq_request.order_by,
            "limit": request.smq_request.limit,
            "joins": request.smq_request.joins,
        }

        logger.info(
            "Request parameters - dialect: %s, cte: %s", request.dialect, request.cte
        )
        logger.info("manifest_content: %s", str(request.manifest_content)[:10])
        result = prepare_smq_to_sql(
            smq=smq_dict,
            manifest_content=request.manifest_content,
            dialect=request.dialect,
            cte=request.cte,
        )

        # SQL 생성 결과 로깅
        if result.get("success") and result.get("results", {}).get("queries"):
            queries = result["results"]["queries"]
            for idx, query_result in enumerate(queries):
                logger.info("Query %d SQL: %s", idx + 1, query_result["query"])
                if query_result.get("metadata"):
                    logger.info(
                        "Query %d metadata (%d columns): %s",
                        idx + 1,
                        len(query_result["metadata"]),
                        json.dumps(query_result["metadata"], indent=2, ensure_ascii=False),
                    )
        else:
            logger.info("no SQL generated")

        # 각 쿼리의 메타데이터를 ColumnMetadata 객체로 변환하고 QueryResult로 래핑
        if result.get("results", {}).get("queries"):
            result["results"]["queries"] = [
                QueryResult(
                    query=query_result["query"],
                    metadata=[
                        ColumnMetadata(**meta) for meta in query_result["metadata"]
                    ],
                )
                for query_result in result["results"]["queries"]
            ]

        return SmqToSqlResponse(**result)
    except Exception as e:
        logger.error("Failed to convert smq to sql: %s", str(e))
        return SmqToSqlResponse(success=False, error=str(e))
