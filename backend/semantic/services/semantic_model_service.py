import asyncio
from pathlib import Path
from typing import Dict, Any

from backend.semantic.model_manager.parser.semantic_parser import assemble_manifest, write_manifest
from backend.semantic.model_manager.linter.semantic_linter import lint_semantic_models
from backend.semantic.model_manager.utils.ddl_parser import parse_ddl
from backend.semantic.model_manager.draft.draft_generator import generate_draft
from backend.dto.semantic_model_dto import DraftResponse
from backend.utils.logger import setup_logger


logger = setup_logger("semantic_model_service")

async def semantic_parse_service(path: str) -> bool:
    """
    semantic_parser.py를 사용하여 semantic model을 파싱하는 서비스입니다.
    semantic_manifest.json을 생성합니다.
    """
    # 절대경로를 Path 객체로 변환
    semantic_path = Path(path)

    try:
        base_dir = str(semantic_path)

        logger.info("semantic_parser를 사용하여 파싱 시작: %s", base_dir)

        # semantic_parser의 assemble_manifest 함수 사용
        loop = asyncio.get_running_loop()
        manifest = await loop.run_in_executor(None, lambda: assemble_manifest(base_dir))

        # semantic_manifest.json 파일 생성
        out_path = str(semantic_path / "semantic_manifest.json")

        await loop.run_in_executor(None, lambda: write_manifest(manifest, out_path))

        logger.info("semantic_parser 파싱 성공: %s", out_path)
        return True

    except Exception as e:
        logger.error("semantic_parser 파싱 중 오류 발생: %s", str(e))
        raise e


async def semantic_lint_service(path: str) -> Dict[str, Any]:
    """
    semantic model 정적 검사를 수행하는 서비스입니다.

    TODO:
        - sources.yml / semantic_models / ddl.sql을 읽어 정적 검사 수행
        - 파싱 단계에서 이미 수행하는 검증 + 추가 린트 규칙 적용
    """
    logger.info("semantic lint 요청: %s", path)

    # CPU 바운드 작업이므로 parse와 마찬가지로 executor에서 실행
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, lambda: lint_semantic_models(path))
    return result


async def draft_service(path: str) -> DraftResponse:
    """
    DDL SQL 파일을 기반으로 sources.yml과 semantic_models 파일들의 초안을 생성하는 서비스입니다.
    """
    logger.info("draft 생성 요청: %s", path)
    
    # 절대경로를 Path 객체로 변환
    semantic_path = Path(path)
    ddl_path = semantic_path / "ddl.sql"
    
    if not ddl_path.exists():
        raise FileNotFoundError(f"ddl.sql not found at: {ddl_path}")
    
    # CPU 바운드 작업이므로 executor에서 실행
    loop = asyncio.get_running_loop()
    
    # DDL 파싱
    parsed_tables = await loop.run_in_executor(None, lambda: parse_ddl(str(ddl_path)))
    
    # Draft 생성
    draft_response = await loop.run_in_executor(
        None,
        lambda: generate_draft(parsed_tables, source_name="default", ddl_path=str(ddl_path))
    )
    
    logger.info("draft 생성 완료: %d개 파일 제안", len(draft_response.proposals))
    return draft_response

