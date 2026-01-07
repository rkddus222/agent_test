"""
Draft 생성 모듈
DDL 파싱 결과를 기반으로 sources.yml과 semantic_models 파일들을 생성합니다.
"""

import yaml
import re
from datetime import datetime, timezone
from typing import Dict, List
from utils.logger import setup_logger

logger = setup_logger("draft_generator")

from semantic.model_manager.utils.ddl_types import TableInfo
from semantic.model_manager.draft.type_mapping import map_db_type_to_data_type, is_numeric_type, DataType
from semantic.model_manager.draft.name_converter import (
    snake_to_camel,
    to_model_filename,
    to_model_name
)
from dto.semantic_model_dto import DraftResponse, Proposal, Edit, Range


def generate_draft(
    parsed_tables: Dict[str, TableInfo],
    source_name: str = "default",
    ddl_path: str = ""
) -> DraftResponse:
    """
    파싱된 테이블 정보를 기반으로 sources.yml과 semantic_models 파일들을 생성합니다.
    
    Args:
        parsed_tables: 테이블명을 키로 하는 TableInfo 딕셔너리
        source_name: source 이름 (기본값: "default")
        ddl_path: DDL 파일 경로 (path 필드에 사용)
        
    Returns:
        DraftResponse 객체
    """
    proposals: List[Proposal] = []
    
    # 1. sources.yml 생성
    sources_proposal = _generate_sources_yml(parsed_tables, source_name)
    proposals.append(sources_proposal)
    logger.info("sources.yml 생성 완료")
    
    # 2. 각 테이블별 semantic model 파일 생성
    for table_name, table_info in parsed_tables.items():
        model_proposal = _generate_semantic_model(table_name, table_info, source_name)
        proposals.append(model_proposal)
    
    logger.info(f"semantic model {len(parsed_tables)}개 생성 완료")
    
    # 3. DraftResponse 생성
    return DraftResponse(
        path=ddl_path,
        generatedAt=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        proposals=proposals
    )


def _generate_sources_yml(
    parsed_tables: Dict[str, TableInfo],
    source_name: str
) -> Proposal:
    """
    sources.yml 파일을 생성합니다.
    """
    # database와 schema 추출 (공통된 것이 있으면 사용)
    databases = set()
    schemas = set()
    
    for table_info in parsed_tables.values():
        if table_info.database:
            databases.add(table_info.database)
        if table_info.schema:
            schemas.add(table_info.schema)
    
    # database와 schema 결정
    database = list(databases)[0] if len(databases) == 1 else ""
    schema = list(schemas)[0] if len(schemas) == 1 else ""
    
    # sources.yml 구조 생성
    sources_data = {
        "sources": [
            {
                "name": source_name,
                "database": database,
                "schema": schema,
                "tables": [
                    {"name": table_name}
                    for table_name in sorted(parsed_tables.keys())
                ]
            }
        ]
    }
    
    # YAML 문자열 생성
    yaml_str = yaml.dump(
        sources_data,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
        indent=2
    )
    
    # Proposal 생성
    return Proposal(
        file="sources.yml",
        edits=[
            Edit(
                type="insert",
                range=Range(
                    startLine=0,
                    startCharacter=0,
                    endLine=0,
                    endCharacter=0
                ),
                newText=yaml_str
            )
        ]
    )


def _generate_semantic_model(
    table_name: str,
    table_info: TableInfo,
    source_name: str
) -> Proposal:
    """
    단일 테이블에 대한 semantic model 파일을 생성합니다.
    """
    model_name = to_model_name(table_name)
    filename = to_model_filename(table_name)
    
    # semantic_models 섹션 생성
    semantic_model = {
        "name": model_name,
        "table": f"{source_name}('{table_name}')",
        "description": table_info.comment or "",
    }
    
    # entities 생성
    entities = []
    
    # Primary key entities
    for pk_col in table_info.primary_keys:
        entity_name = _to_entity_name(pk_col)
        entities.append({
            "name": entity_name,
            "type": "primary",
            "expr": pk_col
        })
    
    # Foreign key entities (DDL에 FK 제약이 있는 경우만)
    for fk_info in table_info.foreign_keys:
        entity_name = _to_entity_name(fk_info.column)
        entities.append({
            "name": entity_name,
            "type": "foreign",
            "expr": fk_info.column
        })
    
    if entities:
        semantic_model["entities"] = entities
    
    # dimensions와 measures 분리
    dimensions = []
    measures = []
    
    for col in table_info.columns:
        # Primary key나 Foreign key는 dimensions/measures에 포함하지 않음
        if col.name in table_info.primary_keys:
            continue
        if any(fk.column == col.name for fk in table_info.foreign_keys):
            continue
        
        col_name_camel = snake_to_camel(col.name)
        data_type = map_db_type_to_data_type(col.type)
        is_numeric = is_numeric_type(data_type)
        
        col_dict = {
            "name": col_name_camel,
            "type": data_type,
            "label": "",
            "description": col.comment or "",
            "expr": col.name
        }
        
        # type_params는 생성하지 않음 (요구사항에 따라)
        
        if is_numeric:
            measures.append(col_dict)
        else:
            dimensions.append(col_dict)
    
    if dimensions:
        semantic_model["dimensions"] = dimensions
    
    if measures:
        semantic_model["measures"] = measures
    
    # 최종 YAML 구조
    yaml_data = {
        "semantic_models": [semantic_model],
        "metrics": []
    }
    
    # YAML 문자열 생성
    yaml_str = yaml.dump(
        yaml_data,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
        indent=2
    )
    
    # 빈 description과 label 제거
    for dim in semantic_model.get("dimensions", []):
        if not dim.get("description"):
            dim.pop("description", None)
        if not dim.get("label"):
            dim.pop("label", None)
    
    for measure in semantic_model.get("measures", []):
        if not measure.get("description"):
            measure.pop("description", None)
        if not measure.get("label"):
            measure.pop("label", None)
    
    # YAML 문자열 생성
    yaml_str = yaml.dump(
        yaml_data,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
        indent=2
    )
    
    # metrics: []를 metrics:로 변경 (빈 배열이 아닌 빈 섹션)
    yaml_str = re.sub(r'metrics:\s*\[\]', 'metrics:', yaml_str)
    
    # Proposal 생성
    return Proposal(
        file=f"semantic_models/{filename}",
        edits=[
            Edit(
                type="insert",
                range=Range(
                    startLine=0,
                    startCharacter=0,
                    endLine=0,
                    endCharacter=0
                ),
                newText=yaml_str
            )
        ]
    )


def _to_entity_name(column_name: str) -> str:
    """
    컬럼명을 entity name으로 변환합니다.
    예: card_aply_sn -> cardAply
    """
    camel = snake_to_camel(column_name)
    # 마지막 단어 제거 (일반적으로 _sn, _cd 등)
    # 간단하게 마지막 단어를 제거하는 방식
    parts = column_name.split('_')
    if len(parts) > 1:
        # 마지막 단어 제외
        base_parts = parts[:-1]
        return snake_to_camel('_'.join(base_parts))
    return camel
