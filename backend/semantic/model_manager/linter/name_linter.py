from __future__ import annotations

import os
from typing import Any, Dict, List, Set, Tuple

from backend.semantic.model_manager.parser.parsing_validation import (
    find_duplicate_names_in_semantic_model,
    find_duplicate_metric_names,
)
from backend.semantic.model_manager.linter.lint_types import (
    SemanticLintIssue,
    make_error,
    make_warn,
    find_line_number,
)
from backend.semantic.model_manager.draft.name_converter import snake_to_camel
from backend.utils.logger import setup_logger


logger = setup_logger("semantic_linter")


def lint_semantic_model_name_uniqueness(
    sm_contexts: List[Tuple[Dict[str, Any], str, List[str]]],
    base_dir: str,
) -> List[SemanticLintIssue]:
    """
    하나의 semantic model 내부에서 dimension / measure 이름의 유일성을 검증합니다.
    (semantic_parser.validate_semantic_model_names와 동일한 로직을 린트 형식으로 변환)
    """
    issues: List[SemanticLintIssue] = []

    for sm, file_path, lines in sm_contexts:
        model_name = sm.get("name")
        rel_file = os.path.relpath(file_path, base_dir)

        duplicate_dims, duplicate_measures, overlap = find_duplicate_names_in_semantic_model(sm)

        # 1) dimension 이름 중복 검사
        for name in duplicate_dims:
            line_no = find_line_number(lines, None, name) or 1
            issues.append(
                make_error(
                    rel_file,
                    line_no,
                    "SEM007_DUPLICATE_DIMENSION_NAME",
                    (
                        f"Duplicate dimension name '{name}' found "
                        f"in semantic model '{model_name}'."
                    ),
                )
            )

        # 2) measure 이름 중복 검사
        for name in duplicate_measures:
            line_no = find_line_number(lines, None, name) or 1
            issues.append(
                make_error(
                    rel_file,
                    line_no,
                    "SEM008_DUPLICATE_MEASURE_NAME",
                    (
                        f"Duplicate measure name '{name}' found "
                        f"in semantic model '{model_name}'."
                    ),
                )
            )

        # 3) dimension / measure 이름이 서로 겹치는지 검사
        for name in overlap:
            line_no = find_line_number(lines, None, name) or 1
            issues.append(
                make_error(
                    rel_file,
                    line_no,
                    "SEM009_DIMENSION_MEASURE_NAME_CONFLICT",
                    (
                        f"Dimension and measure share the same name '{name}' "
                        f"in semantic model '{model_name}'."
                    ),
                )
            )

    return issues


def lint_metric_uniqueness(
    metric_contexts: List[Tuple[Dict[str, Any], str, List[str]]],
    base_dir: str,
) -> List[SemanticLintIssue]:
    """
    Metric 이름의 전역 유일성을 검증합니다.
    (semantic_parser.validate_metric_uniqueness와 동일한 로직을 린트 형식으로 변환)
    """
    issues: List[SemanticLintIssue] = []

    metrics_only: List[Dict[str, Any]] = [mt for mt, _, _ in metric_contexts]
    duplicate_names: Set[str] = set(find_duplicate_metric_names(metrics_only))

    for mt, file_path, lines in metric_contexts:
        name = mt.get("name")
        if not name:
            continue
        if name in duplicate_names:
            rel_file = os.path.relpath(file_path, base_dir)
            line_no = find_line_number(lines, None, name) or 1
            issues.append(
                make_error(
                    rel_file,
                    line_no,
                    "SEM002_METRIC_DUPLICATE_NAME",
                    (
                        f"Metric name '{name}' is duplicated across metric definitions."
                    ),
                )
            )

    return issues


def extract_model_name_candidates(filename: str) -> List[str]:
    """
    파일명에서 semantic model name 후보들을 추출합니다.
    
    Args:
        filename: 파일명 (예: "card_aply.yml", "CoYmdInfoMModel.yml")
        
    Returns:
        가능한 semantic model name 후보 리스트
    """
    # 확장자 제거
    base_name = filename
    for ext in [".yml", ".yaml"]:
        if base_name.endswith(ext):
            base_name = base_name[:-len(ext)]
            break
    
    candidates = []
    
    # 1. 원본 그대로
    candidates.append(base_name)
    
    # 2. "Model" 접미사 제거 후 변환
    if base_name.endswith("Model"):
        without_model = base_name[:-5]  # "Model" 제거
        # PascalCase를 camelCase로 변환
        if without_model and without_model[0].isupper():
            camel_case = without_model[0].lower() + without_model[1:]
            candidates.append(camel_case)
    
    # 3. snake_case를 camelCase로 변환
    if "_" in base_name:
        camel_from_snake = snake_to_camel(base_name)
        candidates.append(camel_from_snake)
    
    # 4. 하이픈을 언더스코어로 변환 후 camelCase
    if "-" in base_name:
        snake_from_hyphen = base_name.replace("-", "_")
        camel_from_hyphen = snake_to_camel(snake_from_hyphen)
        candidates.append(camel_from_hyphen)
    
    # 중복 제거 및 정렬
    return sorted(list(set(candidates)))


def lint_filename_model_name_consistency(
    sm_contexts: List[Tuple[Dict[str, Any], str, List[str]]],
    base_dir: str,
) -> List[SemanticLintIssue]:
    """
    파일명과 semantic model의 name이 일치하는지 검사합니다.
    
    파일명에서 추출한 후보들과 semantic model name을 비교하여
    일치하지 않으면 WARN 레벨 이슈를 생성합니다.
    """
    issues: List[SemanticLintIssue] = []
    
    for sm, file_path, lines in sm_contexts:
        model_name = sm.get("name")
        if not model_name:
            continue
        
        # 파일명 추출
        filename = os.path.basename(file_path)
        candidates = extract_model_name_candidates(filename)
        
        # semantic model name과 일치하는 후보가 있는지 확인
        if model_name not in candidates:
            rel_file = os.path.relpath(file_path, base_dir)
            line_no = find_line_number(lines, None, model_name) or 1
            
            # 가장 유사한 후보 찾기 (선택적)
            best_match = None
            if candidates:
                # 정확히 일치하는 것은 아니지만, 가장 유사한 것 선택
                best_match = candidates[0]
            
            message = (
                f"Semantic model name '{model_name}' does not match filename '{filename}'. "
                f"Expected name based on filename: {', '.join(candidates[:3])}"
            )
            if best_match:
                message += f" (suggested: '{best_match}')"
            
            issues.append(
                make_warn(
                    rel_file,
                    line_no,
                    "SEM501_FILENAME_MODEL_NAME_MISMATCH",
                    message,
                )
            )
    
    return issues


def lint_foreign_entity_primary_match(
    sm_contexts: List[Tuple[Dict[str, Any], str, List[str]]],
    base_dir: str,
) -> List[SemanticLintIssue]:
    """
    type이 "foreign"인 entity의 name이 전체 semantic models의
    primary entity name과 매칭되는지 검증합니다.
    
    foreign entity의 name과 동일한 name을 가진 primary entity가
    전체 semantic models에 존재하지 않으면 경고를 발생시킵니다.
    """
    issues: List[SemanticLintIssue] = []
    
    # 전체 semantic models에서 primary entity 이름 집합 생성
    all_primary_entity_names: Set[str] = set()
    for sm, _, _ in sm_contexts:
        entities = sm.get("entities") or []
        for ent in entities:
            if not isinstance(ent, dict):
                continue
            ent_type = ent.get("type")
            if ent_type == "primary":
                ent_name = ent.get("name")
                if ent_name:
                    all_primary_entity_names.add(ent_name)
    
    # 각 semantic model의 foreign entity 검사
    for sm, file_path, lines in sm_contexts:
        model_name = sm.get("name")
        rel_file = os.path.relpath(file_path, base_dir)
        
        entities = sm.get("entities") or []
        for idx, ent in enumerate(entities):
            if not isinstance(ent, dict):
                continue
            ent_type = ent.get("type")
            if ent_type == "foreign":
                ent_name = ent.get("name", f"entity[{idx}]")
                if ent_name and ent_name not in all_primary_entity_names:
                    line_no = find_line_number(lines, None, ent_name) or 1
                    issues.append(
                        make_warn(
                            rel_file,
                            line_no,
                            "SEM502_FOREIGN_ENTITY_NO_PRIMARY_MATCH",
                            (
                                f"Foreign entity '{ent_name}' in semantic model '{model_name}' "
                                f"does not have a matching primary entity with the same name "
                                f"in any semantic model. This may cause join path generation to fail."
                            ),
                        )
                    )
    
    return issues
