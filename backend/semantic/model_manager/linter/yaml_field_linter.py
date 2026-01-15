from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Set, Tuple

import yaml

from backend.semantic.model_manager.parser.parsing_validation import (
    find_missing_required_fields_in_semantic_model,
)
from backend.semantic.model_manager.linter.field_schema import (
    VALID_TOP_LEVEL_FIELDS,
    VALID_METRIC_FIELDS,
)
from backend.semantic.model_manager.linter.field_typo_linter import (
    _check_typo,
)
from backend.semantic.model_manager.linter.lint_types import (
    SemanticLintIssue,
    make_error,
    find_line_number,
)
from backend.utils.logger import setup_logger


logger = setup_logger("semantic_linter")


def lint_top_level_field_names(
    sem_dir: str,
    base_dir: str,
) -> List[SemanticLintIssue]:
    """
    최상위 레벨 필드명 오타를 검사합니다.
    (semantic_models, metrics)
    """
    issues: List[SemanticLintIssue] = []

    if not os.path.isdir(sem_dir):
        return issues

    for fn in os.listdir(sem_dir):
        if not fn.endswith((".yml", ".yaml")):
            continue
        path = os.path.join(sem_dir, fn)
        rel_file = os.path.relpath(path, base_dir)
        
        try:
            with open(path, "r", encoding="utf-8") as f:
                text = f.read()
            data = yaml.safe_load(text) or {}
        except Exception as e:
            logger.error("Failed to parse yaml %s: %s", path, str(e))
            continue

        lines = text.splitlines()
        for key in data.keys():
            if key not in VALID_TOP_LEVEL_FIELDS:
                # 필드명이 있는 라인 찾기
                line_no = 1
                for idx, line in enumerate(lines, start=1):
                    if re.match(rf"^\s*{re.escape(key)}\s*:", line):
                        line_no = idx
                        break
                
                issues.append(
                    make_error(
                        rel_file,
                        line_no,
                        "SEM015_INVALID_TOP_LEVEL_FIELD",
                        (
                            f"Invalid top-level field name '{key}'. "
                            f"Valid fields are: {', '.join(sorted(VALID_TOP_LEVEL_FIELDS))}."
                        ),
                    )
                )

    return issues


def _get_actual_fields_from_context(
    sm: Dict[str, Any], context: str
) -> Set[str]:
    """
    context 정보로부터 실제 필드명 집합을 추출합니다.
    
    Args:
        sm: semantic model dict
        context: "semantic_model", "entity[0]", "dimension[1] (name)" 등
        
    Returns:
        실제 필드명 집합
    """
    if context == "semantic_model":
        return set(sm.keys())
    
    # context에서 인덱스 추출
    idx_match = re.search(r'\[(\d+)\]', context)
    if not idx_match:
        return set[str]()
    
    try:
        idx = int(idx_match.group(1))
    except (ValueError, IndexError):
        return set[str]()
    
    if context.startswith("entity"):
        entities = sm.get("entities") or []
        if isinstance(entities, list) and idx < len(entities):
            ent = entities[idx]
            if isinstance(ent, dict):
                return set[str](ent.keys())
    elif context.startswith("dimension"):
        dimensions = sm.get("dimensions") or []
        if isinstance(dimensions, list) and idx < len(dimensions):
            dim = dimensions[idx]
            if isinstance(dim, dict):
                return set[str](dim.keys())
    elif context.startswith("measure"):
        measures = sm.get("measures") or []
        if isinstance(measures, list) and idx < len(measures):
            ms = measures[idx]
            if isinstance(ms, dict):
                return set[str](ms.keys())
    
    return set[str]()


def lint_semantic_model_required_fields(
    sm_contexts: List[Tuple[Dict[str, Any], str, List[str]]],
    base_dir: str,
) -> List[SemanticLintIssue]:
    """
    semantic model의 필수 필드가 누락되었는지 검사합니다.
    필수 필드가 없을 경우, 실제 필드명 중에서 필수 필드와 유사한 것이 있는지 확인하여
    오타 제안을 포함합니다.
    """
    issues: List[SemanticLintIssue] = []

    for sm, file_path, lines in sm_contexts:
        model_name = sm.get("name", "unknown")
        rel_file = os.path.relpath(file_path, base_dir)

        missing_fields = find_missing_required_fields_in_semantic_model(sm)
        for field, context in missing_fields:
            # context에서 실제 이름이나 인덱스 추출
            if context == "semantic_model":
                line_no = find_line_number(lines, None, model_name) or 1
                search_term = model_name
            else:
                # entity[0], dimension[1] 등에서 실제 이름 추출 시도
                # context 형식: "entity[0]", "dimension[1] (name)", "measure[2] (name)" 등
                name_match = re.search(r'\(([^)]+)\)', context)
                search_term = name_match.group(1) if name_match else field
                line_no = find_line_number(lines, None, search_term) or 1
            
            # 실제 필드명들 중에서 필수 필드와 유사한 것이 있는지 확인
            actual_fields = _get_actual_fields_from_context(sm, context)
            suggested = _check_typo(field, actual_fields)
            
            if context == "semantic_model":
                if suggested:
                    message = (
                        f"Missing required field '{field}' in semantic model '{model_name}'. "
                        f"Did you mean: '{suggested}'?"
                    )
                else:
                    message = (
                        f"Missing required field '{field}' in semantic model '{model_name}'."
                    )
            else:
                if suggested:
                    message = (
                        f"Missing required field '{field}' in {context} "
                        f"of semantic model '{model_name}'. Did you mean: '{suggested}'?"
                    )
                else:
                    message = (
                        f"Missing required field '{field}' in {context} "
                        f"of semantic model '{model_name}'."
                    )
            
            issues.append(
                make_error(
                    rel_file,
                    line_no,
                    "SEM014_MISSING_REQUIRED_FIELD",
                    message,
                )
            )

    return issues


def lint_metric_field_names(
    metric_contexts: List[Tuple[Dict[str, Any], str, List[str]]],
    base_dir: str,
) -> List[SemanticLintIssue]:
    """
    metric 내부 필드명 오타를 검사합니다.
    """
    issues: List[SemanticLintIssue] = []

    for mt, file_path, lines in metric_contexts:
        metric_name = mt.get("name", "unknown")
        rel_file = os.path.relpath(file_path, base_dir)

        for key in mt.keys():
            if key not in VALID_METRIC_FIELDS:
                line_no = find_line_number(lines, key, metric_name) or 1
                issues.append(
                    make_error(
                        rel_file,
                        line_no,
                        "SEM020_INVALID_METRIC_FIELD",
                        (
                            f"Invalid field name '{key}' in metric '{metric_name}'. "
                            f"Valid fields are: {', '.join(sorted(VALID_METRIC_FIELDS))}."
                        ),
                    )
                )

    return issues
