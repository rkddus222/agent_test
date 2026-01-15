from __future__ import annotations

import os
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Set, Tuple

from backend.semantic.model_manager.linter.field_schema import (
    SEMANTIC_MODEL_REQUIRED_FIELDS,
    SEMANTIC_MODEL_OPTIONAL_FIELDS,
    ENTITY_REQUIRED_FIELDS,
    ENTITY_OPTIONAL_FIELDS,
    DIMENSION_REQUIRED_FIELDS,
    DIMENSION_OPTIONAL_FIELDS,
    MEASURE_REQUIRED_FIELDS,
    MEASURE_OPTIONAL_FIELDS,
    METRIC_REQUIRED_FIELDS,
    METRIC_OPTIONAL_FIELDS,
)
from backend.semantic.model_manager.linter.lint_types import (
    SemanticLintIssue,
    make_warn,
    find_line_number,
)
from backend.utils.logger import setup_logger


logger = setup_logger("semantic_linter")

# 오타 검증 유사도 임계값
TYPO_THRESHOLD = 0.6


def _check_typo(
    actual: str, valid_fields: Set[str], threshold: float = TYPO_THRESHOLD
) -> Optional[str]:
    """
    유사도가 threshold 이상인 가장 유사한 필드명을 반환합니다.

    Args:
        actual: 실제 입력된 필드명
        valid_fields: 유효한 필드명 집합
        threshold: 유사도 임계값 (기본값: 0.6)

    Returns:
        유사도가 threshold 이상인 가장 유사한 필드명, 없으면 None
    """
    best_match = None
    best_ratio = 0.0

    for valid_field in valid_fields:
        ratio = SequenceMatcher(None, actual, valid_field).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = valid_field

    if best_ratio >= threshold:
        return best_match
    return None


def lint_field_typos_in_semantic_models(
    sm_contexts: List[Tuple[Dict[str, Any], str, List[str]]],
    base_dir: str,
) -> List[SemanticLintIssue]:
    """
    semantic model 내부 선택 필드명 오타를 검증합니다.
    - 선택 필드와 정확히 일치: OK
    - 선택 필드와 유사도가 높음: WARN + 오타 제안
    - 선택 필드와 유사도가 낮음: WARN (의도된 필드가 아님)
    """
    issues: List[SemanticLintIssue] = []

    for sm, file_path, lines in sm_contexts:
        model_name = sm.get("name", "unknown")
        rel_file = os.path.relpath(file_path, base_dir)

        # semantic model 레벨 필드 검증 (선택 필드만, 필수 필드는 건너뜀)
        for field_name in sm.keys():
            # 필수 필드는 yaml_field_linter.py에서 검증되므로 건너뜀
            if field_name in SEMANTIC_MODEL_REQUIRED_FIELDS:
                continue
            if field_name not in SEMANTIC_MODEL_OPTIONAL_FIELDS:
                suggested = _check_typo(field_name, SEMANTIC_MODEL_OPTIONAL_FIELDS)
                if suggested:
                    # 선택 필드와 유사한 경우: 오타 제안
                    line_no = find_line_number(lines, field_name, model_name) or 1
                    issues.append(
                        make_warn(
                            rel_file,
                            line_no,
                            "SEM502_TYPO_IN_FIELD_NAME",
                            (
                                f"Field '{field_name}' in semantic model '{model_name}' "
                                f"might be a typo. Did you mean: '{suggested}'?"
                            ),
                        )
                    )
                else:
                    # 선택 필드와 유사하지 않은 경우: 의도된 필드가 아님
                    line_no = find_line_number(lines, field_name, model_name) or 1
                    issues.append(
                        make_warn(
                            rel_file,
                            line_no,
                            "SEM503_INVALID_OPTIONAL_FIELD",
                            (
                                f"Field '{field_name}' in semantic model '{model_name}' "
                                f"is not a valid optional field."
                            ),
                        )
                    )

        # entities 필드 검증 (선택 필드만)
        entities = sm.get("entities") or []
        if isinstance(entities, list):
            for idx, ent in enumerate(entities):
                if not isinstance(ent, dict):
                    continue
                ent_name = ent.get("name", f"entity[{idx}]")
                for field_name in ent.keys():
                    # 필수 필드는 yaml_field_linter.py에서 검증되므로 건너뜀
                    if field_name in ENTITY_REQUIRED_FIELDS:
                        continue
                    if field_name not in ENTITY_OPTIONAL_FIELDS:
                        suggested = _check_typo(field_name, ENTITY_OPTIONAL_FIELDS)
                        if suggested:
                            # 선택 필드와 유사한 경우: 오타 제안
                            line_no = find_line_number(lines, field_name, ent_name) or 1
                            issues.append(
                                make_warn(
                                    rel_file,
                                    line_no,
                                    "SEM502_TYPO_IN_FIELD_NAME",
                                    (
                                        f"Field '{field_name}' in entity '{ent_name}' "
                                        f"of semantic model '{model_name}' might be a typo. "
                                        f"Did you mean: '{suggested}'?"
                                    ),
                                )
                            )
                        else:
                            # 선택 필드와 유사하지 않은 경우: 의도된 필드가 아님
                            line_no = find_line_number(lines, field_name, ent_name) or 1
                            issues.append(
                                make_warn(
                                    rel_file,
                                    line_no,
                                    "SEM503_INVALID_OPTIONAL_FIELD",
                                    (
                                        f"Field '{field_name}' in entity '{ent_name}' "
                                        f"of semantic model '{model_name}' is not a valid optional field."
                                    ),
                                )
                            )

        # dimensions 필드 검증 (선택 필드만)
        dimensions = sm.get("dimensions") or []
        if isinstance(dimensions, list):
            for idx, dim in enumerate(dimensions):
                if not isinstance(dim, dict):
                    continue
                dim_name = dim.get("name", f"dimension[{idx}]")
                for field_name in dim.keys():
                    # 필수 필드는 yaml_field_linter.py에서 검증되므로 건너뜀
                    if field_name in DIMENSION_REQUIRED_FIELDS:
                        continue
                    if field_name not in DIMENSION_OPTIONAL_FIELDS:
                        suggested = _check_typo(field_name, DIMENSION_OPTIONAL_FIELDS)
                        if suggested:
                            # 선택 필드와 유사한 경우: 오타 제안
                            line_no = find_line_number(lines, field_name, dim_name) or 1
                            issues.append(
                                make_warn(
                                    rel_file,
                                    line_no,
                                    "SEM502_TYPO_IN_FIELD_NAME",
                                    (
                                        f"Field '{field_name}' in dimension '{dim_name}' "
                                        f"of semantic model '{model_name}' might be a typo. "
                                        f"Did you mean: '{suggested}'?"
                                    ),
                                )
                            )
                        else:
                            # 선택 필드와 유사하지 않은 경우: 의도된 필드가 아님
                            line_no = find_line_number(lines, field_name, dim_name) or 1
                            issues.append(
                                make_warn(
                                    rel_file,
                                    line_no,
                                    "SEM503_INVALID_OPTIONAL_FIELD",
                                    (
                                        f"Field '{field_name}' in dimension '{dim_name}' "
                                        f"of semantic model '{model_name}' is not a valid optional field."
                                    ),
                                )
                            )

        # measures 필드 검증 (선택 필드만)
        measures = sm.get("measures") or []
        if isinstance(measures, list):
            for idx, ms in enumerate(measures):
                if not isinstance(ms, dict):
                    continue
                ms_name = ms.get("name", f"measure[{idx}]")
                for field_name in ms.keys():
                    # 필수 필드는 yaml_field_linter.py에서 검증되므로 건너뜀
                    if field_name in MEASURE_REQUIRED_FIELDS:
                        continue
                    if field_name not in MEASURE_OPTIONAL_FIELDS:
                        suggested = _check_typo(field_name, MEASURE_OPTIONAL_FIELDS)
                        if suggested:
                            # 선택 필드와 유사한 경우: 오타 제안
                            line_no = find_line_number(lines, field_name, ms_name) or 1
                            issues.append(
                                make_warn(
                                    rel_file,
                                    line_no,
                                    "SEM502_TYPO_IN_FIELD_NAME",
                                    (
                                        f"Field '{field_name}' in measure '{ms_name}' "
                                        f"of semantic model '{model_name}' might be a typo. "
                                        f"Did you mean: '{suggested}'?"
                                    ),
                                )
                            )
                        else:
                            # 선택 필드와 유사하지 않은 경우: 의도된 필드가 아님
                            line_no = find_line_number(lines, field_name, ms_name) or 1
                            issues.append(
                                make_warn(
                                    rel_file,
                                    line_no,
                                    "SEM503_INVALID_OPTIONAL_FIELD",
                                    (
                                        f"Field '{field_name}' in measure '{ms_name}' "
                                        f"of semantic model '{model_name}' is not a valid optional field."
                                    ),
                                )
                            )

    return issues


def lint_field_typos_in_metrics(
    metric_contexts: List[Tuple[Dict[str, Any], str, List[str]]],
    base_dir: str,
) -> List[SemanticLintIssue]:
    """
    metric 내부 선택 필드명 오타를 검증합니다.
    - 선택 필드와 정확히 일치: OK
    - 선택 필드와 유사도가 높음: WARN + 오타 제안
    - 선택 필드와 유사도가 낮음: WARN (의도된 필드가 아님)
    """
    issues: List[SemanticLintIssue] = []

    for mt, file_path, lines in metric_contexts:
        metric_name = mt.get("name", "unknown")
        rel_file = os.path.relpath(file_path, base_dir)

        for field_name in mt.keys():
            # 필수 필드는 yaml_field_linter.py에서 검증되므로 건너뜀
            if field_name in METRIC_REQUIRED_FIELDS:
                continue
            if field_name not in METRIC_OPTIONAL_FIELDS:
                suggested = _check_typo(field_name, METRIC_OPTIONAL_FIELDS)
                if suggested:
                    # 선택 필드와 유사한 경우: 오타 제안
                    line_no = find_line_number(lines, field_name, metric_name) or 1
                    issues.append(
                        make_warn(
                            rel_file,
                            line_no,
                            "SEM502_TYPO_IN_FIELD_NAME",
                            (
                                f"Field '{field_name}' in metric '{metric_name}' "
                                f"might be a typo. Did you mean: '{suggested}'?"
                            ),
                        )
                    )
                else:
                    # 선택 필드와 유사하지 않은 경우: 의도된 필드가 아님
                    line_no = find_line_number(lines, field_name, metric_name) or 1
                    issues.append(
                        make_warn(
                            rel_file,
                            line_no,
                            "SEM503_INVALID_OPTIONAL_FIELD",
                            (
                                f"Field '{field_name}' in metric '{metric_name}' "
                                f"is not a valid optional field."
                            ),
                        )
                    )

    return issues
