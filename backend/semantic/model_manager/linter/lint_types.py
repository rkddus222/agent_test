from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Tuple, TypedDict

import os

from backend.semantic.model_manager.parser.parsing_validation import (
    find_invalid_types_in_semantic_model,
    get_invalid_metric_type,
    get_invalid_metric_data_type,
    get_invalid_entity_type,
    VALID_DATA_TYPES,
    VALID_METRIC_TYPES,
    VALID_ENTITY_TYPES,
)


# 현재는 ERROR, WARN 두 단계만 사용하지만
# 추후 INFO 등이 추가되면 여기서만 확장하면 됩니다.
Severity = Literal["ERROR", "WARN"]


class SemanticLintIssue(TypedDict):
    """
    semantic_linter가 개별 룰 위반을 보고할 때 사용하는 공통 포맷입니다.
    """

    severity: Severity
    file: str
    line: int
    code: str
    message: str


class SemanticLintResult(TypedDict):
    """
    lint_semantic_models의 최종 반환 형태를 명시합니다.
    """

    success: bool
    issues: List[SemanticLintIssue]
    error_count: int
    warning_count: int


def make_issue(
    severity: Severity,
    file: str,
    line: int,
    code: str,
    message: str,
) -> SemanticLintIssue:
    """
    개별 이슈 객체를 생성하는 헬퍼입니다.
    """

    return {
        "severity": severity,
        "file": file,
        "line": line,
        "code": code,
        "message": message,
    }


def make_error(file: str, line: int, code: str, message: str) -> SemanticLintIssue:
    """
    ERROR 레벨 이슈를 생성합니다.
    """

    return make_issue("ERROR", file, line, code, message)


def make_warn(file: str, line: int, code: str, message: str) -> SemanticLintIssue:
    """
    WARN 레벨 이슈를 생성합니다.
    """

    return make_issue("WARN", file, line, code, message)


def make_result(issues: List[SemanticLintIssue]) -> SemanticLintResult:
    """
    issues 리스트로부터 최종 lint 결과를 생성합니다.
    """

    error_count = sum(1 for i in issues if i["severity"] == "ERROR")
    warning_count = sum(1 for i in issues if i["severity"] == "WARN")

    return {
        "success": error_count == 0,
        "issues": issues,
        "error_count": error_count,
        "warning_count": warning_count,
    }


def find_line_number(
    lines: List[str],
    expr: Optional[str],
    name: Optional[str],
) -> Optional[int]:
    """
    expr가 포함된 라인, 없으면 name이 포함된 'name:' 라인을 찾아 라인 번호(1-based)를 반환합니다.
    """
    if expr:
        for idx, line in enumerate(lines, start=1):
            if expr in line:
                return idx
    if name:
        for idx, line in enumerate(lines, start=1):
            if "name:" in line and name in line:
                return idx
    return None


def lint_semantic_model_types(
    sm_contexts: List[Tuple[Dict[str, Any], str, List[str]]],
    base_dir: str,
) -> List[SemanticLintIssue]:
    """
    semantic model 내부 dimension / measure의 type 값(DataType)이
    허용된 값 범위 안에 있는지 검증합니다.

    (semantic_parser.validate_semantic_model_names 에서 수행하는
    type 검증과 동일한 로직을 린트 형식으로 표현)
    """
    issues: List[SemanticLintIssue] = []

    allowed_types = ", ".join(sorted(VALID_DATA_TYPES))

    for sm, file_path, lines in sm_contexts:
        model_name = sm.get("name")
        rel_file = os.path.relpath(file_path, base_dir)

        invalid_dims, invalid_measures = find_invalid_types_in_semantic_model(sm)

        # 4-1) dimension type 값 범위 검사
        for dim_name, invalid_type in invalid_dims:
            line_no = find_line_number(lines, str(invalid_type), dim_name) or 1
            issues.append(
                make_error(
                    rel_file,
                    line_no,
                    "SEM012_INVALID_DIMENSION_TYPE",
                    (
                        f"Dimension '{dim_name}' in semantic model '{model_name}' "
                        f"has invalid type '{invalid_type}'. "
                        f"Allowed DataType values are: {allowed_types}."
                    ),
                )
            )

        # 4-2) measure type 값 범위 검사
        for ms_name, invalid_type in invalid_measures:
            line_no = find_line_number(lines, str(invalid_type), ms_name) or 1
            issues.append(
                make_error(
                    rel_file,
                    line_no,
                    "SEM013_INVALID_MEASURE_TYPE",
                    (
                        f"Measure '{ms_name}' in semantic model '{model_name}' "
                        f"has invalid type '{invalid_type}'. "
                        f"Allowed DataType values are: {allowed_types}."
                    ),
                )
            )

    return issues


def lint_metric_type_enums(
    metric_contexts: List[Tuple[Dict[str, Any], str, List[str]]],
    base_dir: str,
) -> List[SemanticLintIssue]:
    """
    Metric 의 metric_type / type(DataType) 값이 허용된 enum 범위 안에 있는지 검증합니다.

    (semantic_parser.validate_metric_uniqueness 에서 수행하는 enum 검증과 동일한 로직)
    """
    issues: List[SemanticLintIssue] = []

    allowed_metric_types = ", ".join(sorted(VALID_METRIC_TYPES))
    allowed_data_types = ", ".join(sorted(VALID_DATA_TYPES))

    for mt, file_path, lines in metric_contexts:
        name = mt.get("name")
        if not name:
            continue

        rel_file = os.path.relpath(file_path, base_dir)

        invalid_mt = get_invalid_metric_type(mt)
        if invalid_mt is not None:
            line_no = find_line_number(lines, str(invalid_mt), name) or 1
            issues.append(
                make_error(
                    rel_file,
                    line_no,
                    "SEM003_INVALID_METRIC_TYPE",
                    (
                        f"Metric '{name}' has invalid metric_type '{invalid_mt}'. "
                        f"Allowed metric_type values are: {allowed_metric_types}."
                    ),
                )
            )

        invalid_dt = get_invalid_metric_data_type(mt)
        if invalid_dt is not None:
            line_no = find_line_number(lines, str(invalid_dt), name) or 1
            issues.append(
                make_error(
                    rel_file,
                    line_no,
                    "SEM004_INVALID_METRIC_DATA_TYPE",
                    (
                        f"Metric '{name}' has invalid type '{invalid_dt}'. "
                        f"Allowed DataType values are: {allowed_data_types}."
                    ),
                )
            )

    return issues


def lint_entity_types(
    sm_contexts: List[Tuple[Dict[str, Any], str, List[str]]],
    base_dir: str,
) -> List[SemanticLintIssue]:
    """
    semantic model 내부 entity의 type 값(EntityType)이
    허용된 값 범위 안에 있는지 검증합니다.

    EntityType은 "primary" 또는 "foreign"만 허용됩니다.
    """
    issues: List[SemanticLintIssue] = []

    allowed_types = ", ".join(sorted(VALID_ENTITY_TYPES))

    for sm, file_path, lines in sm_contexts:
        model_name = sm.get("name")
        rel_file = os.path.relpath(file_path, base_dir)

        entities = sm.get("entities") or []
        for idx, ent in enumerate(entities):
            if not isinstance(ent, dict):
                continue
            ent_name = ent.get("name", f"entity[{idx}]")
            invalid_type = get_invalid_entity_type(ent)
            if invalid_type is not None:
                line_no = find_line_number(lines, str(invalid_type), ent_name) or 1
                issues.append(
                    make_error(
                        rel_file,
                        line_no,
                        "SEM021_INVALID_ENTITY_TYPE",
                        (
                            f"Entity '{ent_name}' in semantic model '{model_name}' "
                            f"has invalid type '{invalid_type}'. "
                            f"Allowed EntityType values are: {allowed_types}."
                        ),
                    )
                )

    return issues
