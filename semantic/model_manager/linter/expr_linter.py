from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

import os

import vendor_setup  # noqa: F401  # vendor 경로 설정을 위한 side-effect import
import sqlglot
from sqlglot import expressions as exp

from semantic.model_manager.linter.lint_types import (
    SemanticLintIssue,
    make_error,
    find_line_number,
)
from utils.logger import setup_logger


logger = setup_logger("semantic_linter")


def find_invalid_columns_in_expr(expr: str, ddl_columns: Set[str]) -> Set[str]:
    """
    expr에서 ddl_columns에 존재하지 않는 컬럼 후보들을 찾아 반환합니다.

    단순 컬럼명(공백, 괄호, 연산자 등이 없는 경우)도 정상적으로 처리합니다.
    """
    expr = expr.strip()
    if not expr:
        return set()

    invalid: Set[str] = set()
    
    # 1) expr 자체가 단순 컬럼명인지 확인 (공백, 괄호, 연산자 등이 없음)
    is_simple_column = not any(c in expr for c in [' ', '(', ')', '+', '-', '*', '/', ',', '=', '<', '>', "'", '"'])
    if is_simple_column and expr not in ddl_columns:
        invalid.add(expr)
    
    # 2) sqlglot으로 파싱하여 복잡한 표현식에서도 컬럼 추출
    try:
        node = sqlglot.parse_one(expr)
        # Column 노드에서 컬럼명 추출
        for col in node.find_all(exp.Column):
            col_name: Optional[str] = col.name
            if not col_name:
                continue
            if col_name not in ddl_columns:
                invalid.add(col_name)
        
        # Identifier 노드에서도 컬럼명 추출 (Column 노드로 인식되지 않은 경우 대비)
        # 예: "asdf" 같은 단순 컬럼명이 Identifier로 파싱될 수 있음
        for ident in node.find_all(exp.Identifier):
            ident_name: Optional[str] = ident.name
            if not ident_name:
                continue
            # 숫자 토큰은 무시
            if ident_name.isdigit():
                continue
            if ident_name not in ddl_columns:
                invalid.add(ident_name)
    except Exception as e:
        # 파싱 실패는 정상적인 경우일 수 있음 (단순 컬럼명 등)
        logger.debug(
            "Failed to parse expr for DDL column lint '%s': %s",
            expr,
            str(e),
        )

    return invalid


def extract_used_columns_in_expr(expr: str, ddl_columns: Set[str]) -> Set[str]:
    """
    expr에서 ddl_columns에 실제 존재하는 컬럼들을 추출합니다.
    
    단순 컬럼명(공백, 괄호, 연산자 등이 없는 경우)도 정상적으로 처리합니다.
    """
    expr = expr.strip()
    if not expr:
        return set()

    used: Set[str] = set()
    
    # 1) expr 자체가 단순 컬럼명인지 확인 (공백, 괄호, 연산자 등이 없음)
    is_simple_column = not any(c in expr for c in [' ', '(', ')', '+', '-', '*', '/', ',', '=', '<', '>', "'", '"'])
    if is_simple_column and expr in ddl_columns:
        used.add(expr)
    
    # 2) sqlglot으로 파싱하여 복잡한 표현식에서도 컬럼 추출
    try:
        node = sqlglot.parse_one(expr)
        # Column 노드에서 컬럼명 추출
        for col in node.find_all(exp.Column):
            col_name: Optional[str] = col.name
            if col_name and col_name in ddl_columns:
                used.add(col_name)
        
        # Identifier 노드에서도 컬럼명 추출 (Column 노드로 인식되지 않은 경우 대비)
        # 예: "asdf" 같은 단순 컬럼명이 Identifier로 파싱될 수 있음
        for ident in node.find_all(exp.Identifier):
            ident_name: Optional[str] = ident.name
            if ident_name and ident_name in ddl_columns:
                used.add(ident_name)
    except Exception as e:
        # 파싱 실패는 정상적인 경우일 수 있음 (단순 컬럼명 등)
        logger.debug(
            "Failed to parse expr for used column extraction '%s': %s",
            expr,
            str(e),
        )

    return used


def lint_metric_expr_references(
    metric_contexts: List[Tuple[Dict[str, Any], str, List[str]]],
    dim_names: Set[str],
    measure_names: Set[str],
    base_dir: str,
    model_dim_names: Dict[str, Set[str]],
    model_measure_names: Dict[str, Set[str]],
) -> List[SemanticLintIssue]:
    """
    metrics의 expr 또는 type_params 내 참조가 실제 measures/dimensions/metrics에 존재하는지 검사합니다.
    """
    issues: List[SemanticLintIssue] = []

    metric_names: Set[str] = {
        mt.get("name") for mt, _, _ in metric_contexts if mt.get("name")
    }
    # '__'가 없는 토큰에 대해서만 적용되는 전역 이름 집합
    # (dimension / measure / metric 이름)
    allowed_names: Set[str] = set(dim_names) | set(measure_names) | metric_names

    def _lint_expr(
        expr: str,
        metric_name: str,
        file_path: str,
        lines: List[str],
    ) -> None:
        expr = expr.strip()
        if not expr:
            return

        # sqlglot으로 expr을 파싱해서 Identifier/Column 노드를 기준으로 참조를 검사한다.
        try:
            node = sqlglot.parse_one(expr)
        except Exception as e:  # pragma: no cover - 방어용
            logger.error(
                "Failed to parse metric expr '%s' in metric '%s': %s",
                expr,
                metric_name,
                str(e),
            )
            return

        candidate_names: Set[str] = set()

        # Column 노드에서 컬럼 이름 수집
        for col in node.find_all(exp.Column):
            if col.name:
                candidate_names.add(col.name)

        # 추가로 Identifier 노드에서 이름 수집 (함수명이 아닌 경우 대부분 컬럼/식별자)
        for ident in node.find_all(exp.Identifier):
            if ident.name:
                candidate_names.add(ident.name)

        for tok in candidate_names:
            # 숫자 토큰은 무시
            if tok.isdigit():
                continue

            # 1) "model__name" 패턴: model은 semantic model 이름, name은 dimension/measure 이름
            if "__" in tok:
                table_name, column_name = tok.split("__", 1)
                dims_for_model = model_dim_names.get(table_name, set())
                measures_for_model = model_measure_names.get(table_name, set())

                # 해당 semantic model 안의 dimension/measure 이름이면 정상 참조로 간주
                if column_name in dims_for_model or column_name in measures_for_model:
                    continue

                # model 이름은 존재하지만 column_name이 없을 때만 에러를 보고
                # model 이름 자체가 잘못된 경우에도 여기로 떨어지지만,
                # 그 또한 "unknown reference"로 취급한다.
                rel_file = os.path.relpath(file_path, base_dir)
                line_no = find_line_number(lines, expr, metric_name) or 1
                issues.append(
                    make_error(
                        rel_file,
                        line_no,
                        "SEM005_UNDEFINED_MEASURE_IN_EXPR",
                        (
                            f"Reference '{tok}' used in expr of metric '{metric_name}' "
                            f"is not a known metric, measure, or dimension."
                        ),
                    )
                )
                continue

            # 2) "__"가 없는 토큰은 metric / dimension / measure 이름으로만 허용
            if tok in allowed_names:
                continue

            # 그 외는 모두 알 수 없는 참조로 판단
            rel_file = os.path.relpath(file_path, base_dir)
            line_no = find_line_number(lines, expr, metric_name) or 1
            issues.append(
                make_error(
                    rel_file,
                    line_no,
                    "SEM005_UNDEFINED_MEASURE_IN_EXPR",
                    (
                        f"Reference '{tok}' used in expr of metric '{metric_name}' "
                        f"is not a known metric, measure, or dimension."
                    ),
                )
            )

    def _check_name_ref(
        ref_name: Optional[str],
        metric_name: str,
        file_path: str,
        lines: List[str],
    ) -> None:
        if not ref_name:
            return
        if ref_name in allowed_names:
            return
        rel_file = os.path.relpath(file_path, base_dir)
        line_no = find_line_number(lines, ref_name, metric_name) or 1
        issues.append(
            make_error(
                rel_file,
                line_no,
                "SEM005_UNDEFINED_MEASURE_IN_EXPR",
                (
                    f"Reference '{ref_name}' in metric '{metric_name}' "
                    f"is not a known metric, measure, or dimension."
                ),
            )
        )

    for mt, file_path, lines in metric_contexts:
        metric_name = mt.get("name")
        if not metric_name:
            continue

        tp = mt.get("type_params")
        if tp:
            if isinstance(tp, dict):
                # type_params.expr
                expr = tp.get("expr")
                if isinstance(expr, str):
                    _lint_expr(expr, metric_name, file_path, lines)

                # measure, numerator, denominator는 measure/metric 이름을 참조
                measure_field = tp.get("measure")
                if isinstance(measure_field, str):
                    _check_name_ref(measure_field, metric_name, file_path, lines)
                elif isinstance(measure_field, dict):
                    _check_name_ref(
                        measure_field.get("name"),
                        metric_name,
                        file_path,
                        lines,
                    )

                for key in ("numerator", "denominator"):
                    ref = tp.get(key)
                    if isinstance(ref, str):
                        _check_name_ref(ref, metric_name, file_path, lines)

                # metrics[*]
                for mref in tp.get("metrics") or []:
                    if isinstance(mref, str):
                        _check_name_ref(mref, metric_name, file_path, lines)
                    elif isinstance(mref, dict):
                        _check_name_ref(
                            mref.get("name"),
                            metric_name,
                            file_path,
                            lines,
                        )

                # input_measures[*]
                for im in tp.get("input_measures") or []:
                    if isinstance(im, str):
                        _check_name_ref(im, metric_name, file_path, lines)
                    elif isinstance(im, dict):
                        _check_name_ref(
                            im.get("name"),
                            metric_name,
                            file_path,
                            lines,
                        )
        else:
            # 새로운 구조: metric 최상위 레벨에 expr 필드가 있을 수 있음
            expr = mt.get("expr")
            if isinstance(expr, str):
                _lint_expr(expr, metric_name, file_path, lines)

    return issues

