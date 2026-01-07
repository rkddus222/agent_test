from __future__ import annotations

import os
import re
import traceback
from typing import Any, Dict, List, Optional, Set, Tuple

from semantic.model_manager.parser.semantic_parser import (
    scan_sources,
    parse_table_reference,
)
from semantic.model_manager.utils.file_loader import (
    load_semantic_models_with_files,
    load_metrics_with_files,
)
from semantic.model_manager.utils.ddl_parser import (
    parse_ddl_tables,
)
from semantic.model_manager.linter.expr_linter import (
    find_invalid_columns_in_expr,
    extract_used_columns_in_expr,
    lint_metric_expr_references,
)
from semantic.model_manager.linter.lint_types import (
    SemanticLintIssue,
    SemanticLintResult,
    make_error,
    make_warn,
    make_result,
    find_line_number,
    lint_semantic_model_types,
    lint_metric_type_enums,
    lint_entity_types,
)
from semantic.model_manager.linter.yaml_field_linter import (
    lint_top_level_field_names,
    lint_semantic_model_required_fields,
    lint_metric_field_names,
)
from semantic.model_manager.linter.field_typo_linter import (
    lint_field_typos_in_semantic_models,
    lint_field_typos_in_metrics,
)
from semantic.model_manager.linter.name_linter import (
    lint_semantic_model_name_uniqueness,
    lint_metric_uniqueness,
    lint_filename_model_name_consistency,
    lint_foreign_entity_primary_match,
)
from utils.logger import setup_logger


logger = setup_logger("semantic_linter")
def lint_semantic_models(base_dir: str) -> SemanticLintResult:
    """
    semantic_models/*.yml, metrics, sources.yml, ddl.sql을 종합적으로 검사하여
    다음을 검증합니다.

    1. semantic model 내부 dimension / measure 이름 중복 (ERROR)
    2. 파일명과 semantic model name 일치 여부 (WARN)
    3. metric 이름 전역 중복 (ERROR)
    4. metric expr / 참조가 실제 measures/dimensions/metrics에 존재하는지 (ERROR)
    5. dimensions/measures expr에서 사용된 컬럼이 DDL에 실제 존재하는지 (ERROR)
    6. DDL에는 있지만 어떤 dimension/measure에서도 사용되지 않은 컬럼 (WARN)
    7. 테이블 레퍼런스 / sources.yml 매핑 유효성 (ERROR)
    """
    try:
        issues: List[SemanticLintIssue] = []

        # 0) ddl.sql 로드 (없으면 관련 검사만 스킵)
        ddl_full_map: Dict[Tuple[str, str, str], Set[str]] = {}
        ddl_short_map: Dict[str, Set[str]] = {}
        ddl_available = True

        ddl_path = os.path.join(base_dir, "ddl.sql")
        try:
            ddl_full_map, ddl_short_map = parse_ddl_tables(ddl_path)
        except FileNotFoundError as e:
            logger.error(str(e))
            issues.append(
                make_error(
                    "ddl.sql",
                    1,
                    "SEM000_DDL_NOT_FOUND",
                    str(e),
                )
            )
            ddl_available = False

        # 1) sources.yml 로드 (없으면 관련 검사만 스킵)
        sources_yml = os.path.join(base_dir, "sources.yml")
        source_relations: Dict[Tuple[str, str], Dict[str, str]] = {}
        sources_available = True
        try:
            source_relations = scan_sources(sources_yml)
        except Exception as e:
            logger.error("Failed to load sources.yml: %s", str(e))
            issues.append(
                make_error(
                    "sources.yml",
                    1,
                    "SEM001_SOURCES_INVALID",
                    f"Failed to parse sources.yml: {str(e)}",
                )
            )
            sources_available = False

        # 2) semantic_models / metrics 로드
        sem_dir = os.path.join(base_dir, "semantic_models")
        
        # 2-0) 최상위 레벨 필드명 오타 검사 (로드 전에 수행)
        issues.extend(lint_top_level_field_names(sem_dir, base_dir))
        
        sm_contexts = load_semantic_models_with_files(sem_dir)
        metric_contexts = load_metrics_with_files(sem_dir)

        # semantic model / metric 이름 기반 검증 (DDL이나 sources.yml 유무와 무관하게 수행 가능)
        # 2-1) semantic model 필수 필드 검증
        issues.extend(lint_semantic_model_required_fields(sm_contexts, base_dir))
        
        # 2-2) 필드명 오타 검사
        issues.extend(lint_metric_field_names(metric_contexts, base_dir))
        
        # 2-3) semantic model 내부 dimension / measure 이름 중복
        issues.extend(lint_semantic_model_name_uniqueness(sm_contexts, base_dir))

        # 2-3-1) 파일명과 semantic model name 일치 여부 검사
        issues.extend(lint_filename_model_name_consistency(sm_contexts, base_dir))

        # 2-3-2) foreign entity와 primary entity name 매칭 검증
        issues.extend(lint_foreign_entity_primary_match(sm_contexts, base_dir))

        # 2-4) semantic model dimension / measure type 값(DataType) 범위 검증
        issues.extend(lint_semantic_model_types(sm_contexts, base_dir))

        # 2-4-1) semantic model entity type 값(EntityType) 범위 검증
        issues.extend(lint_entity_types(sm_contexts, base_dir))

        # 2-5) metric 이름 전역 중복
        issues.extend(lint_metric_uniqueness(metric_contexts, base_dir))

        # 2-6) metric_type / type(DataType) enum 범위 검증
        issues.extend(lint_metric_type_enums(metric_contexts, base_dir))

        # 2-7) 필드명 오타 검증 (WARN)
        issues.extend(lint_field_typos_in_semantic_models(sm_contexts, base_dir))
        issues.extend(lint_field_typos_in_metrics(metric_contexts, base_dir))

        # 2-5) metric expr / 참조에서 사용하는 이름 검증을 위해
        #      전체 dim/measure/metric 이름 집합 및 모델별 이름 집합 생성
        dim_names: Set[str] = set[str]()
        measure_names: Set[str] = set[str]()
        model_dim_names: Dict[str, Set[str]] = {}
        model_measure_names: Dict[str, Set[str]] = {}

        for sm, _, _ in sm_contexts:
            model_name = sm.get("name")
            if not model_name:
                continue

            dims_for_model: Set[str] = set[str]()
            measures_for_model: Set[str] = set[str]()

            for d in sm.get("dimensions") or []:
                n = d.get("name")
                if n:
                    dim_names.add(n)
                    dims_for_model.add(n)

            for m in sm.get("measures") or []:
                n = m.get("name")
                if n:
                    measure_names.add(n)
                    measures_for_model.add(n)

            if dims_for_model:
                model_dim_names[model_name] = dims_for_model
            if measures_for_model:
                model_measure_names[model_name] = measures_for_model

        # 2-6) metrics expr / type_params 내 참조 검증
        issues.extend(
            lint_metric_expr_references(
                metric_contexts,
                dim_names,
                measure_names,
                base_dir,
                model_dim_names,
                model_measure_names,
            )
        )

        # 3) DDL / sources 기반 검증 (둘 다 있을 때만 수행)
        if ddl_available and sources_available:
            for sm, file_path, lines in sm_contexts:
                model_name = sm.get("name")
                table_field = sm.get("table")
                rel_file = os.path.relpath(file_path, base_dir)

                table_ref = parse_table_reference(table_field)
                if not table_ref:
                    line_no = find_line_number(
                        lines, str(table_field) if table_field is not None else None, model_name
                    ) or 1
                    issues.append(
                        make_error(
                            rel_file,
                            line_no,
                            "SEM010_INVALID_TABLE_REFERENCE",
                            (
                                f"Invalid table reference '{table_field}' "
                                f"in semantic model '{model_name}'."
                            ),
                        )
                    )
                    continue

                source_name, source_table = table_ref
                rel = source_relations.get((source_name, source_table))
                if not rel:
                    line_no = find_line_number(
                        lines, str(table_field) if table_field is not None else None, model_name
                    ) or 1
                    issues.append(
                        make_error(
                            rel_file,
                            line_no,
                            "SEM011_SOURCE_NOT_FOUND",
                            (
                                f"Source '{source_name}.{source_table}' referenced by "
                                f"semantic model '{model_name}' is not defined in sources.yml."
                            ),
                        )
                    )
                    continue

                db, schema, tbl = rel.get("database"), rel.get("schema"), rel.get("table")
                ddl_columns: Optional[Set[str]] = None
                if db is not None and schema is not None and tbl is not None:
                    ddl_columns = ddl_full_map.get((db, schema, tbl))
                if ddl_columns is None:
                    ddl_columns = ddl_short_map.get(tbl or "")
                if not ddl_columns:
                    # DDL에 해당 테이블이 없으면 컬럼 유효성 검사를 건너뜀
                    continue

                used_columns: Set[str] = set()

                # 3-1) dimensions expr 검사 (컬럼 존재 여부 + 사용 컬럼 집계)
                for dim in sm.get("dimensions") or []:
                    dim_name = dim.get("name")
                    expr = dim.get("expr")
                    if not isinstance(expr, str):
                        continue
                    invalid_cols = find_invalid_columns_in_expr(expr, ddl_columns)
                    used_columns.update(extract_used_columns_in_expr(expr, ddl_columns))
                    for col in sorted(invalid_cols):
                        line_no = find_line_number(lines, expr, dim_name) or 1
                        issues.append(
                            make_error(
                                rel_file,
                                line_no,
                                "SEM006_COLUMN_NOT_IN_DDL",
                                (
                                    f"Column '{col}' used in expr of dimension '{dim_name}' "
                                    f"is not defined in DDL table '{db}.{schema}.{tbl}'."
                                ),
                            )
                        )

                # 3-2) measures expr 검사 (컬럼 존재 여부 + 사용 컬럼 집계)
                for ms in sm.get("measures") or []:
                    ms_name = ms.get("name")
                    expr = ms.get("expr")
                    if not isinstance(expr, str):
                        continue
                    invalid_cols = find_invalid_columns_in_expr(expr, ddl_columns)
                    used_columns.update(extract_used_columns_in_expr(expr, ddl_columns))
                    for col in sorted(invalid_cols):
                        line_no = find_line_number(lines, expr, ms_name) or 1
                        issues.append(
                            make_error(
                                rel_file,
                                line_no,
                                "SEM006_COLUMN_NOT_IN_DDL",
                                (
                                    f"Column '{col}' used in expr of measure '{ms_name}' "
                                    f"is not defined in DDL table '{db}.{schema}.{tbl}'."
                                ),
                            )
                        )

                # 3-3) 사용되지 않은 DDL 컬럼 경고 (테이블 단위로 한 번에 보고)
                unused_columns = sorted(col for col in ddl_columns if col not in used_columns)
                if unused_columns:
                    line_no = find_line_number(lines, None, model_name) or 1
                    cols_str = ", ".join(f"'{c}'" for c in unused_columns)
                    issues.append(
                        make_warn(
                            rel_file,
                            line_no,
                            "SEM600_UNUSED_DDL_COLUMN",
                            (
                                f"Columns {cols_str} in DDL table '{db}.{schema}.{tbl}' "
                                f"are not referenced by any dimension or measure in "
                                f"semantic model '{model_name}'."
                            ),
                        )
                    )

            return make_result(issues)
    except Exception as e:
        logger.error(
            "Failed to lint semantic models in '%s': %s",
            base_dir,
            str(e),
            exc_info=True,
        )
        logger.error("Traceback:\n%s", traceback.format_exc())
        # 에러 발생 시 빈 결과 대신 에러 정보를 포함한 결과 반환
        error_issue = make_error(
            base_dir,
            1,
            "SEM999_LINT_ERROR",
            f"Internal error during linting: {str(e)}",
        )
        return make_result([error_issue])

