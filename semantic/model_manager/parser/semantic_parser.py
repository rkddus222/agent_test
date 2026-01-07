from __future__ import annotations
import json
import os
import re
from typing import Dict, List, Tuple, Any

import yaml

from semantic.model_manager.parser.parsing_validation import (
    find_duplicate_names_in_semantic_model,
    find_invalid_types_in_semantic_model,
    find_invalid_enums_in_metrics,
    find_duplicate_metric_names,
    find_missing_required_fields_in_semantic_model,
    VALID_DATA_TYPES,
    VALID_METRIC_TYPES,
)

class ParseError(RuntimeError):
    pass

REF_RE = re.compile(r"ref\(\s*'([^']+)'\s*\)")
SOURCE_RE = re.compile(r"source\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)")
TABLE_RE = re.compile(r"(\w+)\(\s*'([^']+)'\s*\)")

def assert_exists(path: str, what: str) -> None:
    if not os.path.exists(path):
        raise ParseError(f"Required {what} not found: {path}")

def load_yaml(path: str) -> Any:
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def scan_sources(sources_yml: str) -> Dict[Tuple[str, str], Dict[str, str]]:
    data = load_yaml(sources_yml)
    if not data or 'sources' not in data:
        raise ParseError("Invalid sources.yml")
    mapping = {}
    for src in data['sources']:
        db, sch, sname = src.get('database'), src.get('schema'), src.get('name')
        for tbl in src.get('tables', []):
            tname = tbl.get('name')
            mapping[(sname, tname)] = {
                "database": db,
                "schema": sch,
                "table": tname,
            }
    return mapping

def parse_table_reference(table_field: Any) -> Tuple[str, str] | None:
    """Parse table reference like 'rerp_mssql_daquv('MIS_PRJ_ACCT')' into (source_name, table_name)"""
    if isinstance(table_field, str):
        m = TABLE_RE.search(table_field)
        if m:
            return (m.group(1), m.group(2))
    return None

def parse_semantic_models(sem_dir: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    sms, mts = [], []
    for fn in os.listdir(sem_dir):
        if fn.endswith(('.yml', '.yaml')):
            data = load_yaml(os.path.join(sem_dir, fn)) or {}
            file_sms = data.get('semantic_models') or []
            file_mts = data.get('metrics') or []
            sms.extend(file_sms)
            mts.extend(file_mts)
    return sms, mts

def resolve_model_ref(model_field: Any) -> str|None:
    if isinstance(model_field, str):
        m = REF_RE.search(model_field)
        if m:
            return m.group(1)
    return None

def transform_semantic_model(sm: Dict[str, Any], source_relations: Dict[Tuple[str, str], Dict[str, str]]) -> Dict[str, Any]:
    # 필수 필드 검증
    missing_fields = find_missing_required_fields_in_semantic_model(sm)
    if missing_fields:
        model_name = sm.get("name", "unknown")
        field_details = ", ".join(f"'{field}' in {context}" for field, context in missing_fields)
        raise ParseError(
            f"Missing required fields in semantic model '{model_name}': {field_details}."
        )
    
    # Parse table reference like 'rerp_mssql_daquv('MIS_PRJ_ACCT')'
    table_ref = parse_table_reference(sm.get('table'))
    if not table_ref:
        raise ParseError(f"Invalid table reference in {sm.get('name')}")
    
    source_name, table_name = table_ref
    if (source_name, table_name) not in source_relations:
        raise ParseError(f"Source {source_name}.{table_name} not found for {sm.get('name')}")
    
    rel = source_relations[(source_name, table_name)]
    node_relation = {
        "alias": table_name,
        "schema_name": rel["schema"],
        "database": rel["database"]
        # "relation_name": f'"{rel["database"]}"."{rel["schema"]}"."{rel["table"]}"',
    }
    sm_out = {
        "name": sm.get('name'),
        "description": sm.get('description'),
        "node_relation": node_relation,
        "primary_entity": None,
        "entities": sm.get('entities') or [],
        "measures": sm.get('measures') or [],
        "dimensions": sm.get('dimensions') or [],
        "label": sm.get('label'),
        "config": {"meta": {}},
    }
    for ent in sm_out["entities"]:
        ent.setdefault("description", None)
        ent.setdefault("role", None)
        ent.setdefault("label", None)
        ent.setdefault("expr", None)
    for dim in sm_out["dimensions"]:
        dim.setdefault("expr", dim.get("expr", None))
        dim.setdefault("type", None)
        if dim.get("type_params"):
            tp = dict(dim["type_params"])
            tp.setdefault("validity_params", None)
            dim["type_params"] = tp
        else:
            dim["type_params"] = None
        dim.setdefault("label", None)
    for ms in sm_out["measures"]:
        ms.setdefault("label", None)
        ms.setdefault("type", None)  # 새로운 type 필드 추가
        # expr이 숫자인 경우 문자열로 변환
        if "expr" in ms and isinstance(ms["expr"], (int, float)):
            ms["expr"] = str(ms["expr"])
    return sm_out

def normalize_metrics(metrics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for mt in metrics:
        mt2 = dict(mt)
        
        # 새로운 간소화된 구조 감지: type_params가 없는 경우
        has_type_params = "type_params" in mt and mt["type_params"] is not None
        
        if not has_type_params:
            # 새로운 구조: expr이 최상위 레벨에 있음
            # 필수 필드만 설정
            mt2.setdefault("name", None)
            mt2.setdefault("description", None)
            mt2.setdefault("type", None)
            mt2.setdefault("metric_type", None)
            mt2.setdefault("label", None)
            mt2.setdefault("expr", None)
        else:
            # 기존 구조: type_params 기반
            mt2.setdefault("filter", None)
            mt2.setdefault("config", {"meta": {}})
            mt2.setdefault("time_granularity", None)
            mt2.setdefault("metric_type", None)  # simple, ratio, derived, conversion, cumulative
            mt2.setdefault("type", None)
            tp = dict(mt2.get("type_params") or {})
            # Fill full null structure
            tp.setdefault("numerator", None)
            tp.setdefault("denominator", None)
            tp.setdefault("expr", None)
            tp.setdefault("window", None)
            tp.setdefault("grain_to_date", None)
            tp.setdefault("metrics", [])
            tp.setdefault("conversion_type_params", None)
            tp.setdefault("cumulative_type_params", None)
            # cumulative 타입일 때 cumulative_type_params를 적절한 구조로 설정
            if mt2.get("metric_type") == "cumulative":
                tp["cumulative_type_params"] = {
                    "window": None,
                    "grain_to_date": None,
                    "period_agg": "first"
                }
            meas = tp.get("measure")
            if isinstance(meas, str):
                meas = {"name": meas, "filter": None, "alias": None, "join_to_timespine": False, "fill_nulls_with": None}
            if isinstance(meas, dict):
                meas.setdefault("filter", None)
                meas.setdefault("alias", None)
                meas.setdefault("join_to_timespine", False)
                meas.setdefault("fill_nulls_with", None)
                tp["measure"] = meas
                if "input_measures" not in tp:
                    tp["input_measures"] = [dict(meas)]
            mt2["type_params"] = tp
        
        out.append(mt2)
    return out

def parse_time_spine(date_yml_path: str, time_spine_sql: str, database: str, schema: str) -> Dict[str, Any]:
    data = load_yaml(date_yml_path)
    models = data.get('models', [])
    col_name, granularity = None, None
    for m in models:
        if m.get('name') == 'time_spine_daily':
            for c in m.get('columns', []):
                if c.get('granularity'):
                    col_name, granularity = c['name'], c['granularity']
    if not (col_name and granularity):
        raise ParseError("date.yml missing granularity column")
    node_relation = {
        "alias": "time_spine_daily",
        "schema_name": schema,
        "database": database,
        "relation_name": f'"{database}"."{schema}"."time_spine_daily"'
    }
    return {
        "time_spine_table_configurations": [],
        "dsi_package_version": {"major_version": "0", "minor_version": "7", "patch_version": "5"},
        "time_spines": [
            {
                "node_relation": node_relation,
                "primary_column": {"name": col_name, "time_granularity": granularity},
                "custom_granularities": []
            }
        ]
    }

def validate_metric_uniqueness(metrics: List[Dict[str, Any]]) -> None:
    """
    Metric 이름의 유일성을 검증합니다.
    Metric은 전역적으로 참조되므로 여러 semantic model에 같은 이름이 있으면 안 됩니다.
    
    Args:
        metrics: 전역 metrics 리스트
    
    Raises:
        ParseError: 중복된 metric 이름이 발견된 경우
    """
    duplicates = find_duplicate_metric_names(metrics)
    if duplicates:
        raise ParseError(
            f"Duplicate metric names found in metrics definition: {', '.join(duplicates)}. "
            f"Each metric must have a unique name across all semantic models."
        )

    # metric_type / type(DataType) 값 범위 검사
    invalid_metric_types, invalid_metric_data_types = find_invalid_enums_in_metrics(metrics)

    if invalid_metric_types:
        # 예: metric 'm1' -> 'simpel'
        details = ", ".join(
            f"'{name}' -> '{value}'" for name, value in invalid_metric_types
        )
        allowed = ", ".join(sorted(VALID_METRIC_TYPES))
        raise ParseError(
            "Invalid 'metric_type' values found in metrics definition: "
            f"{details}. Allowed values are: {allowed}."
        )

    if invalid_metric_data_types:
        details = ", ".join(
            f"'{name}' -> '{value}'" for name, value in invalid_metric_data_types
        )
        allowed = ", ".join(sorted(VALID_DATA_TYPES))
        raise ParseError(
            "Invalid 'type' values found in metrics definition: "
            f"{details}. Allowed values are: {allowed}."
        )


def validate_semantic_model_names(semantic_models: List[Dict[str, Any]]) -> None:
    """
    하나의 semantic model 내부에서 dimension / measure 이름의 유일성을 검증합니다.

    - 같은 semantic model 안에서 dimension 이름이 중복되면 안 됨
    - 같은 semantic model 안에서 measure 이름이 중복되면 안 됨
    - 같은 semantic model 안에서 dimension 이름과 measure 이름이 서로 겹치면 안 됨

    Args:
        semantic_models: semantic_models/*.yml 에서 읽은 semantic model 리스트

    Raises:
        ParseError: 중복된 이름 또는 잘못된 type 값이 발견된 경우
    """
    for sm in semantic_models:
        model_name = sm.get("name")

        duplicate_dims, duplicate_measures, overlap = find_duplicate_names_in_semantic_model(sm)

        # 1) dimension 이름 중복 검사
        if duplicate_dims:
            raise ParseError(
                f"Duplicate dimension names found in semantic model '{model_name}': "
                f"{', '.join(duplicate_dims)}."
            )

        # 2) measure 이름 중복 검사
        if duplicate_measures:
            raise ParseError(
                f"Duplicate measure names found in semantic model '{model_name}': "
                f"{', '.join(duplicate_measures)}."
            )
 
        # 3) dimension 이름과 measure 이름이 서로 겹치는지 검사
        if overlap:
            raise ParseError(
                f"Dimensions and measures share the same names in semantic model "
                f"'{model_name}': {', '.join(overlap)}."
            )

        # 4) dimension / measure 의 type 값이 DataType 범위 안에 있는지 검사
        invalid_dims, invalid_measures = find_invalid_types_in_semantic_model(sm)
        if invalid_dims or invalid_measures:
            messages: List[str] = []
            if invalid_dims:
                dim_parts = ", ".join(
                    f"'{name}' -> '{value}'" for name, value in invalid_dims
                )
                messages.append(f"dimensions with invalid 'type': {dim_parts}")
            if invalid_measures:
                ms_parts = ", ".join(
                    f"'{name}' -> '{value}'" for name, value in invalid_measures
                )
                messages.append(f"measures with invalid 'type': {ms_parts}")

            allowed = ", ".join(sorted(VALID_DATA_TYPES))
            raise ParseError(
                f"Invalid data types found in semantic model '{model_name}'. "
                + " ; ".join(messages)
                + f". Allowed DataType values are: {allowed}."
            )

def assemble_manifest(base_dir: str) -> Dict[str, Any]:
    date_yml = os.path.join(base_dir, 'date.yml')
    time_spine_sql = os.path.join(base_dir, 'time_spine_daily.sql')
    sources_yml = os.path.join(base_dir, 'sources.yml')
    sem_dir = os.path.join(base_dir, 'semantic_models')

    source_relations = scan_sources(sources_yml)
    sems, metrics = parse_semantic_models(sem_dir)

    # semantic model 내부 dimension / measure 이름 중복 검사
    validate_semantic_model_names(sems)

    semantic_models = [transform_semantic_model(sm, source_relations) for sm in sems]

    # Metric 이름 전역 중복 검사
    validate_metric_uniqueness(metrics)

    if not source_relations:
        raise ParseError("No sources to infer database/schema")
    first_rel = next(iter(source_relations.values()))
    db, sch = first_rel["database"], first_rel["schema"]
    
    # date.yml과 time_spine_daily.sql은 선택적 파일
    if os.path.exists(date_yml) and os.path.exists(time_spine_sql):
        project_configuration = parse_time_spine(date_yml, time_spine_sql, db, sch)
    else:
        # 기본 project_configuration 생성
        project_configuration = {
            "time_spine_table_configurations": [],
            "time_spines": []
        }

    return {
        "semantic_models": semantic_models,
        "metrics": normalize_metrics(metrics),
        "project_configuration": project_configuration,
        "saved_queries": []
    }

def write_manifest(manifest: Dict[str, Any], out_path: str):
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)