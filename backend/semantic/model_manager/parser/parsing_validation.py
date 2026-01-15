from __future__ import annotations

from typing import Any, Dict, List, Tuple, Optional

try:  # Python 3.8+ 에서는 typing.get_args 사용, 하위 버전에서는 __args__ fallback
    from typing import get_args
except Exception:  # pragma: no cover - 방어용
    def get_args(tp: Any) -> tuple:
        return getattr(tp, "__args__", ())

from backend.semantic.types.type_commons import DataType
from backend.semantic.types.metric_type import MetricType
from backend.semantic.types.semantic_model_type import EntityType
from backend.semantic.model_manager.linter.field_schema import (
    SEMANTIC_MODEL_REQUIRED_FIELDS,
    ENTITY_REQUIRED_FIELDS,
    DIMENSION_REQUIRED_FIELDS,
    MEASURE_REQUIRED_FIELDS,
)


# Literal 기반 허용 값 집합 (DataType / MetricType / EntityType 정의와 동기화)
VALID_DATA_TYPES = set(get_args(DataType))
VALID_METRIC_TYPES = set(get_args(MetricType))
VALID_ENTITY_TYPES = set(get_args(EntityType))


def _normalize_type_str(value: Any) -> Optional[str]:
    """
    YAML에서 읽은 type/metric_type 값을 문자열로 정규화합니다.

    - None 이면 None
    - 공백 문자열은 None
    - 그 외는 strip() 후 문자열 반환
    """
    if value is None:
        return None
    if isinstance(value, str):
        v = value.strip()
        return v or None
    # 숫자 등 다른 타입이 들어온 경우도 문자열로 강제 변환
    v = str(value).strip()
    return v or None


def find_duplicate_names_in_semantic_model(
    semantic_model: Dict[str, Any],
) -> Tuple[List[str], List[str], List[str]]:
    """
    semantic model 내부에서 dimension / measure 이름의 중복 및 충돌을 탐지합니다.

    Returns:
        (duplicate_dimension_names, duplicate_measure_names, overlapping_names)
    """
    dims = semantic_model.get("dimensions") or []
    measures = semantic_model.get("measures") or []

    dim_names = [d.get("name") for d in dims if d.get("name")]
    measure_names = [m.get("name") for m in measures if m.get("name")]

    # dimension 이름 중복
    dim_name_counts: Dict[str, int] = {}
    for name in dim_names:
        dim_name_counts[name] = dim_name_counts.get(name, 0) + 1
    duplicate_dims = [name for name, cnt in dim_name_counts.items() if cnt > 1]

    # measure 이름 중복
    measure_name_counts: Dict[str, int] = {}
    for name in measure_names:
        measure_name_counts[name] = measure_name_counts.get(name, 0) + 1
    duplicate_measures = [name for name, cnt in measure_name_counts.items() if cnt > 1]

    # dimension 이름과 measure 이름이 서로 겹치는 경우
    overlap = sorted(set(dim_names) & set(measure_names))

    return duplicate_dims, duplicate_measures, overlap


def _count_metric_names(metrics: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Metric 이름의 빈도수를 계산합니다.

    Args:
        metrics: metric dict 리스트

    Returns:
        metric_name_counts: {metric_name: count}
    """
    metric_name_counts: Dict[str, int] = {}
    for mt in metrics:
        name = mt.get("name")
        if not name:
            continue
        metric_name_counts[name] = metric_name_counts.get(name, 0) + 1
    return metric_name_counts


def find_duplicate_metric_names(metrics: List[Dict[str, Any]]) -> List[str]:
    """
    Metric 이름 중복을 찾아 리스트로 반환합니다.

    Returns:
        duplicates: 중복된 metric 이름 리스트 (정렬되지 않은 상태)
    """
    metric_name_counts = _count_metric_names(metrics)
    return [name for name, cnt in metric_name_counts.items() if cnt > 1]
 

def get_invalid_dimension_type(dim: Dict[str, Any]) -> Optional[str]:
    """
    하나의 dimension dict에서 잘못된 type 값을 찾아 반환합니다.

    Returns:
        잘못된 type 문자열, 또는 None (정상/미지정인 경우)
    """
    t = _normalize_type_str(dim.get("type"))
    if t is None:
        return None
    if t not in VALID_DATA_TYPES:
        return t
    return None


def get_invalid_measure_type(measure: Dict[str, Any]) -> Optional[str]:
    """
    하나의 measure dict에서 잘못된 type 값을 찾아 반환합니다.
    """
    t = _normalize_type_str(measure.get("type"))
    if t is None:
        return None
    if t not in VALID_DATA_TYPES:
        return t
    return None


def get_invalid_metric_type(metric: Dict[str, Any]) -> Optional[str]:
    """
    하나의 metric dict에서 잘못된 metric_type 값을 찾아 반환합니다.

    - None 또는 미지정은 허용 (기존 스펙과의 호환성을 위해)
    - 그 외 값이 MetricType 리터럴 집합에 없으면 에러로 간주
    """
    mt = _normalize_type_str(metric.get("metric_type"))
    if mt is None:
        return None
    if mt not in VALID_METRIC_TYPES:
        return mt
    return None


def get_invalid_metric_data_type(metric: Dict[str, Any]) -> Optional[str]:
    """
    하나의 metric dict에서 잘못된 type(DataType) 값을 찾아 반환합니다.
    """
    t = _normalize_type_str(metric.get("type"))
    if t is None:
        return None
    if t not in VALID_DATA_TYPES:
        return t
    return None


def get_invalid_entity_type(entity: Dict[str, Any]) -> Optional[str]:
    """
    하나의 entity dict에서 잘못된 type 값을 찾아 반환합니다.

    Returns:
        잘못된 type 문자열, 또는 None (정상/미지정인 경우)
    """
    t = _normalize_type_str(entity.get("type"))
    if t is None:
        return None
    if t not in VALID_ENTITY_TYPES:
        return t
    return None


def find_invalid_types_in_semantic_model(
    semantic_model: Dict[str, Any],
) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    """
    하나의 semantic model에서 dimension / measure의 type 값이
    DataType 리터럴 범위 안에 있는지 검사합니다.

    Returns:
        (invalid_dimension_types, invalid_measure_types)

        invalid_dimension_types: [(dimension_name, invalid_type_str), ...]
        invalid_measure_types:   [(measure_name, invalid_type_str), ...]
    """
    dims = semantic_model.get("dimensions") or []
    measures = semantic_model.get("measures") or []

    invalid_dims: List[Tuple[str, str]] = []
    invalid_measures: List[Tuple[str, str]] = []

    for d in dims:
        invalid = get_invalid_dimension_type(d)
        if invalid is not None:
            invalid_dims.append((d.get("name") or "", invalid))

    for m in measures:
        invalid = get_invalid_measure_type(m)
        if invalid is not None:
            invalid_measures.append((m.get("name") or "", invalid))

    return invalid_dims, invalid_measures


def find_invalid_enums_in_metrics(
    metrics: List[Dict[str, Any]],
) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    """
    metrics 리스트에서 metric_type / type 필드의 잘못된 값을 검사합니다.

    Returns:
        (invalid_metric_types, invalid_metric_data_types)

        invalid_metric_types:      [(metric_name, invalid_metric_type_str), ...]
        invalid_metric_data_types: [(metric_name, invalid_type_str), ...]
    """
    invalid_metric_types: List[Tuple[str, str]] = []
    invalid_metric_data_types: List[Tuple[str, str]] = []

    for metric in metrics:
        name = metric.get("name") or ""

        invalid_mt = get_invalid_metric_type(metric)
        if invalid_mt is not None:
            invalid_metric_types.append((name, invalid_mt))

        invalid_dt = get_invalid_metric_data_type(metric)
        if invalid_dt is not None:
            invalid_metric_data_types.append((name, invalid_dt))

    return invalid_metric_types, invalid_metric_data_types


def find_missing_required_fields_in_semantic_model(
    semantic_model: Dict[str, Any],
) -> List[Tuple[str, str]]:
    """
    semantic model의 필수 필드가 누락되었는지 검사합니다.

    Returns:
        missing_fields: [(field_name, context), ...]
            field_name: 누락된 필드 이름
            context: "semantic_model" 또는 "entity", "dimension", "measure" 등
    """
    missing_fields: List[Tuple[str, str]] = []
    model_name = semantic_model.get("name", "unknown")

    # semantic model 레벨 필수 필드
    for required_field in SEMANTIC_MODEL_REQUIRED_FIELDS:
        if not semantic_model.get(required_field):
            missing_fields.append((required_field, "semantic_model"))

    # entities 필수 필드 검사
    entities = semantic_model.get("entities") or []
    if not isinstance(entities, list):
        # entities가 리스트가 아니면 건너뜀 (타입 검증은 별도로)
        pass
    else:
        for idx, ent in enumerate(entities):
            if not isinstance(ent, dict):
                continue
            ent_name = ent.get("name", f"entity[{idx}]")
            for required_field in ENTITY_REQUIRED_FIELDS:
                if not ent.get(required_field):
                    missing_fields.append((required_field, f"entity[{idx}] ({ent_name})"))

    # dimensions 필수 필드 검사
    dimensions = semantic_model.get("dimensions") or []
    if not isinstance(dimensions, list):
        pass
    else:
        for idx, dim in enumerate(dimensions):
            if not isinstance(dim, dict):
                continue
            dim_name = dim.get("name", f"dimension[{idx}]")
            for required_field in DIMENSION_REQUIRED_FIELDS:
                if not dim.get(required_field):
                    missing_fields.append((required_field, f"dimension[{idx}] ({dim_name})"))

    # measures 필수 필드 검사
    measures = semantic_model.get("measures") or []
    if not isinstance(measures, list):
        pass
    else:
        for idx, ms in enumerate(measures):
            if not isinstance(ms, dict):
                continue
            ms_name = ms.get("name", f"measure[{idx}]")
            for required_field in MEASURE_REQUIRED_FIELDS:
                if not ms.get(required_field):
                    missing_fields.append((required_field, f"measure[{idx}] ({ms_name})"))

    return missing_fields
