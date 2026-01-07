from __future__ import annotations

from typing import Set

# 최상위 레벨 필드 정의
VALID_TOP_LEVEL_FIELDS = {"semantic_models", "metrics"}

# Semantic Model 필드 정의
SEMANTIC_MODEL_REQUIRED_FIELDS: Set[str] = {"name", "table"}
SEMANTIC_MODEL_OPTIONAL_FIELDS: Set[str] = {
    "description",
    "entities",
    "dimensions",
    "measures",
    "label",
    "config",
    "node_relation",
    "primary_entity",
}
SEMANTIC_MODEL_ALL_FIELDS: Set[str] = SEMANTIC_MODEL_REQUIRED_FIELDS | SEMANTIC_MODEL_OPTIONAL_FIELDS

# Entity 필드 정의
ENTITY_REQUIRED_FIELDS: Set[str] = {"name", "type"}
ENTITY_OPTIONAL_FIELDS: Set[str] = {"expr", "description", "role", "label"}
ENTITY_ALL_FIELDS: Set[str] = ENTITY_REQUIRED_FIELDS | ENTITY_OPTIONAL_FIELDS

# Dimension 필드 정의
DIMENSION_REQUIRED_FIELDS: Set[str] = {"name", "type"}
DIMENSION_OPTIONAL_FIELDS: Set[str] = {"label", "description", "expr", "type_params"}
DIMENSION_ALL_FIELDS: Set[str] = DIMENSION_REQUIRED_FIELDS | DIMENSION_OPTIONAL_FIELDS

# Measure 필드 정의
MEASURE_REQUIRED_FIELDS: Set[str] = {"name", "type"}
MEASURE_OPTIONAL_FIELDS: Set[str] = {"label", "description", "expr", "agg"}
MEASURE_ALL_FIELDS: Set[str] = MEASURE_REQUIRED_FIELDS | MEASURE_OPTIONAL_FIELDS

# Metric 필드 정의
METRIC_REQUIRED_FIELDS: Set[str] = {"name", "metric_type"}
METRIC_OPTIONAL_FIELDS: Set[str] = {"description", "type", "label", "expr", "type_params"}
METRIC_ALL_FIELDS: Set[str] = METRIC_REQUIRED_FIELDS | METRIC_OPTIONAL_FIELDS

# 하위 호환성을 위한 별칭 (기존 코드에서 사용 중)
VALID_SEMANTIC_MODEL_FIELDS = SEMANTIC_MODEL_ALL_FIELDS
VALID_ENTITY_FIELDS = ENTITY_ALL_FIELDS
VALID_DIMENSION_FIELDS = DIMENSION_ALL_FIELDS
VALID_MEASURE_FIELDS = MEASURE_ALL_FIELDS
VALID_METRIC_FIELDS = METRIC_ALL_FIELDS
