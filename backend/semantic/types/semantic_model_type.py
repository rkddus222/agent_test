from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any, Union, cast, Literal

from backend.semantic.types.type_commons import (
    DataType,
    load_objects,
    optional_to_dict,
    list_to_dict,
)


# -------- Leaf / helper objects --------
@dataclass
class NodeRelation:
    alias: Optional[str] = None
    schema_name: Optional[str] = None
    database: Optional[str] = None
    relation_name: Optional[str] = None

    @staticmethod
    def from_dict(d: Optional[Dict[str, Any]]) -> "NodeRelation":
        if d is None:
            return NodeRelation()
        return NodeRelation(
            alias=d.get("alias"),
            schema_name=d.get("schema_name"),
            database=d.get("database"),
            relation_name=d.get("relation_name"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# -------- Core domain objects you asked for --------
EntityType = Literal["primary", "foreign"]

AggType = Literal[
    "sum",
    "sum_boolean",
    "count",
    "count_distinct",
    "avg",
    "min",
    "max",
]


@dataclass
class Entity:
    name: str
    type: EntityType
    expr: Optional[str] = None
    description: Optional[str] = None
    role: Optional[str] = None
    label: Optional[str] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Entity":
        return Entity(
            name=d["name"],
            type=cast(EntityType, d["type"]),
            expr=d.get("expr"),
            description=d.get("description"),
            role=d.get("role"),
            label=d.get("label"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Measure:
    name: str
    agg: AggType
    description: Optional[str] = None
    expr: Optional[str] = None
    label: Optional[str] = None
    type: Optional[DataType] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Measure":
        return Measure(
            name=d["name"],
            agg=cast(AggType, d["agg"]),
            description=d.get("description"),
            expr=d.get("expr"),
            label=d.get("label"),
            type=cast(Optional[DataType], d.get("type")),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TimeValidityParams:
    # 확장 여지: validity(start_col, end_col) 같은 정의가 올 수 있음
    params: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_dict(d: Optional[Dict[str, Any]]) -> "TimeValidityParams":
        return TimeValidityParams(params=d or {})

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.params)


@dataclass
class DimensionTypeParams:
    time_granularity: Optional[str] = None  # e.g., "day", "hour"
    validity_params: Optional[TimeValidityParams] = None

    @staticmethod
    def from_dict(d: Optional[Dict[str, Any]]) -> Optional["DimensionTypeParams"]:
        if d is None:
            return None
        return DimensionTypeParams(
            time_granularity=d.get("time_granularity"),
            validity_params=TimeValidityParams.from_dict(d.get("validity_params")),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "time_granularity": self.time_granularity,
            "validity_params": optional_to_dict(self.validity_params),
        }


@dataclass
class Dimension:
    name: str
    type: DataType
    description: Optional[str] = None
    type_params: Optional[DimensionTypeParams] = None
    expr: Optional[str] = None
    label: Optional[str] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Dimension":
        return Dimension(
            name=d["name"],
            type=cast(DataType, d["type"]),
            description=d.get("description"),
            type_params=DimensionTypeParams.from_dict(d.get("type_params")),
            expr=d.get("expr"),
            label=d.get("label"),
        )

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["type_params"] = optional_to_dict(self.type_params)
        return out


# -------- Top-level semantic model --------
@dataclass
class SemanticModel:
    name: str
    description: Optional[str] = None
    node_relation: Optional[NodeRelation] = None
    primary_entity: Optional[str] = None
    entities: List[Entity] = field(default_factory=list)
    measures: List[Measure] = field(default_factory=list)
    dimensions: List[Dimension] = field(default_factory=list)
    label: Optional[str] = None
    config: Optional[Dict[str, Any]] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "SemanticModel":
        if "name" not in d:
            raise ValueError(
                f"Missing required field 'name' in semantic model. Available keys: {list(d.keys())}"
            )

        try:
            return SemanticModel(
                name=d["name"],
                description=d.get("description"),
                node_relation=NodeRelation.from_dict(d.get("node_relation")),
                primary_entity=d.get("primary_entity"),
                entities=[Entity.from_dict(x) for x in d.get("entities", [])],
                measures=[Measure.from_dict(x) for x in d.get("measures", [])],
                dimensions=[Dimension.from_dict(x) for x in d.get("dimensions", [])],
                label=d.get("label"),
                config=d.get("config"),
            )
        except Exception as e:
            raise ValueError(
                f"Failed to create SemanticModel '{d.get('name', 'unknown')}': {str(e)}"
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "node_relation": optional_to_dict(self.node_relation),
            "primary_entity": self.primary_entity,
            "entities": list_to_dict(self.entities),
            "measures": list_to_dict(self.measures),
            "dimensions": list_to_dict(self.dimensions),
            "label": self.label,
            "config": self.config,
        }


# -------- Convenience: load a list of models or a single model --------
def load_semantic_models(
    obj: Union[Dict[str, Any], List[Dict[str, Any]]],
) -> List[SemanticModel]:
    """
    입력이 하나의 모델(dict)이어도, 여러 모델(list[dict])이어도 리스트로 반환합니다.
    """
    return load_objects(obj, SemanticModel, "semantic model")
