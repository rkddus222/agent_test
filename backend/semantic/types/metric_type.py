from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any, Union, cast, Literal

from backend.semantic.types.type_commons import (
    DataType,
    load_objects,
    optional_to_dict,
    list_to_dict,
)


@dataclass
class MetricRef:
    """type_params.metrics[*] 항목: 다른 metric을 참조할 때 사용"""

    name: str
    filter: Optional[str] = None
    alias: Optional[str] = None
    offset_window: Optional[Dict[str, Any]] = None
    offset_to_grain: Optional[str] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "MetricRef":
        return MetricRef(
            name=d["name"],
            filter=d.get("filter"),
            alias=d.get("alias"),
            offset_window=d.get("offset_window"),
            offset_to_grain=d.get("offset_to_grain"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class InputMeasure:
    """type_params.input_measures[*] 항목: measure 입력값 정의"""

    name: str
    filter: Optional[str] = None
    alias: Optional[str] = None
    join_to_timespine: Optional[bool] = None
    fill_nulls_with: Optional[Any] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "InputMeasure":
        return InputMeasure(
            name=d["name"],
            filter=d.get("filter"),
            alias=d.get("alias"),
            join_to_timespine=d.get("join_to_timespine"),
            fill_nulls_with=d.get("fill_nulls_with"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class MetricTypeParams:
    """
    다양한 metric 타입에서 공통/옵셔널로 쓰이는 필드 컨테이너.
    필요 시 window/grain_to_date/…에 구체 클래스를 추가해도 됨.
    """

    measure: Optional[str] = None
    numerator: Optional[str] = None
    denominator: Optional[str] = None
    expr: Optional[str] = None
    window: Optional[Dict[str, Any]] = None
    grain_to_date: Optional[Dict[str, Any]] = None
    metrics: List[MetricRef] = field(default_factory=list)
    conversion_type_params: Optional[Dict[str, Any]] = None
    cumulative_type_params: Optional[Dict[str, Any]] = None
    input_measures: List[InputMeasure] = field(default_factory=list)

    @staticmethod
    def from_dict(d: Optional[Dict[str, Any]]) -> Optional["MetricTypeParams"]:
        if d is None:
            return None

        # metrics 필드 처리: 문자열이면 {"name": x} 형태로 변환
        metrics_list = []
        for x in d.get("metrics", []):
            if isinstance(x, str):
                metrics_list.append(MetricRef.from_dict({"name": x}))
            else:
                metrics_list.append(MetricRef.from_dict(x))

        return MetricTypeParams(
            measure=d.get("measure"),
            numerator=d.get("numerator"),
            denominator=d.get("denominator"),
            expr=d.get("expr"),
            window=d.get("window"),
            grain_to_date=d.get("grain_to_date"),
            metrics=metrics_list,
            conversion_type_params=d.get("conversion_type_params"),
            cumulative_type_params=d.get("cumulative_type_params"),
            input_measures=[
                InputMeasure.from_dict(x) for x in d.get("input_measures", [])
            ],
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "measure": self.measure,
            "numerator": self.numerator,
            "denominator": self.denominator,
            "expr": self.expr,
            "window": self.window,
            "grain_to_date": self.grain_to_date,
            "metrics": list_to_dict(self.metrics),
            "conversion_type_params": self.conversion_type_params,
            "cumulative_type_params": self.cumulative_type_params,
            "input_measures": list_to_dict(self.input_measures),
        }


# ---- Metric core object ----
MetricType = Literal["simple", "ratio", "derived", "conversion", "cumulative"]


@dataclass
class Metric:
    name: str
    metric_type: MetricType
    description: Optional[str] = None
    type_params: Optional[MetricTypeParams] = None
    label: Optional[str] = None
    type: Optional[DataType] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Metric":
        if "name" not in d:
            raise ValueError(
                f"Missing required field 'name' in metric. Available keys: {list(d.keys())}"
            )

        if "metric_type" not in d:
            raise ValueError(
                f"Missing required field 'metric_type' in metric '{d.get('name', 'unknown')}'. Available keys: {list(d.keys())}"
            )

        try:
            return Metric(
                name=d["name"],
                metric_type=cast(MetricType, d["metric_type"]),
                description=d.get("description"),
                type_params=MetricTypeParams.from_dict(d.get("type_params")),
                label=d.get("label"),
                type=cast(Optional[DataType], d.get("type")),
            )
        except Exception as e:
            raise ValueError(
                f"Failed to create Metric '{d.get('name', 'unknown')}': {str(e)}"
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "metric_type": self.metric_type,
            "type_params": optional_to_dict(self.type_params),
            "label": self.label,
            "type": self.type,
        }


# ---- Convenience loader ----
def load_metrics(obj: Union[Dict[str, Any], List[Dict[str, Any]]]) -> List[Metric]:
    """
    하나의 metric(dict) 또는 여러 metric(list[dict])을 받아 Metric 리스트로 변환.
    """
    return load_objects(obj, Metric, "metric")
