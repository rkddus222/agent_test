from __future__ import annotations
from typing import Literal, Dict, Any, List, Union
from dataclasses import dataclass, asdict

# -------- Common Data Types --------
DataType = Literal[
    "integer",
    "number",
    "float",
    "decimal",
    "varchar",
    "date",
    "datetime",
    "array",
    "map",
    "boolean"
]

# -------- Common Base Classes with shared utilities --------
@dataclass
class BaseType:
    """Base class for all type objects"""
    
    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "BaseType":
        raise NotImplementedError("Subclasses must implement from_dict")
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def load_objects(
    obj: Union[Dict[str, Any], List[Dict[str, Any]]],
    object_class: type,
    object_name: str = "object"
) -> List[Any]:
    """
    Generic loader for objects from dict or list of dicts.
    Used by both load_semantic_models and load_metrics.
    """
    if isinstance(obj, list):
        if not obj:
            raise ValueError(f"{object_name} list is empty")
        
        objects = []
        for i, x in enumerate(obj):
            try:
                objects.append(object_class.from_dict(x))
            except Exception as e:
                raise ValueError(f"Failed to load {object_name} at index {i}: {str(e)}")
        return objects
    
    if isinstance(obj, dict):
        try:
            return [object_class.from_dict(obj)]
        except Exception as e:
            raise ValueError(f"Failed to load single {object_name}: {str(e)}")


# -------- Serialization Helper Functions --------

def optional_to_dict(obj: Any) -> Any:
    """
    Optional 필드의 to_dict 호출을 위한 헬퍼.
    객체가 None이면 None 반환, 아니면 to_dict() 호출.
    """
    return None if obj is None else obj.to_dict()


def list_to_dict(items: List[Any]) -> List[Dict[str, Any]]:
    """
    객체 리스트를 딕셔너리 리스트로 변환.
    각 객체의 to_dict() 메서드를 호출.
    """
    return [item.to_dict() for item in items]


def safe_from_dict(
    object_class: type,
    data: Dict[str, Any],
    object_name: str = "object"
) -> Any:
    """
    from_dict 호출 시 에러 처리를 표준화한 헬퍼.
    """
    if "name" not in data:
        raise ValueError(
            f"Missing required field 'name' in {object_name}. "
            f"Available keys: {list(data.keys())}"
        )
    
    try:
        return object_class._from_dict_impl(data)
    except Exception as e:
        raise ValueError(
            f"Failed to create {object_name} '{data.get('name', 'unknown')}': {str(e)}"
        )