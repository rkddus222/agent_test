from sqlglot import expressions as exp
from backend.semantic.utils import append_node


def parse_limit(parsed_smq, value, semantic_manifest, dialect):
    if not value:
        return parsed_smq
    if not isinstance(value, int):
        raise ValueError("limit 값은 정수여야 합니다.")
    parsed_smq = append_node(
        parsed_smq,
        "deriv",
        "limit",
        value,
    )
    return parsed_smq
