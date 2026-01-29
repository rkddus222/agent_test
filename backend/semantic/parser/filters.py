import sqlglot
from sqlglot import expressions as exp

from backend.semantic.utils import (
    append_node,
    is_metric_in_expr,
    find_measure_by_name,
    find_dimension_by_name,
    find_metric_by_name,
)


def parse_filters(parsed_smq, values, semantic_manifest, dialect):
    for value in values:
        parsed_smq = _parse_single_value(parsed_smq, value, semantic_manifest, dialect)
    return parsed_smq


def _parse_single_value(parsed_smq, value, semantic_manifest, dialect):
    parsed_value = sqlglot.parse_one(value, read=dialect)

    # 1) metric이 있으면 deriv layer에 추가합니다.
    if is_metric_in_expr(parsed_value, semantic_manifest):
        # 1-a) metric이면 deriv layer에 추가합니다.
        # 주의) 만약 Column이 없으면 literal을 찾아서 그 literal 중 this가 str인 걸 column으로 바꿔줍니다.
        if not list(parsed_value.find_all(exp.Column)):
            for literal in parsed_value.this.find_all(exp.Literal):
                if isinstance(literal.this, str):
                    literal.replace(exp.Column(this=exp.Identifier(this=literal.this)))
        parsed_smq = append_node(
            parsed_smq,
            "deriv",
            "filters",
            parsed_value,
        )
    # 2) metric이 아닌데 앞에 model 접두사가 붙어 있지 않고, metric의 alias인 경우 그냥 deriv에 붙여 줍니다.
    elif "__" not in parsed_value.this.sql():
        parsed_smq = append_node(
            parsed_smq,
            "deriv",
            "filters",
            parsed_value,
        )

    # 3) 아니면 proj layer에 추가합니다.
    else:
        idents = _find_all_identifiers_except_subquery_child(parsed_value)

        # [Fix] 만약 identifier가 없고 literal만 있는데 __가 포함된 경우 identifier로 변환합니다.
        if not idents:
            for literal in parsed_value.find_all(exp.Literal):
                if isinstance(literal.this, str) and "__" in literal.this:
                    new_ident = exp.Identifier(this=literal.this, quoted=False)
                    literal.replace(new_ident)
                    idents.append(new_ident)

        # 3-1) 우선 parsed_value에 있는 모든 칼럼들을 expr이 있는 경우 expr로 바꿔줍니다. (이 과정에서 table_name_set을 모아서 추후 분기에 활용합니다.)
        table_names_set = set()
        for ident in idents:
            ident_name = ident.name
            try:
                table_name, column_name = ident_name.split("__")
                table_names_set.add(table_name)
            except ValueError:
                raise ValueError(
                    f"필터 식의 식별자 '{ident_name}'이 잘못되었습니다. '테이블명__컬럼명' 형식이어야 합니다."
                )
            dimension = find_dimension_by_name(
                table_name, column_name, semantic_manifest
            )
            measure = find_measure_by_name(table_name, column_name, semantic_manifest)
            if not dimension and not measure:
                # metric인지 확인
                metric = find_metric_by_name(column_name, semantic_manifest)
                if metric:
                    raise ValueError(
                        f"필터 식의 식별자 '{ident_name}'이 semantic manifest에 존재하지 않습니다. "
                        f"참고: '{column_name}'은(는) metric입니다. Metric은 model prefix 없이 단독으로 사용해야 합니다. "
                        f"예: 'table_name__metric_name' 대신 'metric_name'을 사용하세요."
                    )
                else:
                    raise ValueError(
                        f"필터 식의 식별자 '{ident_name}'이 semantic manifest에 존재하지 않습니다. "
                        f"table_name='{table_name}', column_name='{column_name}' "
                        f"(filters 파싱 중, 필터 값: '{value}' 처리 중)"
                    )
            if dimension:
                if dimension.get("expr", None):
                    this_to_replace = sqlglot.parse_one(dimension["expr"], read=dialect)
                else:
                    this_to_replace = exp.Identifier(this=dimension["name"])
            if measure:
                if measure.get("expr", None):
                    this_to_replace = sqlglot.parse_one(measure["expr"], read=dialect)
                else:
                    this_to_replace = exp.Identifier(this=measure["name"])
            ident.replace(this_to_replace)

        # 3-2) 만약 table_name이 2개 이상이면 deriv에 붙입니다.
        if len(table_names_set) > 1:
            parsed_smq = append_node(
                parsed_smq,
                "deriv",
                "filters",
                parsed_value,
            )
        # 3-3) 만약 table_name이 1개이면 해당 table의 proj layer에 붙입니다.
        else:
            # table_names_set이 비어있는 경우 (상수 필터 등) deriv에 추가합니다.
            target_table = list(table_names_set)[0] if table_names_set else "deriv"
            parsed_smq = append_node(
                parsed_smq,
                target_table,
                "filters",
                parsed_value,
            )

    return parsed_smq


def _find_all_identifiers_except_subquery_child(node):
    identifiers = []
    for current_node in node.walk(prune=lambda n: isinstance(n, exp.Subquery)):
        if isinstance(current_node, exp.Identifier):
            identifiers.append(current_node)
    return identifiers
