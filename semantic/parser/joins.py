import sqlglot
from sqlglot import expressions as exp
from semantic.utils import append_node, find_dimension_by_name, find_measure_by_name


def parse_joins(parsed_smq, value, semantic_manifest, dialect):
    if not value:
        return parsed_smq
    if len(value) > 1:
        raise ValueError(
            "joins 배열에는 원소가 하나만 가능합니다. 여러 테이블 간 join은 배열의 하나의 원소에 모두 포함하여 작성해주세요. ex) joins = [\"FROM A LEFT JOIN B ON A.id = B.id INNER JOIN C ON A.id = C.id\"]"
        )
    # 주의) join value가 n개인 경우 지원에 대한 추가 개발 필요
    value = value[0]
    parsed_value = sqlglot.parse_one(value, read=dialect)
    for col in parsed_value.find_all(exp.Column):
        col_name = col.name
        if "__" in col_name:
            table_name, column_name = col_name.split("__", 1)
            col.set("this", exp.Identifier(this=column_name))
            col.set("table", exp.Identifier(this=table_name))
        else:
            column_name = col_name
            table_name = col.table
        # measure를 먼저 찾고, 없으면 dimension에서 찾습니다.
        column = find_measure_by_name(
            table_name,
            column_name,
            semantic_manifest,
        )
        if not column:
            column = find_dimension_by_name(
                table_name,
                column_name,
                semantic_manifest,
            )
        node_to_append = exp.Column(this=exp.Identifier(this=column_name))
        if column:
            expr = column.get("expr", None)
            if expr:
                parsed_expr = sqlglot.parse_one(expr, read=dialect)
                if parsed_expr.sql() != column["name"]:
                    node_to_append = exp.Alias(
                        this=parsed_expr, alias=exp.Identifier(this=column_name)
                    )
        parsed_smq = append_node(parsed_smq, table_name, "metrics", node_to_append)
    parsed_smq = append_node(
        parsed_smq,
        "agg",
        "joins",
        parsed_value,
    )
    return parsed_smq
