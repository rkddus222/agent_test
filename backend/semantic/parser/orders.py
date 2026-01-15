import sqlglot
from sqlglot import expressions as exp
from backend.semantic.utils import (
    append_node,
    is_metric_in_expr,
    derived_metric_in_expr,
    find_metric_by_name,
    find_dimension_by_name,
    find_measure_by_name,
)


def parse_orders(parsed_smq, values, semantic_manifest, dialect):
    for value in values:
        parsed_value = sqlglot.parse_one(value, read=dialect)
        desc = False
        # parsed_value가 Neg인 경우, 앞에 "-"가 붙은 경우이므로 desc=True로 설정
        if isinstance(parsed_value, exp.Neg):
            parsed_value = parsed_value.this
            desc = True

        # 1) 해당 value가 dimension인 경우
        if "__" in parsed_value.name:
            _, column_name = parsed_value.name.split("__")
            value_to_append = exp.Ordered(
                this=exp.Column(this=exp.Identifier(this=column_name)),
                desc=desc,
            )
            parsed_smq = append_node(
                parsed_smq,
                "deriv",
                "orders",
                value_to_append,
            )

        # 2) 해당 value가 metric인 경우
        else:
            derived_metric = False
            # 2-1) derive metric인 경우 먼저 unpack해 줍니다
            while derived_metric_in_expr(parsed_value, semantic_manifest):
                derived_metric = True
                for col in parsed_value.find_all(exp.Column):
                    col_name = col.name
                    metric = find_metric_by_name(col_name, semantic_manifest)
                    if not metric:
                        continue
                    expr = metric.get("expr", None)
                    if not expr:
                        raise AttributeError(
                            f"Metric {col_name}에 expr이 없습니다. 시멘틱 모델을 확인해 주세요."
                        )
                    parsed_expr = sqlglot.parse_one(expr)
                    # 주의) 안에 dimension이 있으면 table_name을 떼 주고, 해당 table의 proj layer에 column을 추가해 줘야 합니다.
                    for ident in parsed_expr.find_all(exp.Identifier):
                        ident_metric = find_metric_by_name(
                            ident.name, semantic_manifest
                        )
                        if not ident_metric:
                            table_name, column_name = ident.name.split("__", 1)
                            # measure를 먼저 찾고, 없으면 dimension에서 찾습니다.
                            column = find_measure_by_name(table_name, column_name, semantic_manifest)
                            if not column:
                                column = find_dimension_by_name(
                                    table_name, column_name, semantic_manifest
                                )
                            if column is None:
                                raise ValueError(
                                    f"Measure/Dimension을 찾을 수 없습니다. "
                                    f"table_name='{table_name}', column_name='{column_name}' "
                                    f"(orders 파싱 중, derived metric의 identifier '{ident.name}' 처리 중)"
                                )
                            expr = column.get("expr", None)
                            expr = sqlglot.parse_one(expr) if expr else None
                            if expr and expr != column_name:
                                expr_with_alias = exp.Alias(
                                    this=expr, alias=exp.Identifier(this=column_name)
                                )
                                parsed_smq = append_node(
                                    parsed_smq,
                                    table_name,
                                    "metrics",
                                    expr_with_alias,
                                )
                            else:
                                parsed_smq = append_node(
                                    parsed_smq,
                                    table_name,
                                    "metrics",
                                    exp.Column(
                                        this=exp.Identifier(this=column_name)
                                    ),
                                )
                            ident.replace(exp.Identifier(this=column_name))
                    if is_metric_in_expr(parsed_expr, semantic_manifest):
                        if col is parsed_value:
                            parsed_value = parsed_expr
                        else:
                            col.replace(parsed_expr)
            if derived_metric:
                value_to_append = exp.Ordered(
                    this=parsed_value,
                    desc=desc,
                )
            # 2-2) 일반 metric의 경우
            else:
                column_name = parsed_value.this.name
                value_to_append = exp.Ordered(
                    this=exp.Column(this=exp.Identifier(this=column_name)),
                    desc=desc,
                )
            parsed_smq = append_node(
                parsed_smq,
                "deriv",
                "orders",
                value_to_append,
            )
    return parsed_smq
