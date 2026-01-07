import vendor_setup
import sqlglot
from sqlglot import expressions as exp
from semantic.utils import (
    find_metric_by_name,
    append_node,
    find_table_of_column_from_original_smq,
    find_dimension_by_name,
    find_measure_by_name,
)


def check_prerequisite_of_agg_layer_and_complete(
    parsed_smq, original_smq, semantic_manifest, dialect
):
    """Agg Layer에 필요한 하위 항목들이 다 들어 있는지 확인합니다."""
    if "agg" not in parsed_smq:
        return parsed_smq

    agg_layer = parsed_smq["agg"]

    # 1) agg_layer에 있는 모든 칼럼들을 모읍니다.
    agg_nodes_list = []
    for key in agg_layer:
        agg_nodes_list += agg_layer[key]
    nodes_to_check_prerequisite = []
    for node in agg_nodes_list:
        columns = node.find_all(exp.Column)
        if not columns:
            continue
        for column in columns:
            if column.name not in [node.name for node in nodes_to_check_prerequisite]:
                nodes_to_check_prerequisite.append(column)

    # 2) 각 칼럼들이 metric인지 dimension인지 판단합니다.
    for node in nodes_to_check_prerequisite:
        metric = find_metric_by_name(node.name, semantic_manifest)

        # 2-1) metric이면 해당 모델에 expr 안에 있는 칼럼들이 proj_layer에 다 있는지 확인합니다. (alias가 있으면 alias를, name이면 name을 확인합니다.)
        if metric:
            expr = metric.get("expr", None)
            parsed_expr = sqlglot.parse_one(expr, read=dialect)
            for column in parsed_expr.find_all(exp.Column):
                table_name, column_name = column.name.split("__")
                node_to_append = exp.Column(this=exp.Identifier(this=column_name))

                # 2-1-a) 이미 column이 project layer에 있으면 다음 column으로 continue하고
                if table_name in parsed_smq:
                    proj_layer_columns_in_str = [
                        node.name
                        for node in parsed_smq[table_name].get("metrics", [])
                        if node.name
                    ] + [
                        node.alias
                        for node in parsed_smq[table_name].get("metrics", [])
                        if isinstance(node, exp.Alias)
                    ]
                    if column_name in proj_layer_columns_in_str:
                        continue

                # 2-1-b) column이 proj layer에 없으면 추가합니다.
                parsed_smq = append_node(
                    parsed_smq, table_name, "metrics", node_to_append
                )
        # 2-2) measure/dimension이면 모델을 찾아서 있는지 확인하고 없으면 더합니다.
        else:
            column_name = node.name
            table_name = find_table_of_column_from_original_smq(
                column_name, original_smq
            )
            # 주의) metric의 base로 agg에 추가된, 예를 들어서 "SUM(적립금액) AS 일별적립식_총적립금액"과 같은 경우 이 "적립금액" column은 table_name = None이다.
            # 이건 parser에서 잘 추가했으리라 믿고 넘어가 보자!
            if table_name is None:
                continue

            node_to_append = exp.Column(this=exp.Identifier(this=column_name))
            # 2-2-a) 이미 column이 project layer에 있으면 continue하고
            if table_name in parsed_smq:
                proj_layer_columns_in_str = [
                    node.name
                    for node in parsed_smq[table_name].get("metrics", [])
                    if node.name
                ] + [
                    node.alias
                    for node in parsed_smq[table_name].get("metrics", [])
                    if isinstance(node, exp.Alias)
                ]
                if column_name in proj_layer_columns_in_str:
                    continue
            # 2-2-b) column이 proj layer에 없으면 추가합니다.
            column = find_dimension_by_name(table_name, column_name, semantic_manifest)
            if not column:
                column = find_measure_by_name(
                    table_name, column_name, semantic_manifest
                )
            if not column:
                raise ValueError(
                    f"Agg layer에 있는 column {column_name}을(를) semantic manifest에서 찾을 수 없습니다."
                )
            if column.get("expr", None):
                expr = column["expr"]
                parsed_expr = sqlglot.parse_one(expr, read=dialect)
                if parsed_expr.sql() == column_name:
                    node_to_append = exp.Column(this=exp.Identifier(this=column_name))
                else:
                    node_to_append = exp.Alias(
                        this=parsed_expr, alias=exp.Identifier(this=column_name)
                    )
            parsed_smq = append_node(parsed_smq, table_name, "metrics", node_to_append)

    return parsed_smq
