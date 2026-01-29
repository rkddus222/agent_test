import vendor_setup
import sqlglot
from sqlglot import expressions as exp
from backend.semantic.utils import (
    append_node,
    find_metric_by_name,
    find_dimension_by_name,
    find_measure_by_name,
)  # 이 util의 위치를 parser/composer의 상위로 변경 필요


def check_prerequisite_of_deriv_layer_and_complete(
    parsed_smq, original_smq, semantic_manifest, dialect
):
    """Deriv Layer에 필요한 하위 항목들이 다 들어 있는지 확인합니다."""
    if "deriv" not in parsed_smq:
        return parsed_smq

    deriv_nodes_dict = parsed_smq["deriv"]

    # 1) derive_nodes에 있는 모든 칼럼들을 모읍니다.
    deriv_nodes_list = []
    for key in deriv_nodes_dict:
        if key != "limit":
            deriv_nodes_list += deriv_nodes_dict[key]

    nodes_to_check_prerequisite = []

    for node in deriv_nodes_list:
        columns = node.find_all(exp.Column)
        if not columns:
            continue
        for column in columns:
            if column.name not in [node.name for node in nodes_to_check_prerequisite]:
                nodes_to_check_prerequisite.append(column)

    # 2) 각 칼럼들이 metric인지 dimension인지 판단합니다.
    for node in nodes_to_check_prerequisite:
        metric = find_metric_by_name(node.name, semantic_manifest)

        # 2-1) metric이면 agg에 expr AS alias가 있는지 확인하고, 없으면 추가합니다. (expr column들이 proj에 있는지는 check_prerequisited_of_agg_...에서 확인)
        if metric:
            agg_layer_aliases_in_str = [
                node.alias
                for node in parsed_smq["agg"].get("metrics", [])
                if node.alias
            ]
            if metric["name"] in agg_layer_aliases_in_str:
                continue

            else:
                parsed_metric_expr = sqlglot.parse_one(metric["expr"], read=dialect)
                for ident in parsed_metric_expr.find_all(exp.Identifier):
                    # 먼저 metric인지 확인
                    ident_metric = find_metric_by_name(ident.name, semantic_manifest)
                    if ident_metric:
                        # metric인 경우는 이미 다른 곳에서 처리되므로 건너뜁니다.
                        # identifier 이름은 그대로 유지합니다.
                        continue
                    
                    # metric이 아니면 table__column 형식이어야 합니다.
                    if "__" not in ident.name:
                        raise ValueError(
                            f"Metric expr에 사용된 identifier '{ident.name}'이 잘못되었습니다. "
                            f"'table__column' 형식이어야 하거나, semantic manifest에 정의된 metric이어야 합니다. "
                            f"(metric '{metric['name']}'의 expr 처리 중)"
                        )
                    
                    table_name, column_name = ident.name.split("__", 1)
                    ident.replace(exp.Identifier(this=column_name))
                    # 주의) 여기서 해당 measure를 proj layer에 추가합니다!
                    column = find_measure_by_name(
                        table_name, column_name, semantic_manifest
                    )
                    if not column:
                        column = find_dimension_by_name(
                            table_name, column_name, semantic_manifest
                        )
                    if column:
                        expr = column.get("expr")
                        if expr:
                            parsed_column_expr = sqlglot.parse_one(expr, read=dialect)
                            parsed_smq = append_node(
                                parsed_smq,
                                table_name,
                                "metrics",
                                exp.Alias(
                                    this=parsed_column_expr,
                                    alias=exp.Identifier(this=column_name),
                                ),
                            )

                    else:
                        parsed_smq = append_node(
                            parsed_smq,
                            table_name,
                            "metrics",
                            exp.Column(this=exp.Identifier(this=column_name)),
                        )

                parsed_smq = append_node(
                    parsed_smq,
                    "agg",
                    "metrics",
                    exp.Alias(
                        this=parsed_metric_expr,
                        alias=exp.Identifier(this=metric["name"]),
                    ),
                )

        # 2-2) node.name이 alias인 경우는 건너뜁니다.
        elif node.name in [node.alias for node in deriv_nodes_list if node.alias]:
            continue

        # 2-3) dimension이면 agg에 dimension이 있는지 확인하고, 없으면 추가합니다.
        else:
            agg_layer_columns_in_str = [
                node.name for node in parsed_smq["agg"].get("metrics", []) if node.name
            ] + [
                node.alias
                for node in parsed_smq["agg"].get("metrics", [])
                if node.alias
            ]

            node_name = node.name if node.name else node.this.name

            if node_name in agg_layer_columns_in_str:
                continue
            else:
                # node에 table 정보가 있으면 함께 추가 (ambiguous column 방지)
                if node.table:
                    column_to_append = exp.Column(
                        this=exp.Identifier(this=node_name),
                        table=node.table
                    )
                else:
                    column_to_append = exp.Column(this=exp.Identifier(this=node_name))
                parsed_smq = append_node(
                    parsed_smq,
                    "agg",
                    "metrics",
                    column_to_append,
                )
            # 주의) agg layer에 없는 걸 proj layer에 추가하는 건 아마 필요 없는 듯합니다..?

    return parsed_smq
