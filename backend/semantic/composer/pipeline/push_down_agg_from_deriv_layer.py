from backend.semantic.utils import (
    append_node,
    find_metric_by_name,
    find_table_of_column_from_original_smq,
    find_dimension_by_name,
    find_measure_by_name,
)
from sqlglot import expressions as exp
from copy import deepcopy
import sqlglot

name_for_agg_function_map = {
    "sum": "합계",
    "count": "개수",
    "avg": "평균",
    "max": "최대",
    "min": "최소",
}


def push_down_agg_from_deriv_layer(parsed_smq, original_smq, semantic_manifest):
    # deriv layer가 없으면 그냥 return
    if "deriv" not in parsed_smq:
        return parsed_smq

    deriv_layer = parsed_smq["deriv"]
    deriv_metrics = deriv_layer.get("metrics", [])
    deriv_orders = deriv_layer.get("orders", [])
    deriv_nodes = deriv_metrics + deriv_orders

    # 주의) metric이 없으면 그냥 return
    if not deriv_nodes:
        return parsed_smq

    for node in deriv_nodes:
        node_with_agg_func = list(node.find_all(exp.AggFunc))
        if node_with_agg_func:
            for node in node_with_agg_func:
                if isinstance(node.parent, exp.Window):
                    continue
                agg_func = node.key.lower()
                agg_name = name_for_agg_function_map.get(agg_func, "기타")
                deriv_metric_name = ""
                for column_node in node.find_all(exp.Column):
                    deriv_metric_name += f"{column_node.name}_"
                deriv_metric_name += agg_name
                # 여기서부턴 agg layer에다가 새로 추가하는 로직
                deepcopied_node = deepcopy(node)
                is_inner_node_metric = False
                for inner_node in deepcopied_node.find_all(exp.Column):
                    metric_of_inner_node = find_metric_by_name(
                        inner_node.name, semantic_manifest
                    )

                    if metric_of_inner_node:
                        is_inner_node_metric = True

                    if not metric_of_inner_node:
                        table_name = find_table_of_column_from_original_smq(
                            inner_node.name, original_smq
                        )
                        metric_of_inner_node = find_measure_by_name(
                            table_name, inner_node.name, semantic_manifest
                        )

                    if not metric_of_inner_node:
                        metric_of_inner_node = find_dimension_by_name(
                            table_name, inner_node.name, semantic_manifest
                        )

                    if metric_of_inner_node is None:
                        raise ValueError(
                            f"Metric/Measure/Dimension을 찾을 수 없습니다. "
                            f"inner_node.name='{inner_node.name}', table_name='{table_name}' "
                            f"(push_down_agg_from_deriv_layer 중, deriv_metric_name='{deriv_metric_name}' 처리 중)"
                        )
                    expr_of_inner_node_metric = metric_of_inner_node.get("expr", None)

                    if not is_inner_node_metric:
                        parsed_smq["deriv"]["metrics"].remove(node)
                        parsed_smq = append_node(
                            parsed_smq,
                            "deriv",
                            "metrics",
                            exp.Column(this=exp.Identifier(this=deriv_metric_name)),
                        )

                    if is_inner_node_metric and expr_of_inner_node_metric:
                        parsed_expr = sqlglot.parse_one(expr_of_inner_node_metric)
                        for ident in parsed_expr.find_all(exp.Identifier):
                            ident_name = ident.name
                            if "__" in ident_name:
                                _, ident_name_wo_table_name = ident_name.split("__", 1)
                                ident.replace(
                                    exp.Identifier(this=ident_name_wo_table_name)
                                )
                        inner_node.replace(parsed_expr)

                parsed_smq = append_node(
                    parsed_smq,
                    "agg",
                    "metrics",
                    exp.Alias(
                        this=deepcopied_node,
                        alias=exp.Identifier(this=deriv_metric_name),
                    ),
                )

                # 주의) 모든 작업이 끝난 후 deriv layer에서 해당 layer를 교체해 줍니다.
                node.replace(exp.Identifier(this=deriv_metric_name))

    return parsed_smq
