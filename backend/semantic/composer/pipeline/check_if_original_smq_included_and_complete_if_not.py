import vendor_setup
import sqlglot
from sqlglot import expressions as exp
from backend.semantic.utils import append_node


def check_if_original_smq_included_and_complete_if_not(parsed_smq, original_smq):
    """original smq의 metrics에 대해서 parsed_smq에 다 포함되어 있는지 확인하고,누락된 항목이 있으면 추가합니다.
    해당 metric들이 하위 레이어에 포함되어 있는지는 여기서 체크하지 않고, check_derive_prerequisite_of_* 에서 체크합니다
    추후 metrics 이외의 항목에 대해서도 체크가 필요할 시 이 pipeline에서 체크합니다."""

    # 0) 최상단 레이어를 확인합니다.
    uppermost_layer = "deriv" if "deriv" in parsed_smq else "agg"

    # 1) original smq의 metrics를 가져와, parsed_smq의 uppermost layer와 비교합니다. 없는 것들은 not_included_metrics에 담습니다.
    not_included_metrics = []

    uppermost_metrics = parsed_smq[uppermost_layer].get("metrics", [])
    stringfied_uppermost_metrics = [metric.sql() for metric in uppermost_metrics] + [
        metric.alias for metric in uppermost_metrics if isinstance(metric, exp.Alias)
    ]

    original_metrics = original_smq.get("metrics", [])
    for metric in original_metrics:
        if metric not in stringfied_uppermost_metrics:
            not_included_metrics.append(metric)

    # 2) not_included_metric을 uppermost_layer에 추가합니다.metric이면 그대로, measure나 dimension이면 table_name을 떼고 추가합니다.
    for metric in not_included_metrics:
        parsed_metric = sqlglot.parse_one(metric)

        # 2-1) metric이 식인 경우
        if not isinstance(parsed_metric, (exp.Column, exp.Identifier)):
            node_to_append = parsed_metric
            for ident in parsed_metric.find_all(exp.Identifier):
                if "__" in ident.name:
                    _, column_name = ident.name.split("__", 1)
                    ident.replace(exp.Identifier(this=column_name))
            parsed_smq = append_node(
                parsed_smq, uppermost_layer, "metrics", node_to_append
            )
            continue

        # 2-2) metric이 column이나 identifier인 경우 = 식이 아닌 경우
        if "__" in metric:
            _, column_name = metric.split("__", 1)
            node_to_append = exp.Column(this=exp.Identifier(this=column_name))
            parsed_smq = append_node(
                parsed_smq, uppermost_layer, "metrics", node_to_append
            )
        else:
            node_to_append = sqlglot.parse_one(metric)
            parsed_smq = append_node(
                parsed_smq, uppermost_layer, "metrics", node_to_append
            )

    return parsed_smq

