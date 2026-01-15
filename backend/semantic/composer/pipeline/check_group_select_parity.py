import vendor_setup
from sqlglot import expressions as exp
from backend.semantic.utils import append_node, AGGREGATION_EXPRESSIONS


def check_group_select_parity_and_complete(parsed_smq):
    """Agg layer에서 group과 select가 일치하는지 확인합니다.
    (aggregation function이 아닌 select는 다 group에 들어 있어야 합니다.)
    지금은 non agg select -> group 단방향으로만 옮깁니다.
    """
    if "agg" not in parsed_smq:
        return parsed_smq
    agg_layer = parsed_smq["agg"]
    agg_layer_metrics = agg_layer["metrics"]
    agg_layer_groups = agg_layer.get("groups", [])

    for metric in agg_layer_metrics:
        if metric.find(AGGREGATION_EXPRESSIONS):
            continue
        agg_layer_groups_in_str = [
            group.name for group in agg_layer_groups if group.name
        ]
        if metric.name not in agg_layer_groups_in_str:
            if isinstance(metric, exp.Alias):
                parsed_smq = append_node(parsed_smq, "agg", "groups", metric.this)
            else:
                parsed_smq = append_node(parsed_smq, "agg", "groups", metric)

    return parsed_smq
