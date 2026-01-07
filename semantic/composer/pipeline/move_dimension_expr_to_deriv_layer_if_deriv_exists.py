from semantic.utils import AGGREGATION_EXPRESSIONS, append_node
from sqlglot import exp
import sqlglot


def move_dimension_expr_to_deriv_layer_if_deriv_exists(parsed_smq, original_smq):
    if "deriv" not in parsed_smq:
        return parsed_smq

    if "agg" not in parsed_smq:
        return parsed_smq

    agg_layer = parsed_smq["agg"]

    if "metrics" not in agg_layer:
        return parsed_smq

    agg_layer_metrics = agg_layer["metrics"]
    node_to_move = []

    for node in agg_layer_metrics:
        if isinstance(node, AGGREGATION_EXPRESSIONS):
            node_to_move.append(node)
            continue
        if isinstance(node, exp.Alias):
            for metric in original_smq["metrics"]:
                parsed_metric = sqlglot.parse_one(metric)
                if not isinstance(parsed_metric, exp.Alias):
                    continue
                if (
                    node.alias == parsed_metric.alias
                    and type(node.this) is type(parsed_metric.this)
                    and isinstance(node.this, (exp.AggFunc, exp.Window))
                    and isinstance(parsed_metric.this, (exp.AggFunc, exp.Window))
                ):
                    if node.this.this.name in parsed_metric.this.this.name:
                        node_to_move.append(node)

    for node in node_to_move:
        agg_layer_metrics.remove(node)
        parsed_smq = append_node(parsed_smq, "deriv", "metrics", node)

    return parsed_smq
