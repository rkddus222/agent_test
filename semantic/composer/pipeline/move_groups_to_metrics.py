from semantic.utils import append_node


def move_groups_to_metrics(parsed_smq):
    uppermost_layer = "deriv" if "deriv" in parsed_smq else "agg"
    groups = parsed_smq.get("agg", {}).get("groups", [])

    uppermost_metrics = parsed_smq[uppermost_layer].get("metrics", [])
    uppermost_metrics_in_str = [
        node.name for node in uppermost_metrics if node.name
    ] + [node.alias for node in uppermost_metrics if node.alias]

    for group in groups:

        if group not in uppermost_metrics_in_str:
            parsed_smq = append_node(
                parsed_smq,
                uppermost_layer,
                "metrics",
                group,
            )

    return parsed_smq
