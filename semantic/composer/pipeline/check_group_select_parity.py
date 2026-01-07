import vendor_setup
from sqlglot import expressions as exp
from semantic.utils import append_node, AGGREGATION_EXPRESSIONS


def _is_aggregation_function(expr: exp.Expression) -> bool:
    """표현식이 집계 함수인지 확인"""
    # AGGREGATION_EXPRESSIONS에 포함된 경우
    if isinstance(expr, AGGREGATION_EXPRESSIONS):
        return True
    
    # Count with Distinct (COUNT(DISTINCT ...))
    if isinstance(expr, exp.Count) and isinstance(expr.this, exp.Distinct):
        return True
    
    # Anonymous 함수 중 집계 함수 확인 (COUNT_DISTINCT 등)
    if isinstance(expr, exp.Anonymous):
        func_name = expr.this.upper() if hasattr(expr, 'this') and expr.this else ""
        if func_name in ("COUNT_DISTINCT", "COUNT", "SUM", "AVG", "AVERAGE", "MAX", "MIN"):
            return True
    
    # Quantile 등 다른 집계 함수 확인
    if isinstance(expr, exp.Quantile):
        return True
    
    # Alias로 감싸진 경우 내부 확인
    if isinstance(expr, exp.Alias):
        return _is_aggregation_function(expr.this)
    
    # 집계 함수를 포함하는지 확인
    if expr.find(AGGREGATION_EXPRESSIONS):
        return True
    
    # COUNT(DISTINCT ...) 패턴 확인
    count_nodes = list(expr.find_all(exp.Count))
    for count_node in count_nodes:
        if isinstance(count_node.this, exp.Distinct):
            return True
    
    # Anonymous 함수 중 집계 함수 확인
    anonymous_nodes = list(expr.find_all(exp.Anonymous))
    for anon_node in anonymous_nodes:
        func_name = anon_node.this.upper() if hasattr(anon_node, 'this') and anon_node.this else ""
        if func_name in ("COUNT_DISTINCT", "COUNT", "SUM", "AVG", "AVERAGE", "MAX", "MIN"):
            return True
    
    return False


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
        # 집계 함수인 경우 건너뛰기 (COUNT_DISTINCT 포함)
        if _is_aggregation_function(metric):
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
