from typing import List
from sqlglot import expressions as exp
import sqlglot
from backend.utils.logger import setup_logger

logger = setup_logger("smq_parser")

ARITHMETIC_EXPRESSIONS = (
    exp.Add,
    exp.Sub,
    exp.Mul,
    exp.Div,
    exp.Mod,
    exp.Pow,
)

AGGREGATION_EXPRESSIONS = (
    exp.Sum,
    exp.Count,
    exp.Avg,
    exp.Max,
    exp.Min,
)

FILTER_EXPRESSIONS = (
    exp.GT,
    exp.GTE,
    exp.LT,
    exp.LTE,
    exp.EQ,
    exp.NEQ,
    exp.In,
    exp.Between,
    exp.Like,
    exp.Is,
)


def find_metric_by_name(metric_name, semantic_manifest):
    """Semantic manifest에서 metric 이름으로 검색"""
    logger.info("🔵 find_metric_by_name 시작 - metric_name: %s", metric_name)
    metrics = semantic_manifest.get("metrics", [])
    logger.info("🔵 find_metric_by_name - metrics 개수: %d", len(metrics))
    for idx, metric in enumerate(metrics):
        if metric["name"] == metric_name:
            logger.info("🔵 find_metric_by_name 완료 - 찾음 (index: %d)", idx)
            return metric
    logger.info("🔵 find_metric_by_name 완료 - 없음")
    return None


def find_dimension_by_name(model_name, dimension_name, semantic_manifest):
    """Semantic manifest에서 dimension 이름으로 검색"""
    logger.info("🔵 find_dimension_by_name 시작 - model_name: %s, dimension_name: %s", model_name, dimension_name)
    models = semantic_manifest.get("semantic_models", [])
    logger.info("🔵 find_dimension_by_name - semantic_models 개수: %d", len(models))
    for model_idx, model in enumerate(models):
        if model["name"] == model_name:
            logger.info("🔵 find_dimension_by_name - 모델 찾음 (index: %d), dimensions 검색 시작", model_idx)
            dimensions = model.get("dimensions", [])
            logger.info("🔵 find_dimension_by_name - dimensions 개수: %d", len(dimensions))
            for dim_idx, dimension in enumerate(dimensions):
                if dimension["name"] == dimension_name:
                    logger.info("🔵 find_dimension_by_name 완료 - 찾음 (model_index: %d, dim_index: %d)", model_idx, dim_idx)
                    return dimension
            logger.info("🔵 find_dimension_by_name - 해당 모델에서 dimension 없음")
    logger.info("🔵 find_dimension_by_name 완료 - 없음")
    return None


def find_measure_by_name(model_name, measure_name, semantic_manifest):
    """Semantic manifest에서 measure 이름으로 검색"""
    logger.info("🔵 find_measure_by_name 시작 - model_name: %s, measure_name: %s", model_name, measure_name)
    models = semantic_manifest.get("semantic_models", [])
    logger.info("🔵 find_measure_by_name - semantic_models 개수: %d", len(models))
    for model_idx, model in enumerate(models):
        if model["name"] == model_name:
            logger.info("🔵 find_measure_by_name - 모델 찾음 (index: %d), measures 검색 시작", model_idx)
            measures = model.get("measures", [])
            logger.info("🔵 find_measure_by_name - measures 개수: %d", len(measures))
            for measure_idx, measure in enumerate(measures):
                if measure["name"] == measure_name:
                    logger.info("🔵 find_measure_by_name 완료 - 찾음 (model_index: %d, measure_index: %d)", model_idx, measure_idx)
                    return measure
            logger.info("🔵 find_measure_by_name - 해당 모델에서 measure 없음")
    logger.info("🔵 find_measure_by_name 완료 - 없음")
    return None


def is_metric_in_expr(expr, semantic_manifest):
    idents = list(expr.find_all(exp.Identifier))
    if not idents:
        idents = list(expr.find_all(exp.Literal))
        for ident in idents:
            metric = find_metric_by_name(ident.this, semantic_manifest)
            if metric:
                return True
            continue
    for ident in idents:
        metric = find_metric_by_name(ident.name, semantic_manifest)
        if metric:
            return True
        continue
    return False


def derived_metric_in_expr(expr, semantic_manifest):
    idents = expr.find_all(exp.Identifier)
    for ident in idents:
        metric = find_metric_by_name(ident.name, semantic_manifest)
        if metric:
            expr = metric.get("expr", None) if metric else None
            if expr:
                parsed_expr = sqlglot.parse_one(expr)
                expr_idents = parsed_expr.find_all(exp.Identifier)
                for expr_ident in expr_idents:
                    if is_metric_in_expr(expr_ident, semantic_manifest):
                        return True
    return False


def append_node(smq, table, key, node):
    if table not in smq:
        smq[table] = {}

    if key == "limit":
        smq[table][key] = node
        return smq

    if key not in smq[table]:
        smq[table][key] = []

    for existing_node in smq[table][key]:

        if node.name and existing_node.name and node.name == existing_node.name:
            return smq
        if node.alias and existing_node.alias and node.alias == existing_node.alias:
            return smq
        if node.sql() and existing_node.sql() and node.sql() == existing_node.sql():
            return smq

    smq[table][key].append(node)

    return smq


def find_table_of_column_from_original_smq(column: str, original_smq: dict):
    for key in original_smq:
        if key == "limit":
            continue
        if original_smq[key] is None:
            continue
        for clause in original_smq[key]:
            if "__" in clause and column in clause:
                parsed_cluase = sqlglot.parse_one(clause)
                if (
                    isinstance(parsed_cluase, exp.Alias)
                    and parsed_cluase.alias == column
                ):
                    return None
                if not isinstance(parsed_cluase, exp.Column):
                    columns_in_pasred_cluase = parsed_cluase.find_all(exp.Column)
                    selected_columns = [
                        node for node in columns_in_pasred_cluase if column in node.name
                    ]
                    if selected_columns:
                        if len(selected_columns) == 1:
                            parsed_cluase = selected_columns[0]
                        if "__" not in parsed_cluase.this.name:
                            return None
                extracted_name = parsed_cluase.this.name
                table_name, splited_column_name = extracted_name.split("__", 1)
                if column == splited_column_name:
                    return table_name
    return None


def replace_from_with_real_table(base_select, semantic_manifest, dialect):
    table_name = base_select.args["from"].name

    semantic_models = semantic_manifest.get("semantic_models", None)
    table_info = None
    for model in semantic_models:
        if model["name"] != table_name:
            continue
        elif model["name"] == table_name:
            table_info = model.get("node_relation", None)
            break
        return base_select

    if not table_info:
        raise AttributeError(
            "해당 model에 node_relation가 없습니다. 시멘틱 모델을 다시 확인하세요."
        )

    if not table_info.get("schema_name", None) and not table_info.get("database", None):
        table_expr = exp.Table(
            this=exp.Identifier(
                this=table_info["alias"],
                quoted=True if dialect == "bigquery" else False,
            )
        )
    else:
        table_expr = exp.Table(
            this=exp.Identifier(
                this=table_info["alias"],
                quoted=True if dialect == "bigquery" else False,
            ),
            db=exp.Identifier(
                this=table_info["schema_name"],
                quoted=True if dialect == "bigquery" else False,
            ),
            catalog=exp.Identifier(
                this=table_info["database"],
                quoted=True if dialect == "bigquery" else False,
            ),
        )

    base_select.set("from", exp.From(this=table_expr))

    return base_select


def add_table_prefix_to_columns(
    metrics: List[exp.Expression], base_layers: List[str]
) -> List[exp.Expression]:
    """
    agg 레이어의 expressions에서 테이블 프리픽스를 처리합니다.

    - base_layers에 있는 테이블: 그대로 유지 (table.column)
    - base_layers에 없는 테이블: 프리픽스 제거 (column)

    Args:
        metrics: List[exp.Expression] - 처리할 expressions 리스트
        base_layers: List[str] - proj 레이어 이름 리스트

    Returns:
        List[exp.Expression] - 처리된 expressions 리스트
    """
    processed_metrics = []
    for metric in metrics:
        # 이미 Alias가 있는 경우 (예: SUM(...) AS total) 그대로 유지
        if isinstance(metric, exp.Alias):
            processed_metrics.append(metric)
            continue

        # Column인 경우 테이블 프리픽스 확인
        if isinstance(metric, exp.Column):
            if metric.table:
                table_name = (
                    metric.table.name
                    if hasattr(metric.table, "name")
                    else str(metric.table)
                )
                if table_name in base_layers:
                    # base_layers에 있으면 그대로 유지
                    processed_metrics.append(metric)
                    continue
                # base_layers에 없으면 프리픽스 제거
                processed_metrics.append(exp.Column(this=metric.this, table=None))
                continue

        # 그 외의 경우 그대로 유지
        processed_metrics.append(metric)

    return processed_metrics


def remove_table_prefix_from_columns(
    metrics: List[exp.Expression],
) -> List[exp.Expression]:
    """
    uppermost(deriv) 레이어의 expressions에서 테이블 프리픽스를 제거합니다.
    FROM agg 서브쿼리를 참조할 때, 서브쿼리 결과 컬럼명으로 접근해야 합니다.

    예: account.account_no → account_no

    agg 서브쿼리의 결과 컬럼명은 테이블 프리픽스 없이 컬럼명만 됩니다.
    """

    def _transform_column(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Column):
            if node.table:
                # 테이블 프리픽스 제거, 컬럼명만 유지
                return exp.Column(this=node.this, table=None)
        return node

    processed_metrics = []
    for metric in metrics:
        processed_metric = metric.transform(_transform_column)
        processed_metrics.append(processed_metric)

    return processed_metrics
