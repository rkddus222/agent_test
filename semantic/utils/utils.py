from sqlglot import expressions as exp
import sqlglot

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
    for metric in semantic_manifest.get("metrics", []):
        if metric["name"] == metric_name:
            return metric
    return None


def find_dimension_by_name(model_name, dimension_name, semantic_manifest):
    """Semantic manifest에서 dimension 이름으로 검색"""
    for model in semantic_manifest.get("semantic_models", []):
        if model["name"] == model_name:
            for dimension in model.get("dimensions", []):
                if dimension["name"] == dimension_name:
                    return dimension
    return None


def find_measure_by_name(model_name, measure_name, semantic_manifest):
    """Semantic manifest에서 measure 이름으로 검색"""
    for model in semantic_manifest.get("semantic_models", []):
        if model["name"] == model_name:
            for measure in model.get("measures", []):
                if measure["name"] == measure_name:
                    return measure
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
