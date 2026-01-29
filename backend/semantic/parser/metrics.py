import vendor_setup
import sqlglot
from sqlglot import expressions as exp

from backend.semantic.utils import (
    find_metric_by_name,
    find_dimension_by_name,
    find_measure_by_name,
    is_metric_in_expr,
    append_node,
    derived_metric_in_expr,
    AGGREGATION_EXPRESSIONS,
)


def parse_metrics(parsed_smq, values, semantic_manifest, dialect):
    for value in values:
        parsed_smq = _parse_single_value(parsed_smq, value, semantic_manifest, dialect)
    return parsed_smq


def _parse_single_value(parsed_smq, value, semantic_manifest, dialect):

    # 0) 우선 sqlglot으로 파싱하고, alias가 있으면 alias만 따로 떼어 낸다.
    parsed_value = sqlglot.parse_one(value, read=dialect)
    alias = None

    if isinstance(parsed_value, exp.Alias):
        alias = parsed_value.alias
        parsed_value = parsed_value.this

    # 0-1) derived metric인 경우 (무한 루프 방지: 최대 10회)
    max_iterations = 10
    iteration_count = 0
    processed_metrics = []  # 순환 참조 감지를 위한 리스트 (순서 보존)
    
    while derived_metric_in_expr(parsed_value, semantic_manifest):
        iteration_count += 1
        
        if iteration_count > max_iterations:
            raise RecursionError(
                f"Metric expansion exceeded maximum depth ({max_iterations}). "
                f"이것은 보통 순환 참조(circular reference)를 의미합니다. "
                f"참여한 metrics: {' → '.join(processed_metrics)}. "
                f"시멘틱 모델의 metric 정의를 확인해 주세요."
            )
        
        changed = False
        for col in parsed_value.find_all(exp.Column):
            col_name = col.name
            
            # 순환 참조 감지
            if col_name in processed_metrics:
                cycle_path = ' → '.join(processed_metrics) + ' → ' + col_name
                raise RecursionError(
                    f"Metric 간 순환 참조가 감지되었습니다: {cycle_path}. "
                    f"시멘틱 모델의 metric 정의에서 순환 의존성을 제거해 주세요."
                )
            
            metric = find_metric_by_name(col_name, semantic_manifest)
            if not metric:
                continue
            
            processed_metrics.append(col_name)
            expr = metric.get("expr", None)
            if not expr:
                raise AttributeError(
                    f"Metric {col_name}에 expr이 없습니다. 시멘틱 모델을 확인해 주세요."
                )
            parsed_expr = sqlglot.parse_one(expr)
            if is_metric_in_expr(parsed_expr, semantic_manifest):
                if not alias:
                    alias = parsed_value.sql()
                if col is parsed_value:
                    parsed_value = parsed_expr
                    changed = True
                else:
                    col.replace(parsed_expr)
                    changed = True
        
        # 변경사항이 없으면 루프 종료
        if not changed:
            break

    # 1) 식인지 아닌지 구분한다
    # exp.Columnm, exp.Literal -> 식이 아님
    # -> 식이면 1-a로, 아니면 2
    if not isinstance(parsed_value, (exp.Column, exp.Literal)):
        # 1-a) metric의 식
        if is_metric_in_expr(parsed_value, semantic_manifest):
            metrics_in_expression = []
            for ident in parsed_value.find_all(exp.Identifier):
                ident_metric = find_metric_by_name(ident.name, semantic_manifest)
                if not ident_metric:
                    table_name, column_name = ident.name.split("__", 1)
                    # measure를 먼저 찾고, 없으면 dimension에서 찾습니다.
                    column = find_measure_by_name(table_name, column_name, semantic_manifest)
                    if not column:
                        column = find_dimension_by_name(
                            table_name, column_name, semantic_manifest
                        )
                    if column is None:
                        raise ValueError(
                            f"Measure/Dimension을 찾을 수 없습니다. "
                            f"table_name='{table_name}', column_name='{column_name}' "
                            f"(metrics 파싱 중, 식 내부의 identifier '{ident.name}' 처리 중)"
                        )
                    expr = column.get("expr", None)
                    expr = sqlglot.parse_one(expr) if expr else None
                    if expr and expr.sql() != column_name:
                        expr_with_alias = exp.Alias(
                            this=expr,
                            alias=exp.Identifier(this=column_name),
                        )
                        parsed_smq = append_node(
                            parsed_smq,
                            table_name,
                            "metrics",
                            expr_with_alias,
                        )
                    else:
                        parsed_smq = append_node(
                            parsed_smq,
                            table_name,
                            "metrics",
                            exp.Column(this=exp.Identifier(this=column_name)),
                        )
                    # table_name 정보를 보존하기 위해 Column으로 변환하고 table 속성 추가
                    ident.replace(
                        exp.Column(
                            this=exp.Identifier(this=column_name),
                            table=exp.Identifier(this=table_name)
                        )
                    )
                else:
                    metrics_in_expression.append(ident_metric)

            for metric in metrics_in_expression:
                parsed_smq = _parse_individual_metric_in_metrics_clause(
                    parsed_smq, metric, semantic_manifest, dialect
                )

            parsed_smq = append_node(
                parsed_smq,
                "deriv",
                "metrics",
                exp.Alias(
                    this=parsed_value,
                    alias=exp.Identifier(this=alias) if alias else None,
                ),
            )
            return parsed_smq

        # 1-b) dimension의 식
        else:
            table_names = set()
            idents = parsed_value.find_all(exp.Identifier)
            for ident in idents:
                if "__" in ident.name:
                    table_name, dimension_name = ident.name.split("__", 1)
                    table_names.add(table_name)
                    ident.replace(exp.Identifier(this=dimension_name))
            # 1-b-ㄱ) dimension 식이 단일 테이블에 속하는 경우 해당 테이블의 dsl에 추가한다.
            if len(table_names) == 1:
                table_name = table_names.pop()
                node_to_append = (
                    parsed_value
                    if not alias
                    else exp.Alias(this=parsed_value, alias=exp.Identifier(this=alias))
                )
                if parsed_value.find(AGGREGATION_EXPRESSIONS):
                    parsed_smq = append_node(
                        parsed_smq,
                        "agg",
                        "metrics",
                        node_to_append,
                    )
                else:
                    parsed_smq = append_node(
                        parsed_smq,
                        table_name,
                        "metrics",
                        node_to_append,
                    )
                return parsed_smq

            # 1-b-ㄴ) dimension 식이 다중 테이블에 속하는 경우 deriv 레이어에 추가한다. 이 경우 각 테이블에 추가하고, agg layer에는 metric와 groupBy 모두에 추가한다.
            else:
                for ident in idents:
                    table_name, dimension_name = ident.name.split("__", 1)
                    parsed_smq = append_node(
                        parsed_smq,
                        table_name,
                        "metrics",
                        exp.Column(this=exp.Identifier(this=dimension_name)),
                    )

                # agg layer의 metrics와 groupBy에 추가
                node_with_alias = exp.Alias(
                    this=parsed_value,
                    alias=exp.Identifier(this=alias) if alias else parsed_value,
                )
                parsed_smq = append_node(parsed_smq, "agg", "metrics", node_with_alias)
                parsed_smq = append_node(
                    parsed_smq,
                    "deriv",
                    "metrics",
                    node_with_alias,
                )
                return parsed_smq

    # 2) metric인지 dimension인지 구분한다
    name = parsed_value.name
    if not name:
        name = parsed_value.this.name

    # 3) dimension/measure의 경우(=column의 name에 "__"가 있는 경우) model dsl에 추가한다.
    if "__" in name:
        table_name, column_name = name.split("__", 1)
        # measure를 먼저 찾고, 없으면 dimension에서 찾습니다.
        column = find_measure_by_name(table_name, column_name, semantic_manifest)
        if not column:
            column = find_dimension_by_name(
                table_name, column_name, semantic_manifest
            )
        if column is None:
            raise ValueError(
                f"Measure/Dimension을 찾을 수 없습니다. "
                f"table_name='{table_name}', column_name='{column_name}' "
                f"(metrics 파싱 중, column name='{name}' 처리 중)"
            )
        # 3-1) column의 expr이 있고 expr.sql()이 column_name과 같지 않은 경우 Alias(this=(Column(this=Identifier(this=...))), alias(this=Identifier(this=...)))
        expr = column.get("expr", None)
        if expr and expr != column_name:
            parsed_expr = sqlglot.parse_one(expr, read=dialect)
            expr_with_alias = exp.Alias(
                this=parsed_expr, alias=exp.Identifier(this=column_name)
            )
            parsed_smq = append_node(
                parsed_smq,
                table_name,
                "metrics",
                expr_with_alias,
            )
        # 3-2) column의 expr이 없거나 expr.sql()이 column_name과 같은 경우 그냥 Column(this=Identifier(this=...))
        else:
            parsed_smq = append_node(
                parsed_smq,
                table_name,
                "metrics",
                exp.Column(this=exp.Identifier(this=column_name)),
            )
        return parsed_smq

    # 4) metric의 경우 complex dsl에 추가하고, 필요한 base column이 model dsl에 없으면 추가한다.
    else:
        metric = find_metric_by_name(name, semantic_manifest)
        if not metric:
            raise ValueError(
                f"{parsed_value} 안의 {name if name else parsed_value.this.sql()}에 해당하는 metric이 semantic manifest에 없습니다."
            )
        parsed_smq = _parse_individual_metric_in_metrics_clause(
            parsed_smq, metric, semantic_manifest, dialect
        )

    # 5) 이름에 __도 없고, metric도 None이다 -> 둘 다 아니다 -> ValueError.
    if not metric:
        raise ValueError(
            "Metric SMQ의 값은 dimension, metric, 혹은 dimension과 metric의 식이어야 합니다."
        )

    # 6) alias 관련 처리?

    return parsed_smq


def _parse_individual_metric_in_metrics_clause(
    parsed_smq, metric, semantic_manifest, dialect
):
    # 0) expr을 parse합니다.
    expr = metric.get("expr", None)
    name = metric.get("name", None)
    if not expr:
        raise ValueError("Metric에는 반드시 expr이 있어야 합니다.")
    parsed_expr = sqlglot.parse_one(expr, read=dialect)

    # 1) expr에 있는 measures를 찾아서 각 table의 dsl에 추가합니다.
    idents = parsed_expr.find_all(exp.Identifier)
    metrics_in_expression = []
    for ident in idents:
        # 먼저 metric인지 확인
        ident_metric = find_metric_by_name(ident.name, semantic_manifest)
        if ident_metric:
            # metric인 경우는 재귀적으로 처리하기 위해 수집
            metrics_in_expression.append(ident_metric)
            # parsed_expr의 identifier는 metric 이름으로 유지 (나중에 agg layer에서 사용)
            continue
        
        # metric이 아니면 table__column 형식이어야 합니다.
        if "__" not in ident.name:
            raise ValueError(
                f"Metric expr에 사용된 identifier '{ident.name}'이 잘못되었습니다. "
                f"'table__column' 형식이어야 하거나, semantic manifest에 정의된 metric이어야 합니다."
            )
        
        table_name, column_name = ident.name.split("__", 1)
        column = find_measure_by_name(table_name, column_name, semantic_manifest)
        # 주의 measure에 없으면 dimension에서 찾아봅니다.
        if not column:
            column = find_dimension_by_name(table_name, column_name, semantic_manifest)
        if not column:
            raise ValueError(
                f"Metric expr에 사용된 column {column_name}을(를) semantic manifest에서 찾을 수 없습니다."
            )

        # 주의) 나중에 agg layer에 추가할 때를 대비하여, parsed_expr의 ident 이름은 measure_name으로 바꿔줍니다.
        # table_name 정보를 보존하기 위해 Column으로 변환하고 table 속성 추가
        ident.replace(
            exp.Column(
                this=exp.Identifier(this=column_name),
                table=exp.Identifier(this=table_name)
            )
        )

        # 1-1) measure의 expr이 있는 경우 Alias(this=(Column(this=Identifier(this=...))), alias(this=Identifier(this=...)))
        measure_expr = column.get("expr", None)
        if measure_expr and measure_expr != column_name:
            parsed_measure_expr = sqlglot.parse_one(measure_expr, read=dialect)
            expr_with_alias = exp.Alias(
                this=parsed_measure_expr, alias=exp.Identifier(this=column_name)
            )
            parsed_smq = append_node(
                parsed_smq,
                table_name,
                "metrics",
                expr_with_alias,
            )

        # 1-2) measure의 epxr이 없는 경우 그냥 Column(this=Identifier(this=...))
        else:
            parsed_smq = append_node(
                parsed_smq,
                table_name,
                "metrics",
                exp.Column(this=exp.Identifier(this=column_name)),
            )
    
    # 1-3) expr 내부에 metric이 있는 경우 재귀적으로 처리합니다.
    for metric_in_expr in metrics_in_expression:
        parsed_smq = _parse_individual_metric_in_metrics_clause(
            parsed_smq, metric_in_expr, semantic_manifest, dialect
        )

    # 2) agg layer에 expr을 추가합니다.
    agg_node = exp.Alias(this=parsed_expr, alias=exp.Identifier(this=name))
    parsed_smq = append_node(parsed_smq, "agg", "metrics", agg_node)

    return parsed_smq
