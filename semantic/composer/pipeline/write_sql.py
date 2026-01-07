import vendor_setup
from typing import Dict, Any, List, Optional
from sqlglot import exp
from semantic.utils import replace_from_with_real_table, AGGREGATION_EXPRESSIONS


def write_sql(parsed_smq: Dict[str, Any], semantic_manifest, dialect: str) -> str:
    """
    질문에 나온 defaultdict(...) 형태의 dict를 받아서
    - uppermost_layer(deriv 또는 agg)를 제외한 나머지는 모두 CTE
    - uppermost_layer를 최종 SELECT로
    하는 SQL 문자열을 생성한다.
    """
    # 1) uppermost_layer 결정
    uppermost = "deriv" if "deriv" in parsed_smq else "agg"

    # 2) 레이어 키들
    #   예: ["acct_installment_saving_daily", "agg", "deriv", "acct_installment_saving_src"]
    layer_keys = list(parsed_smq.keys())

    # uppermost 제외하면 CTE 대상
    cte_layer_keys = [k for k in layer_keys if k != uppermost]

    # 베이스 레이어(semantic table들)와 중간 레이어(agg 등) 구분
    # 여기선 단순히 'agg', 'deriv' 를 중간/상위 레이어로 보고 나머지는 베이스로 본다.
    base_layers = [k for k in cte_layer_keys if k not in ("agg", "deriv")]
    has_agg = "agg" in parsed_smq

    ctes: List[exp.CTE] = []

    # ---- 3) 베이스 레이어들 CTE 생성 ----
    #
    # 여기서는 "FROM 실제 물리 테이블" 부분을 모르기 때문에,
    # 일단은 동일한 이름의 테이블을 사용한다고 가정한다.
    # (실제 구현에서는 semantic layer -> physical table 매핑을 사용해서 바꾸면 됨)
    for base_name in base_layers:
        smq_of_layer = parsed_smq[base_name]

        # agg 레이어의 GROUP BY에 사용될 컬럼들을 베이스 레이어 SELECT에 추가
        if has_agg:
            agg_layer = parsed_smq["agg"]
            agg_groups = agg_layer.get("groups", [])
            base_metrics = smq_of_layer.get("metrics", [])
            base_metrics_names = [
                node.name for node in base_metrics if node.name
            ] + [
                node.alias for node in base_metrics if isinstance(node, exp.Alias) and node.alias
            ]
            
            for group in agg_groups:
                # 집계 함수인 경우 내부 컬럼 추출
                if _is_aggregation_function(group):
                    extracted_col = _extract_column_from_aggregation(group)
                    if extracted_col and extracted_col.name and extracted_col.name not in base_metrics_names:
                        smq_of_layer.setdefault("metrics", []).append(extracted_col)
                else:
                    # 집계 함수가 아닌 경우
                    group_name = group.name if hasattr(group, 'name') and group.name else None
                    if group_name and group_name not in base_metrics_names:
                        # 컬럼 노드 생성
                        if isinstance(group, exp.Column):
                            smq_of_layer.setdefault("metrics", []).append(group)
                        elif isinstance(group, exp.Alias):
                            # Alias인 경우 this를 확인
                            inner = group.this
                            if isinstance(inner, exp.Column) and inner.name not in base_metrics_names:
                                smq_of_layer.setdefault("metrics", []).append(inner)

        base_select = _build_select(
            smq_of_layer=smq_of_layer,
            from_name=base_name,  # 실제 구현에선 물리 테이블 이름으로 매핑
        )
        base_select = replace_from_with_real_table(
            base_select, semantic_manifest, dialect
        )

        ctes.append(
            exp.CTE(
                this=base_select,
                alias=exp.to_identifier(base_name),
            )
        )

    # ---- 4) agg 레이어가 있고, uppermost가 deriv일 경우 agg도 CTE로 만듦 ----
    if has_agg and uppermost == "deriv":
        agg_layer = parsed_smq["agg"]

        # agg의 FROM은
        #  - join_select가 있으면 그걸 서브쿼리로 쓰고
        #  - 없으면 일단 첫 번째 base_layer를 사용
        from_ = None
        joins = None
        if "joins" in agg_layer:
            agg_layer_joins_clause = agg_layer["joins"][0]
            from_ = agg_layer_joins_clause.args["from"]
            joins = agg_layer_joins_clause.args["joins"]

        agg_select = _build_select(
            smq_of_layer=agg_layer,
            from_name=(base_layers[0] if "joins" not in agg_layer else None),
            from_=from_,
            joins=joins,
        )

        ctes.append(
            exp.CTE(
                this=agg_select,
                alias=exp.to_identifier("agg"),
            )
        )

    # ---- 5) uppermost 최종 SELECT 구성 ----
    upper_cfg = parsed_smq[uppermost]

    if uppermost == "deriv":
        # deriv의 FROM은 agg
        final_select = _build_select(
            smq_of_layer=upper_cfg,
            from_name="agg",
            dialect=dialect,
        )
    else:
        # uppermost가 agg인 경우
        agg_layer = parsed_smq["agg"]

        # agg의 FROM은
        #  - join_select가 있으면 그걸 서브쿼리로 쓰고
        #  - 없으면 일단 첫 번째 base_layer를 사용
        from_ = None
        joins = None
        if "joins" in agg_layer:
            agg_layer_joins_clause = agg_layer["joins"][0]
            from_ = agg_layer_joins_clause.args["from"]
            joins = agg_layer_joins_clause.args["joins"]

        final_select = _build_select(
            smq_of_layer=agg_layer,
            from_name=(base_layers[0] if "joins" not in agg_layer else None),
            from_=from_,
            joins=joins,
            dialect=dialect,
        )

    # ---- 6) WITH (CTE들) + 최종 SELECT 조립 ----
    if ctes:
        with_clause = exp.With(expressions=ctes)
        final_select.set("with", with_clause)

        return final_select
    else:
        # CTE가 하나도 없으면 그냥 최종 SELECT만 반환
        return final_select


def _build_select(
    smq_of_layer: Dict[str, Any],
    from_name: Optional[str] = None,
    from_: Optional[exp.Select] = None,
    joins: Optional[list[exp.Join]] = None,
    dialect: str = None,
) -> exp.Select:
    """
    하나의 레이어(dict: metrics, filters, groups, orders, join)를
    sqlglot Select 노드로 바꾼다.
    """
    select_expr = exp.Select()

    # SELECT 절
    metrics: List[exp.Expression] = smq_of_layer.get("metrics") or []
    if metrics:
        select_expr.set("expressions", metrics)

    # FROM 절
    if from_ is not None and joins is not None:
        # join 정보가 있으면, 그걸 서브쿼리로 감싼다.
        select_expr.set("from", from_)
        select_expr.set("joins", joins)
    elif from_name is not None:
        # 기본 규칙: from 절은 우선 key값(or 이전 레이어 key)
        select_expr.set(
            "from", exp.From(this=exp.Table(this=exp.to_identifier(from_name)))
        )

    # WHERE 절
    filters: List[exp.Expression] = smq_of_layer.get("filters") or []
    if filters:
        # 여러 개면 AND 로 묶어준다.
        where_condition = None
        qualify_condition = None
        for f in filters:
            if f.find(exp.Window) or (
                from_name == "agg"
                and f.this.name
                in [node.alias for node in list(select_expr.find_all(exp.Alias))]
            ):
                qualify_condition = (
                    f
                    if qualify_condition is None
                    else exp.And(this=qualify_condition, expression=f)
                )
            else:
                where_condition = (
                    f
                    if where_condition is None
                    else exp.And(this=where_condition, expression=f)
                )
        if qualify_condition:
            select_expr.set("qualify", exp.Qualify(this=qualify_condition))
        if where_condition:
            select_expr.set("where", exp.Where(this=where_condition))

    # GROUP BY
    groups: List[exp.Expression] = smq_of_layer.get("groups") or []
    if groups:
        # GROUP BY에서 집계 함수 제거
        cleaned_groups = []
        for group in groups:
            if _is_aggregation_function(group):
                # 집계 함수인 경우, 내부 컬럼 추출
                cleaned_group = _extract_column_from_aggregation(group)
                if cleaned_group:
                    cleaned_groups.append(cleaned_group)
            else:
                cleaned_groups.append(group)
        
        if cleaned_groups:
            group = exp.Group(expressions=cleaned_groups)
            select_expr.set("group", group)

    # ORDER BY
    orders: List[exp.Expression] = smq_of_layer.get("orders") or []
    if orders:
        order = exp.Order(expressions=orders)
        select_expr.set("order", order)

    # LIMIT
    limit: Optional[exp.Expression] = smq_of_layer.get("limit", None)
    if limit is not None:
        select_expr.set("limit", exp.Limit(expression=exp.Literal.number(limit)))

    return select_expr


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
    count_nodes = expr.find_all(exp.Count)
    for count_node in count_nodes:
        if isinstance(count_node.this, exp.Distinct):
            return True
    
    # Anonymous 함수 중 집계 함수 확인
    anonymous_nodes = expr.find_all(exp.Anonymous)
    for anon_node in anonymous_nodes:
        func_name = anon_node.this.upper() if hasattr(anon_node, 'this') and anon_node.this else ""
        if func_name in ("COUNT_DISTINCT", "COUNT", "SUM", "AVG", "AVERAGE", "MAX", "MIN"):
            return True
    
    return False


def _extract_column_from_aggregation(expr: exp.Expression) -> Optional[exp.Expression]:
    """집계 함수에서 원래 컬럼 추출"""
    # Alias로 감싸진 경우
    if isinstance(expr, exp.Alias):
        inner = expr.this
        if _is_aggregation_function(inner):
            return _extract_column_from_aggregation(inner)
        return inner
    
    # COUNT_DISTINCT 같은 Anonymous 함수인 경우
    if isinstance(expr, exp.Anonymous):
        func_name = expr.this.upper() if hasattr(expr, 'this') and expr.this else ""
        if func_name in ("COUNT_DISTINCT", "COUNT", "SUM", "AVG", "AVERAGE", "MAX", "MIN"):
            # Anonymous 함수의 인자에서 컬럼 찾기
            if hasattr(expr, 'expressions') and expr.expressions:
                for arg in expr.expressions:
                    columns = list(arg.find_all(exp.Column)) if hasattr(arg, 'find_all') else []
                    if columns:
                        return columns[0]
                    # 컬럼이 없으면 Identifier 찾기
                    identifiers = list(arg.find_all(exp.Identifier)) if hasattr(arg, 'find_all') else []
                    if identifiers:
                        return exp.Column(this=identifiers[0])
    
    # COUNT(DISTINCT ...)인 경우
    if isinstance(expr, exp.Count) and isinstance(expr.this, exp.Distinct):
        # Distinct 내부의 컬럼 찾기
        distinct_expr = expr.this
        columns = list(distinct_expr.find_all(exp.Column))
        if columns:
            return columns[0]
        # 컬럼이 없으면 Identifier 찾기
        identifiers = list(distinct_expr.find_all(exp.Identifier))
        if identifiers:
            # Identifier를 Column으로 변환
            return exp.Column(this=identifiers[0])
    
    # 다른 집계 함수인 경우
    if isinstance(expr, AGGREGATION_EXPRESSIONS):
        # 집계 함수의 인자에서 컬럼 찾기
        columns = list(expr.find_all(exp.Column))
        if columns:
            return columns[0]
        # 컬럼이 없으면 Identifier 찾기
        identifiers = list(expr.find_all(exp.Identifier))
        if identifiers:
            # Identifier를 Column으로 변환
            return exp.Column(this=identifiers[0])
    
    return None
