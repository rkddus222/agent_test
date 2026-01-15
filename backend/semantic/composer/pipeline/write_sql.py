import vendor_setup
from typing import Dict, Any, List, Optional
from sqlglot import exp
from backend.semantic.utils import replace_from_with_real_table


def write_sql(parsed_smq: Dict[str, Any], semantic_manifest, dialect: str) -> exp.Select:
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
                in [node.alias for node in select_expr.find_all(exp.Alias)]
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
        group = exp.Group(expressions=groups)
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
