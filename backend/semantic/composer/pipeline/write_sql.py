import vendor_setup
from typing import Dict, Any, List, Optional
from sqlglot import exp
from backend.semantic.utils import replace_from_with_real_table, find_table_of_column_from_original_smq, find_dimension_by_name, find_measure_by_name, find_metric_by_name, AGGREGATION_EXPRESSIONS


def write_sql(parsed_smq: Dict[str, Any], semantic_manifest, dialect: str, original_smq: Optional[Dict[str, Any]] = None) -> exp.Select:
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

    # 상위 레이어에서 사용되는 컬럼 수집 (하위 레이어 SELECT에 추가하기 위해)
    upper_layer = parsed_smq.get(uppermost, {})
    upper_groups = upper_layer.get("groups", [])
    upper_filters = upper_layer.get("filters", [])
    
    # 상위 레이어에서 사용되는 컬럼명 수집
    upper_required_columns = set()
    for group_expr in upper_groups:
        for col in group_expr.find_all(exp.Column):
            if col.name:
                upper_required_columns.add(col.name)
        for ident in group_expr.find_all(exp.Identifier):
            if ident.name:
                upper_required_columns.add(ident.name)
    for filter_expr in upper_filters:
        for col in filter_expr.find_all(exp.Column):
            if col.name:
                upper_required_columns.add(col.name)
        for ident in filter_expr.find_all(exp.Identifier):
            if ident.name:
                upper_required_columns.add(ident.name)
    
    # ---- 3) 베이스 레이어들 CTE 생성 ----
    #
    # 여기서는 "FROM 실제 물리 테이블" 부분을 모르기 때문에,
    # 일단은 동일한 이름의 테이블을 사용한다고 가정한다.
    # (실제 구현에서는 semantic layer -> physical table 매핑을 사용해서 바꾸면 됨)
    for base_name in base_layers:
        smq_of_layer = parsed_smq[base_name]
        
        # 상위 레이어에서 필요한 컬럼이 이 레이어의 필터에 사용되었는지 확인
        # 필터에 사용된 컬럼은 SELECT 절에 추가되어야 함
        base_filters = smq_of_layer.get("filters", [])
        base_filter_columns = set()
        for filter_expr in base_filters:
            for col in filter_expr.find_all(exp.Column):
                if col.name and col.name in upper_required_columns:
                    base_filter_columns.add(col.name)
            for ident in filter_expr.find_all(exp.Identifier):
                if ident.name and ident.name in upper_required_columns:
                    base_filter_columns.add(ident.name)
        
        # 상위 레이어에서 필요한 컬럼이 이 레이어의 필터에 사용되었으면 metrics에 추가
        if base_filter_columns:
            existing_metrics = smq_of_layer.get("metrics", [])
            existing_metric_names = set()
            for metric in existing_metrics:
                if isinstance(metric, exp.Column) and metric.name:
                    existing_metric_names.add(metric.name)
                elif isinstance(metric, exp.Alias):
                    if metric.alias and isinstance(metric.alias, exp.Identifier):
                        existing_metric_names.add(metric.alias.name)
                    elif isinstance(metric.this, exp.Column) and metric.this.name:
                        existing_metric_names.add(metric.this.name)
            
            for col_name in base_filter_columns:
                if col_name not in existing_metric_names:
                    if "metrics" not in smq_of_layer:
                        smq_of_layer["metrics"] = []
                    smq_of_layer["metrics"].append(exp.Column(this=exp.Identifier(this=col_name)))

        base_select = _build_select(
            smq_of_layer=smq_of_layer,
            from_name=base_name,  # 실제 구현에선 물리 테이블 이름으로 매핑
            add_table_alias=False,  # proj layer는 단일 테이블이므로 alias 불필요
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
            add_table_alias=True,  # agg layer는 JOIN이 있거나 CTE 참조하므로 alias 필요
            original_smq=original_smq,
            semantic_manifest=semantic_manifest,
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
        # deriv의 FROM은 agg (단일 CTE 참조이므로 컬럼에 테이블 alias 불필요)
        final_select = _build_select(
            smq_of_layer=upper_cfg,
            from_name="agg",
            dialect=dialect,
            add_table_alias=False,  # 최종 SELECT는 단일 CTE 참조이므로 alias 불필요
            original_smq=original_smq,
            semantic_manifest=semantic_manifest,
        )
        # 최종 SELECT에서 컬럼의 테이블 alias 제거 (단일 CTE 참조이므로)
        _remove_table_alias_from_columns(final_select)
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

        # uppermost가 agg인 경우
        # JOIN이 있으면 alias 필요, 없으면 단일 테이블이므로 alias 불필요
        has_joins_in_agg = "joins" in agg_layer
        final_select = _build_select(
            smq_of_layer=agg_layer,
            from_name=(base_layers[0] if not has_joins_in_agg else None),
            from_=from_,
            joins=joins,
            dialect=dialect,
            add_table_alias=has_joins_in_agg,  # JOIN이 있을 때만 alias 필요
            original_smq=original_smq,
            semantic_manifest=semantic_manifest,
        )
        # 최종 SELECT에서 컬럼의 테이블 alias 제거 (단일 테이블/CTE 참조이므로)
        if not has_joins_in_agg:
            _remove_table_alias_from_columns(final_select)

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
    add_table_alias: bool = True,  # proj layer는 False, agg/deriv는 True
    original_smq: Optional[Dict[str, Any]] = None,
    semantic_manifest: Optional[Dict[str, Any]] = None,
) -> exp.Select:
    """
    하나의 레이어(dict: metrics, filters, groups, orders, join)를
    sqlglot Select 노드로 바꾼다.
    """
    select_expr = exp.Select()

    # SELECT 절
    metrics: List[exp.Expression] = smq_of_layer.get("metrics") or []
    
    # 문제 2 해결: group_by와 filters에 사용된 컬럼을 SELECT 절에 추가
    groups: List[exp.Expression] = smq_of_layer.get("groups") or []
    filters: List[exp.Expression] = smq_of_layer.get("filters") or []
    
    # 필요한 컬럼들을 수집
    required_columns = set()
    
    # groups에 사용된 컬럼 수집
    if groups:
        for group_expr in groups:
            # Column 노드를 찾아서 컬럼명 수집
            for col in group_expr.find_all(exp.Column):
                if col.name:
                    required_columns.add(col.name)
            # Identifier 노드도 확인 (컬럼명만 있는 경우)
            for ident in group_expr.find_all(exp.Identifier):
                if ident.name:
                    required_columns.add(ident.name)
    
    # filters에 사용된 컬럼 수집
    if filters:
        for filter_expr in filters:
            # Column 노드를 찾아서 컬럼명 수집
            for col in filter_expr.find_all(exp.Column):
                if col.name:
                    required_columns.add(col.name)
            # Identifier 노드도 확인
            for ident in filter_expr.find_all(exp.Identifier):
                if ident.name:
                    required_columns.add(ident.name)
    
    # metrics에 이미 있는지 확인하고 없으면 추가
    existing_metric_names = set()
    for metric in metrics:
        if isinstance(metric, exp.Column):
            if metric.name:
                existing_metric_names.add(metric.name)
        elif isinstance(metric, exp.Alias):
            if metric.alias and isinstance(metric.alias, exp.Identifier):
                existing_metric_names.add(metric.alias.name)
            elif isinstance(metric.this, exp.Column) and metric.this.name:
                existing_metric_names.add(metric.this.name)
    
    # 필요한 컬럼 중 metrics에 없는 것들을 추가
    for col_name in required_columns:
        if col_name not in existing_metric_names:
            # 테이블 alias 추가 (proj layer는 불필요)
            if add_table_alias and from_name:
                metrics.append(exp.Column(
                    this=exp.Identifier(this=col_name),
                    table=exp.Identifier(this=from_name)
                ))
            else:
                metrics.append(exp.Column(this=exp.Identifier(this=col_name)))
    
    # FROM 절 처리 및 테이블 alias 결정
    table_alias = from_name  # 기본값은 from_name
    join_tables = {}  # JOIN이 있을 때 사용되는 테이블들 {table_alias: table_name}
    
    if from_ is not None and joins is not None:
        # join 정보가 있으면, 그걸 서브쿼리로 감싼다.
        select_expr.set("from", from_)
        select_expr.set("joins", joins)
        
        # JOIN이 있는 경우, JOIN 절에서 사용되는 모든 테이블 수집
        # FROM 절의 테이블
        if isinstance(from_, exp.Select):
            from_clause = from_.find(exp.From)
            if from_clause:
                from_table = from_clause.this
                if isinstance(from_table, exp.Table):
                    table_alias_from = from_table.alias_or_name
                    # CTE 이름을 semantic model 이름으로 사용 (agg layer에서 JOIN하는 테이블들은 모두 CTE)
                    join_tables[table_alias_from] = table_alias_from
                elif isinstance(from_table, exp.Subquery):
                    if from_table.alias:
                        table_alias_from = from_table.alias.name
                        join_tables[table_alias_from] = table_alias_from
        
        # JOIN 절의 테이블들
        for join in joins:
            join_table = join.this
            if isinstance(join_table, exp.Table):
                table_alias_join = join_table.alias_or_name
                # CTE 이름을 semantic model 이름으로 사용 (agg layer에서 JOIN하는 테이블들은 모두 CTE)
                join_tables[table_alias_join] = table_alias_join
            elif isinstance(join_table, exp.Subquery):
                if join_table.alias:
                    table_alias_join = join_table.alias.name
                    join_tables[table_alias_join] = table_alias_join
    elif from_name is not None:
        # 기본 규칙: from 절은 우선 key값(or 이전 레이어 key)
        select_expr.set(
            "from", exp.From(this=exp.Table(this=exp.to_identifier(from_name)))
        )
    
    # 모든 컬럼 참조에 테이블 alias 추가 (ambiguous 에러 방지)
    # proj layer는 단일 테이블이므로 alias 불필요, agg/deriv는 필요
    # SELECT 절의 컬럼과 테이블 alias 매핑 생성 (GROUP BY에서 사용하기 위해)
    select_column_table_map = {}  # {column_name: table_alias}
    
    if add_table_alias:
        if joins is not None and join_tables:
            # JOIN이 있는 경우, 각 컬럼이 어느 테이블에 속하는지 판단
            for metric in metrics:
                _add_table_alias_to_columns_with_joins(
                    metric, join_tables, joins, original_smq, semantic_manifest
                )
        elif table_alias:
            # JOIN이 없는 경우, 단일 테이블 alias 사용
            for metric in metrics:
                _add_table_alias_to_columns(metric, table_alias)
    
    # SELECT 절의 컬럼과 테이블 alias 매핑 생성 (테이블 alias 추가 후)
    # SELECT 절의 모든 컬럼을 확인하여 매핑 생성
    for metric in metrics:
        _build_column_table_map(metric, select_column_table_map)
    
    # SELECT 절의 expressions에서 직접 컬럼 추출 (더 정확한 방법)
    if metrics:
        for metric in metrics:
            # Column인 경우
            if isinstance(metric, exp.Column):
                if metric.name and metric.table:
                    table_name = metric.table.name if isinstance(metric.table, exp.Identifier) else str(metric.table)
                    select_column_table_map[metric.name] = table_name
            # Alias인 경우
            elif isinstance(metric, exp.Alias):
                # Alias 이름과 컬럼 이름 모두 매핑
                alias_name = None
                if metric.alias:
                    alias_name = metric.alias.name if isinstance(metric.alias, exp.Identifier) else str(metric.alias)
                
                # Alias의 this가 Column인 경우
                if isinstance(metric.this, exp.Column):
                    if metric.this.name and metric.this.table:
                        table_name = metric.this.table.name if isinstance(metric.this.table, exp.Identifier) else str(metric.this.table)
                        select_column_table_map[metric.this.name] = table_name
                        if alias_name:
                            select_column_table_map[alias_name] = table_name
                # Alias의 this 내부에 Column이 있는 경우 (집계 함수 등)
                else:
                    for col in metric.this.find_all(exp.Column):
                        if col.name and col.table:
                            table_name = col.table.name if isinstance(col.table, exp.Identifier) else str(col.table)
                            select_column_table_map[col.name] = table_name
                    if alias_name:
                        # alias 이름은 집계 함수의 결과이므로, 첫 번째 컬럼의 테이블 사용 (임시)
                        # 실제로는 alias 이름은 GROUP BY에서 사용되지 않을 수 있음
                        pass
    
    # dimension → metric 순서로 정렬 (모든 레이어에 적용)
    if metrics:
        metrics = _sort_metrics_dimension_first(metrics)
    
    if metrics:
        select_expr.set("expressions", metrics)

    # WHERE 절 (filters는 이미 위에서 수집됨)
    if filters:
        # 여러 개면 AND 로 묶어준다.
        where_condition = None
        qualify_condition = None
        for f in filters:
            # 모든 컬럼 참조에 테이블 alias 추가
            if add_table_alias:
                if joins is not None and join_tables:
                    # JOIN이 있는 경우, 각 컬럼이 어느 테이블에 속하는지 판단
                    _add_table_alias_to_columns_with_joins(
                        f, join_tables, joins, original_smq, semantic_manifest
                    )
                elif table_alias:
                    # JOIN이 없는 경우, 단일 테이블 alias 사용
                    _add_table_alias_to_columns(f, table_alias)
            
            # 문제 1 해결: OR 조건이 포함된 필터를 괄호로 감싸기
            wrapped_filter = f
            if f.find(exp.Or) and not isinstance(f, exp.Paren):
                # OR가 포함되어 있고 이미 괄호로 감싸져 있지 않으면 괄호로 감싸기
                wrapped_filter = exp.Paren(this=f)
            
            if f.find(exp.Window) or (
                table_alias == "agg"
                and f.this.name
                in [node.alias for node in select_expr.find_all(exp.Alias)]
            ):
                qualify_condition = (
                    wrapped_filter
                    if qualify_condition is None
                    else exp.And(this=qualify_condition, expression=wrapped_filter)
                )
            else:
                where_condition = (
                    wrapped_filter
                    if where_condition is None
                    else exp.And(this=where_condition, expression=wrapped_filter)
                )
        if qualify_condition:
            select_expr.set("qualify", exp.Qualify(this=qualify_condition))
        if where_condition:
            select_expr.set("where", exp.Where(this=where_condition))

    # GROUP BY
    if groups:
        # 모든 컬럼 참조에 테이블 alias 추가
        if add_table_alias:
            # GROUP BY 절의 컬럼에 SELECT 절과 동일한 테이블 alias 적용
            # SELECT 절의 expressions를 직접 확인하여 매핑 생성 (가장 정확한 방법)
            select_expressions = select_expr.expressions if select_expr.expressions else []
            select_metrics_map = {}  # {column_name: table_alias}
            
            for expr in select_expressions:
                # Column인 경우
                if isinstance(expr, exp.Column):
                    if expr.name and expr.table:
                        table_name = expr.table.name if isinstance(expr.table, exp.Identifier) else str(expr.table)
                        select_metrics_map[expr.name] = table_name
                # Alias인 경우 (this가 Column)
                elif isinstance(expr, exp.Alias) and isinstance(expr.this, exp.Column):
                    if expr.this.name and expr.this.table:
                        table_name = expr.this.table.name if isinstance(expr.this.table, exp.Identifier) else str(expr.this.table)
                        select_metrics_map[expr.this.name] = table_name
                # 집계 함수 등 내부에 Column이 있는 경우
                else:
                    for col in expr.find_all(exp.Column):
                        if col.name and col.table:
                            table_name = col.table.name if isinstance(col.table, exp.Identifier) else str(col.table)
                            select_metrics_map[col.name] = table_name
            
            # select_column_table_map과 select_metrics_map을 병합 (select_metrics_map 우선)
            combined_map = {**select_column_table_map, **select_metrics_map}
            
            for group_expr in groups:
                # 먼저 combined_map에서 찾기
                has_unmapped_columns = False
                for col in group_expr.find_all(exp.Column):
                    if col.table:
                        continue  # 이미 테이블 alias가 있으면 건너뜀
                    
                    column_name = col.name
                    # SELECT 절에서 동일한 컬럼 이름 찾기 (combined_map 우선 사용)
                    if column_name in combined_map:
                        col.set("table", exp.Identifier(this=combined_map[column_name]))
                    else:
                        # 매핑에서 찾지 못한 컬럼이 있음
                        has_unmapped_columns = True
                
                # combined_map에서 찾지 못한 컬럼이 있는 경우, 추가 처리
                if has_unmapped_columns:
                    if joins is not None and join_tables:
                        # JOIN이 있는 경우, 각 컬럼이 어느 테이블에 속하는지 판단
                        _add_table_alias_to_columns_with_joins(
                            group_expr, join_tables, joins, original_smq, semantic_manifest
                        )
                    elif table_alias:
                        # JOIN이 없는 경우, 단일 테이블 alias 사용
                        for col in group_expr.find_all(exp.Column):
                            if not col.table:
                                col.set("table", exp.Identifier(this=table_alias))
        group = exp.Group(expressions=groups)
        select_expr.set("group", group)

    # ORDER BY
    orders: List[exp.Expression] = smq_of_layer.get("orders") or []
    if orders:
        # 모든 컬럼 참조에 테이블 alias 추가
        if add_table_alias:
            for order_expr in orders:
                if joins is not None and join_tables:
                    # JOIN이 있는 경우, 각 컬럼이 어느 테이블에 속하는지 판단
                    _add_table_alias_to_columns_with_joins(
                        order_expr, join_tables, joins, original_smq, semantic_manifest
                    )
                elif table_alias:
                    # JOIN이 없는 경우, 단일 테이블 alias 사용
                    _add_table_alias_to_columns(order_expr, table_alias)
        order = exp.Order(expressions=orders)
        select_expr.set("order", order)

    # LIMIT
    limit: Optional[exp.Expression] = smq_of_layer.get("limit", None)
    if limit is not None:
        select_expr.set("limit", exp.Limit(expression=exp.Literal.number(limit)))

    return select_expr


def _add_table_alias_to_columns(expression: exp.Expression, table_name: str) -> None:
    """
    표현식 내의 모든 컬럼 참조에 테이블 alias를 추가합니다.
    이미 테이블 alias가 있는 컬럼은 건너뜁니다.
    """
    if expression is None:
        return
    
    for col in expression.find_all(exp.Column):
        # 이미 테이블 alias가 있으면 건너뜀
        if col.table:
            continue
        # 테이블 alias 추가
        col.set("table", exp.Identifier(this=table_name))


def _add_table_alias_to_columns_with_joins(
    expression: exp.Expression, 
    join_tables: Dict[str, str], 
    joins: List[exp.Join],
    original_smq: Optional[Dict[str, Any]] = None,
    semantic_manifest: Optional[Dict[str, Any]] = None,
) -> None:
    """
    JOIN이 있을 때 표현식 내의 모든 컬럼 참조에 올바른 테이블 alias를 추가합니다.
    각 컬럼이 어느 테이블에 속하는지 다음 순서로 찾습니다:
    1. 원본 SMQ에서 table__column 형식으로 찾기
    2. Semantic manifest에서 각 테이블의 dimension/measure 확인
    3. JOIN 절의 ON 조건에서 찾기
    """
    if expression is None:
        return
    
    for col in expression.find_all(exp.Column):
        # 이미 테이블 alias가 있으면 건너뜀
        if col.table:
            continue
        
        column_name = col.name
        found_table = None
        
        # 1. 원본 SMQ에서 table__column 형식으로 찾기 (가장 정확한 방법)
        if original_smq:
            found_table = find_table_of_column_from_original_smq(column_name, original_smq)
            if found_table:
                # found_table이 join_tables에 있는지 확인
                if found_table in join_tables:
                    col.set("table", exp.Identifier(this=found_table))
                    continue
                # found_table이 join_tables의 값(semantic model 이름)과 일치하는지 확인
                for table_alias, table_name in join_tables.items():
                    if found_table == table_name:
                        col.set("table", exp.Identifier(this=table_alias))
                        found_table = table_alias  # found_table 업데이트
                        break
                if found_table and found_table in join_tables:
                    continue
        
        # 2. Semantic manifest에서 각 테이블의 dimension/measure 확인
        # 모든 테이블을 확인하여 정확히 일치하는 테이블 찾기
        if semantic_manifest and not found_table:
            matching_tables = []
            for table_alias in join_tables.keys():
                # table_alias는 CTE 이름 = semantic model 이름
                table_name = join_tables[table_alias]
                
                # dimension 또는 measure 확인
                dimension = find_dimension_by_name(table_name, column_name, semantic_manifest)
                measure = find_measure_by_name(table_name, column_name, semantic_manifest)
                
                if dimension or measure:
                    matching_tables.append(table_alias)
            
            # metric의 expr에서도 확인 (metric expr에 table__column 형식이 포함될 수 있음)
            if not matching_tables:
                metrics = semantic_manifest.get("metrics", [])
                for metric in metrics:
                    expr = metric.get("expr", "")
                    if expr and f"__{column_name}" in expr:
                        # expr에서 table__column 형식 찾기
                        import sqlglot
                        try:
                            parsed_expr = sqlglot.parse_one(expr)
                            # expr 내의 모든 컬럼 찾기
                            for col in parsed_expr.find_all(exp.Column):
                                if col.name and "__" in col.name:
                                    table_name, col_name = col.name.split("__", 1)
                                    if col_name == column_name and table_name in join_tables:
                                        matching_tables.append(table_name)
                                        break
                        except:
                            # 파싱 실패 시 문자열로 찾기
                            if f"__{column_name}" in expr:
                                # "table__column" 패턴 찾기
                                import re
                                pattern = rf"(\w+)__{re.escape(column_name)}"
                                matches = re.findall(pattern, expr)
                                for match in matches:
                                    if match in join_tables:
                                        matching_tables.append(match)
                                        break
            
            # 정확히 하나의 테이블에서만 찾은 경우에만 사용
            if len(matching_tables) == 1:
                found_table = matching_tables[0]
            elif len(matching_tables) > 1:
                # 여러 테이블에서 찾은 경우, 첫 번째 사용 (나중에 개선 가능)
                found_table = matching_tables[0]
        
        # 3. JOIN 절의 ON 조건에서 해당 컬럼이 사용되는 테이블 찾기
        if not found_table:
            for join in joins:
                on_condition = join.args.get("on")
                if on_condition:
                    # ON 조건에서 해당 컬럼 이름을 가진 컬럼 찾기
                    for on_col in on_condition.find_all(exp.Column):
                        if on_col.name == column_name and on_col.table:
                            # 해당 컬럼이 사용되는 테이블 찾기
                            table_alias = on_col.table.name if isinstance(on_col.table, exp.Identifier) else str(on_col.table)
                            if table_alias in join_tables:
                                found_table = table_alias
                                break
                    if found_table:
                        break
        
        # 4. 위 방법으로 찾지 못한 경우, 첫 번째 테이블 사용 (기본값)
        if not found_table and join_tables:
            found_table = list(join_tables.keys())[0]
        
        if found_table:
            col.set("table", exp.Identifier(this=found_table))


def _remove_table_alias_from_columns(expression: exp.Expression) -> None:
    """
    표현식 내의 모든 컬럼 참조에서 테이블 alias를 제거합니다.
    최종 SELECT에서 단일 CTE/테이블을 참조할 때 사용합니다.
    """
    if expression is None:
        return
    
    for col in expression.find_all(exp.Column):
        # 테이블 alias 제거
        if col.table:
            col.set("table", None)


def _build_column_table_map(expression: exp.Expression, column_table_map: Dict[str, str]) -> None:
    """
    표현식 내의 컬럼과 테이블 alias 매핑을 생성합니다.
    SELECT 절의 컬럼들을 GROUP BY에서 재사용하기 위해 사용합니다.
    """
    if expression is None:
        return
    
    # Alias가 있는 경우 alias 이름도 매핑에 추가
    alias_name = None
    if isinstance(expression, exp.Alias) and expression.alias:
        alias_name = expression.alias.name if isinstance(expression.alias, exp.Identifier) else str(expression.alias)
    
    # 표현식 내의 모든 컬럼 찾기
    for col in expression.find_all(exp.Column):
        if col.name and col.table:
            table_name = col.table.name if isinstance(col.table, exp.Identifier) else str(col.table)
            # 컬럼 이름과 테이블 alias 매핑
            column_table_map[col.name] = table_name
            # Alias가 있는 경우 alias 이름도 매핑
            if alias_name:
                column_table_map[alias_name] = table_name


def _apply_table_alias_from_map(expression: exp.Expression, column_table_map: Dict[str, str]) -> bool:
    """
    표현식 내의 컬럼에 SELECT 절의 컬럼과 동일한 테이블 alias를 적용합니다.
    GROUP BY 절에서 SELECT 절의 컬럼과 동일한 테이블 alias를 사용하기 위해 사용합니다.
    
    Returns:
        bool: 매핑이 적용되었는지 여부
    """
    if expression is None:
        return False
    
    applied = False
    for col in expression.find_all(exp.Column):
        # 이미 테이블 alias가 있으면 건너뜀
        if col.table:
            continue
        
        column_name = col.name
        if column_name and column_name in column_table_map:
            col.set("table", exp.Identifier(this=column_table_map[column_name]))
            applied = True
    
    return applied


def _sort_metrics_dimension_first(metrics: List[exp.Expression]) -> List[exp.Expression]:
    """
    metrics 리스트를 dimension → metric 순서로 정렬합니다.
    분류 기준:
    - dimension: 집계 함수(SUM, COUNT, AVG, MAX, MIN)나 윈도우 함수가 없고, 다른 metric을 참조하지 않는 표현식
    - metric: 집계 함수나 윈도우 함수가 포함된 표현식, 또는 다른 metric(집계 함수 결과)을 참조하는 표현식
    """
    # 먼저 모든 metric의 alias 이름을 수집 (다른 metric을 참조하는지 확인하기 위해)
    metric_aliases = set()
    for metric in metrics:
        if isinstance(metric, exp.Alias) and metric.alias:
            alias_name = metric.alias.name if isinstance(metric.alias, exp.Identifier) else str(metric.alias)
            if alias_name:
                metric_aliases.add(alias_name)
        # 집계 함수가 포함된 경우, alias가 없어도 metric으로 분류됨
    
    dimensions = []
    aggregated_metrics = []
    
    for metric in metrics:
        # 집계 함수나 윈도우 함수가 포함되어 있는지 확인
        has_agg = metric.find(AGGREGATION_EXPRESSIONS) is not None
        has_window = metric.find(exp.Window) is not None
        
        # 다른 metric alias를 참조하는지 확인
        references_metric = False
        if not (has_agg or has_window):
            # 표현식 내의 모든 Identifier를 확인하여 metric alias를 참조하는지 확인
            for ident in metric.find_all(exp.Identifier):
                if ident.name and ident.name in metric_aliases:
                    references_metric = True
                    break
            # Column도 확인 (alias된 metric을 참조할 수 있음)
            for col in metric.find_all(exp.Column):
                if col.name and col.name in metric_aliases:
                    references_metric = True
                    break
        
        if has_agg or has_window or references_metric:
            aggregated_metrics.append(metric)
        else:
            dimensions.append(metric)
    
    # dimension 먼저, 그 다음 aggregated metrics
    return dimensions + aggregated_metrics
