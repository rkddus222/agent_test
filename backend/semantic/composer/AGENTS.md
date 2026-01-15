# SMQ Composer 구조 분석

## 개요

`service/semantic/composer` 모듈은 파싱된 SMQ를 SQL 쿼리로 변환하는 역할을 합니다. 이 문서는 `composer.py`와 각 파이프라인 단계의 구조와 동작 방식을 설명합니다.

> **참고**: 이 문서는 **파싱된 SMQ → SQL** 변환에 초점을 맞춥니다.  
> SMQ를 파싱하는 과정은 `service/semantic/parser/AGENTS.md` 문서를 참고해 주세요.

## 전체 아키텍처

```
파싱된 SMQ 입력
  ↓
SQLComposer.compose()
  ├─ [임시] Dimension 식을 deriv 레이어로 이동
  ├─ [Deriv] Agg 함수를 agg 레이어로 push down
  ├─ [임시] GroupBy를 Metrics에 추가
  ├─ [전체] SMQ 완전성 검증 및 보완
  ├─ [전체] Anonymous 노드를 legit 노드로 변환
  ├─ [전체] Subquery의 FROM 절을 실제 테이블로 변환
  ├─ [Deriv] Deriv 레이어 전제조건 확인 및 완성
  ├─ [Agg] Group/Select 일치 확인 및 완성
  ├─ [Agg] Agg 레이어 전제조건 확인 및 완성
  ├─ [Agg] Default JOIN 추가
  ├─ [전체] Alias 추가 (식에 alias가 없는 경우)
  ├─ [전체] BigQuery backtick 추가 (BigQuery인 경우)
  ├─ [전체] BigQuery special char 치환 (BigQuery인 경우)
  └─ [전체] SQL 작성
  ↓
SQL 출력
```

## 핵심 설계 원칙

**SQLComposer의 설계 원칙**:
1. **파이프라인 방식**: 여러 단계를 순차적으로 거쳐 SQL 생성
2. **자동 완성**: 누락된 항목을 자동으로 추가하여 완전한 SQL 생성
3. **레이어별 처리**: Proj → Agg → Deriv 순서로 하위 레이어부터 처리
4. **Dialect 지원**: BigQuery 등 다양한 SQL dialect 지원

## SQLComposer 클래스

**위치**: `service/semantic/composer/composer.py`

**역할**: 파싱된 SMQ를 SQL 쿼리로 변환

**주요 메서드**:

### `compose(parsed_smq, original_smq) -> str`

파싱된 SMQ를 SQL 쿼리로 변환하는 핵심 메서드입니다.

**처리 흐름** (```36:101:service/semantic/composer/composer.py```):

1. **Dimension 식 이동** (임시 처리)
2. **Agg 함수 push down** (Deriv 레이어)
3. **GroupBy를 Metrics에 추가** (임시 처리)
4. **SMQ 완전성 검증**
5. **Anonymous 노드 변환**
6. **Subquery FROM 절 변환**
7. **Deriv 레이어 전제조건 확인**
8. **Group/Select 일치 확인**
9. **Agg 레이어 전제조건 확인**
10. **Default JOIN 추가**
11. **Alias 추가**
12. **BigQuery 처리** (backtick, special char)
13. **SQL 작성**

## 파이프라인 단계별 상세 분석

### 1. move_dimension_expr_to_deriv_layer_if_deriv_exists

**위치**: `service/semantic/composer/pipeline/move_dimension_expr_to_deriv_layer_if_deriv_exists.py`

**역할**: Deriv 레이어가 있는 경우, agg 레이어의 dimension 식을 deriv 레이어로 이동

**복잡도**: ⭐⭐

**처리 흐름**:

```6:43:service/semantic/composer/pipeline/move_dimension_expr_to_deriv_layer_if_deriv_exists.py
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
```

**처리 로직**:
- Deriv 레이어가 없으면 스킵
- Agg 레이어의 metrics 중:
  - 집계 함수가 포함된 노드
  - Original SMQ의 metric과 일치하는 노드 (AggFunc 또는 Window)
- 해당 노드를 deriv 레이어로 이동

### 2. push_down_agg_from_deriv_layer

**위치**: `service/semantic/composer/pipeline/push_down_agg_from_deriv_layer.py`

**역할**: Deriv 레이어에 있는 집계 함수를 agg 레이어로 push down

**복잡도**: ⭐⭐⭐⭐

**처리 흐름**:

```21:112:service/semantic/composer/pipeline/push_down_agg_from_deriv_layer.py
def push_down_agg_from_deriv_layer(parsed_smq, original_smq, semantic_manifest):
    # deriv layer가 없으면 그냥 return
    if "deriv" not in parsed_smq:
        return parsed_smq

    deriv_layer = parsed_smq["deriv"]
    deriv_metrics = deriv_layer.get("metrics", [])
    deriv_orders = deriv_layer.get("orders", [])
    deriv_nodes = deriv_metrics + deriv_orders

    # 주의) metric이 없으면 그냥 return
    if not deriv_nodes:
        return parsed_smq

    for node in deriv_nodes:
        node_with_agg_func = list(node.find_all(exp.AggFunc))
        if node_with_agg_func:
            for node in node_with_agg_func:
                if isinstance(node.parent, exp.Window):
                    continue
                agg_func = node.key.lower()
                agg_name = name_for_agg_function_map.get(agg_func, "기타")
                deriv_metric_name = ""
                for column_node in node.find_all(exp.Column):
                    deriv_metric_name += f"{column_node.name}_"
                deriv_metric_name += agg_name
                # 여기서부턴 agg layer에다가 새로 추가하는 로직
                deepcopied_node = deepcopy(node)
                is_inner_node_metric = False
                for inner_node in deepcopied_node.find_all(exp.Column):
                    metric_of_inner_node = find_metric_by_name(
                        inner_node.name, semantic_manifest
                    )

                    if metric_of_inner_node:
                        is_inner_node_metric = True

                    if not metric_of_inner_node:
                        table_name = find_table_of_column_from_original_smq(
                            inner_node.name, original_smq
                        )
                        metric_of_inner_node = find_measure_by_name(
                            table_name, inner_node.name, semantic_manifest
                        )

                    if not metric_of_inner_node:
                        metric_of_inner_node = find_dimension_by_name(
                            table_name, inner_node.name, semantic_manifest
                        )

                    if metric_of_inner_node is None:
                        raise ValueError(
                            f"Metric/Measure/Dimension을 찾을 수 없습니다. "
                            f"inner_node.name='{inner_node.name}', table_name='{table_name}' "
                            f"(push_down_agg_from_deriv_layer 중, deriv_metric_name='{deriv_metric_name}' 처리 중)"
                        )
                    expr_of_inner_node_metric = metric_of_inner_node.get("expr", None)

                    if not is_inner_node_metric:
                        parsed_smq["deriv"]["metrics"].remove(node)
                        parsed_smq = append_node(
                            parsed_smq,
                            "deriv",
                            "metrics",
                            exp.Column(this=exp.Identifier(this=deriv_metric_name)),
                        )

                    if is_inner_node_metric and expr_of_inner_node_metric:
                        parsed_expr = sqlglot.parse_one(expr_of_inner_node_metric)
                        for ident in parsed_expr.find_all(exp.Identifier):
                            ident_name = ident.name
                            if "__" in ident_name:
                                _, ident_name_wo_table_name = ident_name.split("__", 1)
                                ident.replace(
                                    exp.Identifier(this=ident_name_wo_table_name)
                                )
                        inner_node.replace(parsed_expr)

                parsed_smq = append_node(
                    parsed_smq,
                    "agg",
                    "metrics",
                    exp.Alias(
                        this=deepcopied_node,
                        alias=exp.Identifier(this=deriv_metric_name),
                    ),
                )

                # 주의) 모든 작업이 끝난 후 deriv layer에서 해당 layer를 교체해 줍니다.
                node.replace(exp.Identifier(this=deriv_metric_name))

    return parsed_smq
```

**처리 로직**:
1. Deriv 레이어의 metrics와 orders에서 집계 함수 찾기
2. 윈도우 함수는 제외 (윈도우 함수는 deriv 레이어에 유지)
3. 집계 함수 내부의 컬럼 확인:
   - Metric이면 metric의 expr로 치환
   - Measure/Dimension이면 그대로 유지
4. Agg 레이어에 새로운 metric 추가 (집계 함수 + alias)
5. Deriv 레이어의 원본 노드를 새로 생성한 metric 이름으로 교체

**예시**:
```python
# 입력: deriv 레이어에 "SUM(revenue)" 
# 처리: 
#   1. Agg 레이어에 "SUM(revenue) AS revenue_합계" 추가
#   2. Deriv 레이어의 "SUM(revenue)"를 "revenue_합계"로 교체
```

### 3. move_groups_to_metrics

**위치**: `service/semantic/composer/pipeline/move_groups_to_metrics.py`

**역할**: GroupBy에 있는 항목을 최상위 레이어의 Metrics에도 추가

**복잡도**: ⭐

**처리 흐름**:

```4:23:service/semantic/composer/pipeline/move_groups_to_metrics.py
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
```

**처리 로직**:
- 최상위 레이어(deriv 또는 agg) 결정
- Agg 레이어의 groups 확인
- 최상위 레이어의 metrics에 없으면 추가

### 4. check_if_original_smq_included_and_complete_if_not

**위치**: `service/semantic/composer/pipeline/check_if_original_smq_included_and_complete_if_not.py`

**역할**: Original SMQ의 metrics가 파싱된 SMQ에 모두 포함되어 있는지 확인하고, 누락된 항목 추가

**복잡도**: ⭐⭐⭐

**처리 흐름**:

```7:57:service/semantic/composer/pipeline/check_if_original_smq_included_and_complete_if_not.py
def check_if_original_smq_included_and_complete_if_not(parsed_smq, original_smq):
    """original smq의 metrics에 대해서 parsed_smq에 다 포함되어 있는지 확인하고,누락된 항목이 있으면 추가합니다.
    해당 metric들이 하위 레이어에 포함되어 있는지는 여기서 체크하지 않고, check_derive_prerequisite_of_* 에서 체크합니다
    추후 metrics 이외의 항목에 대해서도 체크가 필요할 시 이 pipeline에서 체크합니다."""

    # 0) 최상단 레이어를 확인합니다.
    uppermost_layer = "deriv" if "deriv" in parsed_smq else "agg"

    # 1) original smq의 metrics를 가져와, parsed_smq의 uppermost layer와 비교합니다. 없는 것들은 not_included_metrics에 담습니다.
    not_included_metrics = []

    uppermost_metrics = parsed_smq[uppermost_layer].get("metrics", [])
    stringfied_uppermost_metrics = [metric.sql() for metric in uppermost_metrics] + [
        metric.alias for metric in uppermost_metrics if isinstance(metric, exp.Alias)
    ]

    original_metrics = original_smq.get("metrics", [])
    for metric in original_metrics:
        if metric not in stringfied_uppermost_metrics:
            not_included_metrics.append(metric)

    # 2) not_included_metric을 uppermost_layer에 추가합니다.metric이면 그대로, measure나 dimension이면 table_name을 떼고 추가합니다.
    for metric in not_included_metrics:
        parsed_metric = sqlglot.parse_one(metric)

        # 2-1) metric이 식인 경우
        if not isinstance(parsed_metric, (exp.Column, exp.Identifier)):
            node_to_append = parsed_metric
            for ident in parsed_metric.find_all(exp.Identifier):
                if "__" in ident.name:
                    _, column_name = ident.name.split("__", 1)
                    ident.replace(exp.Identifier(this=column_name))
            parsed_smq = append_node(
                parsed_smq, uppermost_layer, "metrics", node_to_append
            )
            continue

        # 2-2) metric이 column이나 identifier인 경우 = 식이 아닌 경우
        if "__" in metric:
            _, column_name = metric.split("__", 1)
            node_to_append = exp.Column(this=exp.Identifier(this=column_name))
            parsed_smq = append_node(
                parsed_smq, uppermost_layer, "metrics", node_to_append
            )
        else:
            node_to_append = sqlglot.parse_one(metric)
            parsed_smq = append_node(
                parsed_smq, uppermost_layer, "metrics", node_to_append
            )

    return parsed_smq
```

**처리 로직**:
1. 최상위 레이어(deriv 또는 agg) 결정
2. Original SMQ의 metrics와 최상위 레이어의 metrics 비교
3. 누락된 metrics 추가:
   - 식인 경우: `table__column` 형식의 identifier를 `column`으로 변환
   - 컬럼인 경우: `table__column` 형식이면 `column`만 추출

### 5. transform_anonymous_node_into_legit_one

**위치**: `service/semantic/composer/pipeline/tansform_anonymous_node_into_legit_one.py`

**역할**: Anonymous 노드(예: `AVG()`, `SUM()`)를 legit한 노드(예: `exp.Avg`, `exp.Sum`)로 변환

**복잡도**: ⭐⭐

**처리 흐름**:

```18:34:service/semantic/composer/pipeline/tansform_anonymous_node_into_legit_one.py
def transform_anonymous_node_into_legit_one(parsed_smq):
    for layer in parsed_smq.values():
        for dsl_nodes in layer.values():
            # key가 limit인 경우 int인데, 이런 경우 continue!
            if type(dsl_nodes) is not list:
                continue
            for node in dsl_nodes:
                anonymous_nodes = list(node.find_all(exp.Anonymous))
                if anonymous_nodes:
                    for anon_node in anonymous_nodes:
                        func_name = anon_node.this.upper()
                        if func_name in FUNCTION_MAP:
                            legit_class = FUNCTION_MAP[func_name]
                            new_node = legit_class(this=anon_node.expressions)
                            anon_node.replace(new_node)

    return parsed_smq
```

**처리 로직**:
- 모든 레이어의 모든 노드에서 Anonymous 노드 찾기
- FUNCTION_MAP에 정의된 함수면 해당 클래스로 변환
- 지원 함수: AVG, SUM, MAX, MIN, COUNT 등

### 6. replace_from_with_real_table_in_subqueries

**위치**: `service/semantic/composer/pipeline/replace_from_with_real_table_in_subqueries.py`

**역할**: Subquery 안의 FROM 절이 deriv/agg가 아니면 실제 물리 테이블로 변환

**복잡도**: ⭐⭐⭐

**처리 흐름**:

```6:39:service/semantic/composer/pipeline/replace_from_with_real_table_in_subqueries.py
def replace_from_with_real_table_in_subqueries(parsed_smq, semantic_manifest, dialect):
    # 현재는 filter에만 적용합니다
    for layer in parsed_smq:
        for key in parsed_smq[layer]:
            if key == "limit":
                continue
            for node in parsed_smq[layer][key]:
                for subquery in node.find_all(exp.Subquery):
                    select = subquery.this
                    # 1) subquery의 다른 칼럼 중에 앞에 from의 테이블 네임이 붙은 게 있으면 제거해 줍니다
                    from_clause = select.args.get("from")
                    if not from_clause:
                        continue
                    this_arg = from_clause.args.get("this")
                    if not this_arg:
                        pass
                    from_table_name = this_arg.name
                    for column in select.find_all(exp.Column):
                        table_name, column_name = (
                            column.name.split("__", 1)
                            if "__" in column.name
                            else (None, None)
                        )
                        if table_name == from_table_name:
                            column.replace(
                                exp.Column(this=exp.Identifier(this=column_name))
                            )

                    # 2) from을 실제 테이블 네임으로 교체해 줍니다
                    new_select = replace_from_with_real_table(
                        select, semantic_manifest, dialect
                    )
                    subquery.set("this", new_select)
    return parsed_smq
```

**처리 로직**:
1. 모든 레이어의 모든 노드에서 Subquery 찾기
2. Subquery의 FROM 절 확인:
   - FROM 절의 테이블명과 일치하는 컬럼의 `table__column` 형식을 `column`으로 변환
   - FROM 절을 실제 물리 테이블로 변환 (`replace_from_with_real_table` 사용)

### 7. check_prerequisite_of_deriv_layer_and_complete

**위치**: `service/semantic/composer/pipeline/check_prerequisite_of_derive_layer.py`

**역할**: Deriv 레이어에 필요한 하위 항목들이 모두 포함되어 있는지 확인하고, 누락된 항목을 agg 레이어에서 찾아 추가

**복잡도**: ⭐⭐⭐⭐

**처리 흐름**:

```12:138:service/semantic/composer/pipeline/check_prerequisite_of_derive_layer.py
def check_prerequisite_of_deriv_layer_and_complete(
    parsed_smq, original_smq, semantic_manifest, dialect
):
    """Deriv Layer에 필요한 하위 항목들이 다 들어 있는지 확인합니다."""
    if "deriv" not in parsed_smq:
        return parsed_smq

    deriv_nodes_dict = parsed_smq["deriv"]

    # 1) derive_nodes에 있는 모든 칼럼들을 모읍니다.
    deriv_nodes_list = []
    for key in deriv_nodes_dict:
        if key != "limit":
            deriv_nodes_list += deriv_nodes_dict[key]

    nodes_to_check_prerequisite = []

    for node in deriv_nodes_list:
        columns = node.find_all(exp.Column)
        if not columns:
            continue
        for column in columns:
            if column.name not in [node.name for node in nodes_to_check_prerequisite]:
                nodes_to_check_prerequisite.append(column)

    # 2) 각 칼럼들이 metric인지 dimension인지 판단합니다.
    for node in nodes_to_check_prerequisite:
        metric = find_metric_by_name(node.name, semantic_manifest)

        # 2-1) metric이면 agg에 expr AS alias가 있는지 확인하고, 없으면 추가합니다. (expr column들이 proj에 있는지는 check_prerequisited_of_agg_...에서 확인)
        if metric:
            agg_layer_aliases_in_str = [
                node.alias
                for node in parsed_smq["agg"].get("metrics", [])
                if node.alias
            ]
            if metric["name"] in agg_layer_aliases_in_str:
                continue

            else:
                parsed_metric_expr = sqlglot.parse_one(metric["expr"], read=dialect)
                for ident in parsed_metric_expr.find_all(exp.Identifier):
                    # 먼저 metric인지 확인
                    ident_metric = find_metric_by_name(ident.name, semantic_manifest)
                    if ident_metric:
                        # metric인 경우는 이미 다른 곳에서 처리되므로 건너뜁니다.
                        # identifier 이름은 그대로 유지합니다.
                        continue
                    
                    # metric이 아니면 table__column 형식이어야 합니다.
                    if "__" not in ident.name:
                        raise ValueError(
                            f"Metric expr에 사용된 identifier '{ident.name}'이 잘못되었습니다. "
                            f"'table__column' 형식이어야 하거나, semantic manifest에 정의된 metric이어야 합니다. "
                            f"(metric '{metric['name']}'의 expr 처리 중)"
                        )
                    
                    table_name, column_name = ident.name.split("__", 1)
                    ident.replace(exp.Identifier(this=column_name))
                    # 주의) 여기서 해당 measure를 proj layer에 추가합니다!
                    column = find_measure_by_name(
                        table_name, column_name, semantic_manifest
                    )
                    if not column:
                        column = find_dimension_by_name(
                            table_name, column_name, semantic_manifest
                        )
                    if column:
                        expr = column.get("expr")
                        if expr:
                            parsed_column_expr = sqlglot.parse_one(expr, read=dialect)
                            parsed_smq = append_node(
                                parsed_smq,
                                table_name,
                                "metrics",
                                exp.Alias(
                                    this=parsed_column_expr,
                                    alias=exp.Identifier(this=column_name),
                                ),
                            )

                    else:
                        parsed_smq = append_node(
                            parsed_smq,
                            table_name,
                            "metrics",
                            exp.Column(this=exp.Identifier(this=column_name)),
                        )

                parsed_smq = append_node(
                    parsed_smq,
                    "agg",
                    "metrics",
                    exp.Alias(
                        this=parsed_metric_expr,
                        alias=exp.Identifier(this=metric["name"]),
                    ),
                )

        # 2-2) node.name이 alias인 경우는 건너뜁니다.
        elif node.name in [node.alias for node in deriv_nodes_list if node.alias]:
            continue

        # 2-3) dimension이면 agg에 dimension이 있는지 확인하고, 없으면 추가합니다.
        else:
            agg_layer_columns_in_str = [
                node.name for node in parsed_smq["agg"].get("metrics", []) if node.name
            ] + [
                node.alias
                for node in parsed_smq["agg"].get("metrics", [])
                if node.alias
            ]

            node_name = node.name if node.name else node.this.name

            if node_name in agg_layer_columns_in_str:
                continue
            else:
                parsed_smq = append_node(
                    parsed_smq,
                    "agg",
                    "metrics",
                    exp.Column(this=exp.Identifier(this=node_name)),
                )
            # 주의) agg layer에 없는 걸 proj layer에 추가하는 건 아마 필요 없는 듯합니다..?

    return parsed_smq
```

**처리 로직**:
1. Deriv 레이어의 모든 컬럼 수집
2. 각 컬럼 확인:
   - **Metric인 경우**: Agg 레이어에 해당 metric이 없으면 추가
     - Metric의 expr 내부 컬럼을 proj 레이어에 추가
   - **Dimension인 경우**: Agg 레이어에 해당 dimension이 없으면 추가

### 8. check_group_select_parity_and_complete

**위치**: `service/semantic/composer/pipeline/check_group_select_parity.py`

**역할**: Agg 레이어에서 GROUP BY와 SELECT 절이 일치하는지 확인하고, 집계 함수가 아닌 SELECT 항목을 GROUP BY에 추가

**복잡도**: ⭐⭐

**처리 흐름**:

```6:29:service/semantic/composer/pipeline/check_group_select_parity.py
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
```

**처리 로직**:
- Agg 레이어의 metrics 확인
- 집계 함수가 포함된 metric은 스킵
- 집계 함수가 없는 metric이 groups에 없으면 groups에 추가

### 9. check_prerequisite_of_agg_layer_and_complete

**위치**: `service/semantic/composer/pipeline/check_prerequisite_of_agg_layer.py`

**역할**: Agg 레이어에 필요한 하위 항목들이 모두 포함되어 있는지 확인하고, 누락된 항목을 proj 레이어에서 찾아 추가

**복잡도**: ⭐⭐⭐⭐

**처리 흐름**:

```13:111:service/semantic/composer/pipeline/check_prerequisite_of_agg_layer.py
def check_prerequisite_of_agg_layer_and_complete(
    parsed_smq, original_smq, semantic_manifest, dialect
):
    """Agg Layer에 필요한 하위 항목들이 다 들어 있는지 확인합니다."""
    if "agg" not in parsed_smq:
        return parsed_smq

    agg_layer = parsed_smq["agg"]

    # 1) agg_layer에 있는 모든 칼럼들을 모읍니다.
    agg_nodes_list = []
    for key in agg_layer:
        agg_nodes_list += agg_layer[key]
    nodes_to_check_prerequisite = []
    for node in agg_nodes_list:
        columns = node.find_all(exp.Column)
        if not columns:
            continue
        for column in columns:
            if column.name not in [node.name for node in nodes_to_check_prerequisite]:
                nodes_to_check_prerequisite.append(column)

    # 2) 각 칼럼들이 metric인지 dimension인지 판단합니다.
    for node in nodes_to_check_prerequisite:
        metric = find_metric_by_name(node.name, semantic_manifest)

        # 2-1) metric이면 해당 모델에 expr 안에 있는 칼럼들이 proj_layer에 다 있는지 확인합니다. (alias가 있으면 alias를, name이면 name을 확인합니다.)
        if metric:
            expr = metric.get("expr", None)
            parsed_expr = sqlglot.parse_one(expr, read=dialect)
            for column in parsed_expr.find_all(exp.Column):
                table_name, column_name = column.name.split("__")
                node_to_append = exp.Column(this=exp.Identifier(this=column_name))

                # 2-1-a) 이미 column이 project layer에 있으면 다음 column으로 continue하고
                if table_name in parsed_smq:
                    proj_layer_columns_in_str = [
                        node.name
                        for node in parsed_smq[table_name].get("metrics", [])
                        if node.name
                    ] + [
                        node.alias
                        for node in parsed_smq[table_name].get("metrics", [])
                        if isinstance(node, exp.Alias)
                    ]
                    if column_name in proj_layer_columns_in_str:
                        continue

                # 2-1-b) column이 proj layer에 없으면 추가합니다.
                parsed_smq = append_node(
                    parsed_smq, table_name, "metrics", node_to_append
                )
        # 2-2) measure/dimension이면 모델을 찾아서 있는지 확인하고 없으면 더합니다.
        else:
            column_name = node.name
            table_name = find_table_of_column_from_original_smq(
                column_name, original_smq
            )
            # 주의) metric의 base로 agg에 추가된, 예를 들어서 "SUM(적립금액) AS 일별적립식_총적립금액"과 같은 경우 이 "적립금액" column은 table_name = None이다.
            # 이건 parser에서 잘 추가했으리라 믿고 넘어가 보자!
            if table_name is None:
                continue

            node_to_append = exp.Column(this=exp.Identifier(this=column_name))
            # 2-2-a) 이미 column이 project layer에 있으면 continue하고
            if table_name in parsed_smq:
                proj_layer_columns_in_str = [
                    node.name
                    for node in parsed_smq[table_name].get("metrics", [])
                    if node.name
                ] + [
                    node.alias
                    for node in parsed_smq[table_name].get("metrics", [])
                    if isinstance(node, exp.Alias)
                ]
                if column_name in proj_layer_columns_in_str:
                    continue
            # 2-2-b) column이 proj layer에 없으면 추가합니다.
            column = find_dimension_by_name(table_name, column_name, semantic_manifest)
            if not column:
                column = find_measure_by_name(
                    table_name, column_name, semantic_manifest
                )
            if not column:
                raise ValueError(
                    f"Agg layer에 있는 column {column_name}을(를) semantic manifest에서 찾을 수 없습니다."
                )
            if column.get("expr", None):
                expr = column["expr"]
                parsed_expr = sqlglot.parse_one(expr, read=dialect)
                if parsed_expr.sql() == column_name:
                    node_to_append = exp.Column(this=exp.Identifier(this=column_name))
                else:
                    node_to_append = exp.Alias(
                        this=parsed_expr, alias=exp.Identifier(this=column_name)
                    )
            parsed_smq = append_node(parsed_smq, table_name, "metrics", node_to_append)

    return parsed_smq
```

**처리 로직**:
1. Agg 레이어의 모든 컬럼 수집
2. 각 컬럼 확인:
   - **Metric인 경우**: Metric의 expr 내부 컬럼이 proj 레이어에 없으면 추가
   - **Dimension/Measure인 경우**: Proj 레이어에 없으면 추가
     - `expr`이 있으면 Alias로 추가
     - `expr`이 없으면 Column으로 추가

### 10. add_default_join

**위치**: `service/semantic/composer/pipeline/add_default_join.py`

**역할**: Proj 레이어가 2개 이상인데 agg 레이어에 JOIN이 없으면 자동으로 JOIN 추가

**복잡도**: ⭐⭐⭐⭐⭐ (가장 복잡)

**처리 흐름**:

```9:40:service/semantic/composer/pipeline/add_default_join.py
def add_default_join(parsed_smq, original_smq, semantic_manifest, dialect):

    if original_smq.get("joins"):
        return parsed_smq

    models = list(parsed_smq.keys())
    base_models = [model for model in models if model not in {"agg", "deriv"}]

    if len(base_models) == 1:
        return parsed_smq

    join_sql = generate_join_sql(semantic_manifest, base_models)
    join_node = sqlglot.parse_one(join_sql, dialect=dialect)
    join_columns = join_node.find_all(exp.Column)
    # 만약에 join column이 proj layer에 없으면 추가해 줍니다.
    for col in join_columns:
        table_name = col.table
        column_name = col.this.this
        proj_layer_metrics_in_str = [
            node.name for node in parsed_smq[table_name].get("metrics", []) if node.name
        ]
        if column_name not in proj_layer_metrics_in_str:
            parsed_smq = append_node(
                parsed_smq,
                table_name,
                "metrics",
                exp.Column(this=exp.Identifier(this=column_name)),
            )

    parsed_smq = append_node(parsed_smq, "agg", "joins", join_node)

    return parsed_smq
```

**JOIN 생성 로직** (`generate_join_sql`):

```221:295:service/semantic/composer/pipeline/add_default_join.py
def generate_join_sql(semantic_manifest: list, models: list[str]) -> str:
    """
    모델 이름 리스트로부터 SQL JOIN 절 생성

    Args:
        semantic_models: 전체 semantic model 리스트
        models: 조인할 모델 이름들 (예: ["acct_installment_saving_src", "acct_installment_saving_daily"])

    Returns:
        SQL JOIN 절 문자열 (예: "FROM acct_installment_saving_src A LEFT JOIN acct_installment_saving_daily B ON A.계좌번호 = B.계좌번호")

    Raises:
        JoinError: 모델들을 조인할 수 없는 경우
    """

    semantic_models = semantic_manifest["semantic_models"]

    if not models:
        return ""

    if len(models) == 1:
        return f"FROM {models[0]}"

    # 모델 이름으로 semantic model 조회
    name_to_sm = {sm["name"]: sm for sm in semantic_models}
    sms = []
    for model_name in models:
        sm = name_to_sm.get(model_name)
        if not sm:
            raise ValueError(f"Model '{model_name}' not found in semantic models")
        sms.append(sm)

    # 조인 그래프 구성
    edges, adj = _build_join_graph_and_paths_by_name(sms)
    comps = _connected_components_names(models, adj)

    # 연결되지 않은 컴포넌트가 있으면 에러
    if len(comps) >= 2:
        model_sets = [tuple(comp) for comp in comps]
        raise JoinError(
            "Multiple disjoint model sets detected. Cannot generate JOIN clause.",
            model_sets=model_sets,
        )

    comp = comps[0]

    # 단일 모델인 경우 (이미 위에서 처리했지만 방어적 처리)
    if len(comp) == 1:
        if len(models) >= 2:
            raise JoinError(
                "No joinable pairs among provided models.",
                model_sets=[(m,) for m in models],
            )
        return f"FROM {comp[0]}"

    # 조인 순서 결정
    join_sequence = _build_join_sequence_for_connected_component(comp, edges, adj)

    if not join_sequence:
        raise JoinError(
            f"Cannot find join paths for models: {comp}",
            model_sets=[tuple(comp)],
        )

    # SQL 생성: 전체 테이블 이름을 alias로 사용
    # 첫 번째 모델로 FROM 시작
    first_model = join_sequence[0][0]  # lhm of first join
    sql_parts = [f"FROM {first_model}"]

    # 각 조인 추가 (모두 LEFT JOIN 사용)
    for lhm, lhe, rhm, rhe in join_sequence:
        join_clause = f"LEFT JOIN {rhm} ON {lhm}.{lhe} = {rhm}.{rhe}"
        sql_parts.append(join_clause)

    return " ".join(sql_parts)
```

**처리 로직**:
1. Original SMQ에 joins가 있으면 스킵
2. Base models(proj 레이어) 확인
3. Base models가 1개면 스킵
4. **JOIN 그래프 구성**:
   - 각 모델 쌍에 대해 JOIN 경로 찾기 (`find_join_path`)
   - Primary key와 Foreign key 관계 확인
5. **연결된 컴포넌트 확인**:
   - 모든 모델이 연결되어 있는지 확인
   - 연결되지 않은 컴포넌트가 있으면 `JoinError` 발생
6. **JOIN 순서 결정** (BFS 사용):
   - 첫 번째 모델을 기준으로 BFS로 JOIN 순서 결정
7. **SQL 생성**:
   - 모든 JOIN은 LEFT JOIN 사용
   - JOIN 컬럼을 proj 레이어에 추가

**JoinError**:
- 여러 모델을 JOIN할 수 없는 경우 발생
- `model_sets` 속성에 분리된 모델 그룹 정보 포함

### 11. add_alias_is_uppermost_select_is_statements_without_alias

**위치**: `service/semantic/composer/pipeline/add_alias_is_uppermost_select_is_statements_without_alias.py`

**역할**: 최상위 레이어의 SELECT 절에 식이 있는데 alias가 없으면, 그 식을 str으로 바꿔서 alias로 추가

**복잡도**: ⭐⭐

**처리 흐름**:

```5:26:service/semantic/composer/pipeline/add_alias_is_uppermost_select_is_statements_without_alias.py
def add_alias_is_uppermost_select_is_statements_without_alias(parsed_smq):
    uppermost_layer = "deriv" if "deriv" in parsed_smq else "agg"
    select_statements = parsed_smq[uppermost_layer].get("metrics", [])
    for node in select_statements:
        if isinstance(node, exp.Alias) and node.alias:
            continue
        if isinstance(node, exp.Alias) and not node.alias:
            aliased_node = exp.Alias(
                this=node.this,
                alias=exp.Column(this=exp.Identifier(this=node.this.sql())),
            )
            parsed_smq[uppermost_layer]["metrics"].remove(node)
            parsed_smq[uppermost_layer]["metrics"].append(aliased_node)
        elif node.find(ARITHMETIC_EXPRESSIONS):
            aliased_node = exp.Alias(
                this=node,
                alias=exp.Column(this=exp.Identifier(this=node.sql())),
            )
            parsed_smq[uppermost_layer]["metrics"].remove(node)
            parsed_smq[uppermost_layer]["metrics"].append(aliased_node)

    return parsed_smq
```

**처리 로직**:
- 최상위 레이어의 metrics 확인
- Alias가 없는 식(산술 연산 포함)에 대해 식의 SQL 문자열을 alias로 추가

### 12. add_backtick_if_bigquery

**위치**: `service/semantic/composer/pipeline/add_backtick_if_bigquery.py`

**역할**: BigQuery dialect인 경우 모든 identifier에 backtick 추가

**복잡도**: ⭐

**처리 흐름**:

```5:18:service/semantic/composer/pipeline/add_backtick_if_bigquery.py
def add_backtick_if_bigquery(parsed_smq, dialect):
    """bigquery의 경우 모든 identifier에 backtick을 추가합니다."""
    if not dialect.lower() == "bigquery":
        return parsed_smq

    for layer in parsed_smq:
        for key in parsed_smq[layer]:
            if key == "limit":
                continue
            for node in parsed_smq[layer][key]:
                for ident in node.find_all(exp.Identifier):
                    ident.set("quoted", True)

    return parsed_smq
```

**처리 로직**:
- BigQuery dialect인 경우에만 처리
- 모든 레이어의 모든 identifier에 `quoted=True` 설정

### 13. replace_special_char_for_bigquery

**위치**: `service/semantic/composer/pipeline/replace_special_char_for_bigquery.py`

**역할**: BigQuery dialect인 경우 모든 identifier의 특수 문자를 `_`로 치환

**복잡도**: ⭐⭐

**처리 흐름**:

```5:30:service/semantic/composer/pipeline/replace_special_char_for_bigquery.py
def replace_special_char_for_bigquery(parsed_smq, dialect):
    if not dialect.lower() == "bigquery":
        return parsed_smq

    uppermost_layer = "deriv" if "deriv" in parsed_smq else "agg"

    for key in parsed_smq[uppermost_layer]:
        if key == "limit":
            continue
        for node in parsed_smq[uppermost_layer][key]:
            for ident in node.find_all(exp.Identifier):
                new_ident = _replace_special_chars(ident.this)
                ident.set("this", new_ident)

    return parsed_smq


def _replace_special_chars(text: str):
    # 대상 문자 리스트 (문자 그대로 또는 Unicode escape)
    special_chars = r"!\"\$\(\)\*,\./;\?@\[\]\\\^`\{\}~"

    # 1. 앞뒤 공백 제거 + "_" 치환
    #    예: "  !  " → "_" / "  $text" 는 건드리지 않음
    text = re.sub(rf"\s*([{special_chars}])\s*", r"_", text)

    return text
```

**처리 로직**:
- BigQuery dialect인 경우에만 처리
- 최상위 레이어의 identifier만 처리
- 특수 문자(`!`, `"`, `$`, `(`, `)`, `*`, `,`, `.`, `/`, `;`, `?`, `@`, `[`, `]`, `\`, `^`, `` ` ``, `{`, `}`, `~`)를 `_`로 치환

### 14. write_sql

**위치**: `service/semantic/composer/pipeline/write_sql.py`

**역할**: 파싱된 SMQ를 실제 SQL 문으로 변환

**복잡도**: ⭐⭐⭐⭐

**처리 흐름**:

```7:122:service/semantic/composer/pipeline/write_sql.py
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
```

**`_build_select` 함수**:

```125:199:service/semantic/composer/pipeline/write_sql.py
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
```

**처리 로직**:
1. **최상위 레이어 결정**: deriv 또는 agg
2. **CTE 생성**:
   - Base 레이어들(proj 레이어) → CTE
   - Agg 레이어(uppermost가 deriv인 경우) → CTE
3. **최종 SELECT 구성**:
   - Uppermost가 deriv: FROM agg
   - Uppermost가 agg: FROM base_layers[0] 또는 JOIN
4. **SELECT 절 구성**:
   - SELECT: metrics
   - FROM: from_name 또는 JOIN 정보
   - WHERE/QUALIFY: filters
     - **BigQuery 특별 처리**: 윈도우 함수가 있거나 agg 레이어의 필터는 QUALIFY 절 사용
   - GROUP BY: groups
   - ORDER BY: orders
   - LIMIT: limit
5. **WITH 절 추가**: CTE가 있으면 WITH 절 추가

## 복잡도 분석

### 가장 복잡한 로직: `add_default_join`

**복잡도**: ⭐⭐⭐⭐⭐

**이유**:
1. **그래프 알고리즘**: 모델 간 JOIN 관계를 그래프로 표현
2. **연결된 컴포넌트 찾기**: DFS/BFS를 사용하여 연결된 모델 그룹 찾기
3. **JOIN 경로 찾기**: Primary key와 Foreign key 관계를 기반으로 JOIN 경로 결정
4. **JOIN 순서 결정**: BFS를 사용하여 최적의 JOIN 순서 결정
5. **에러 처리**: 여러 모델을 JOIN할 수 없는 경우 `JoinError` 발생

**예시 시나리오**:
```python
# 입력: 3개의 모델 (orders, customers, products)
# 처리:
#   1. 각 모델 쌍에 대해 JOIN 경로 찾기
#   2. 그래프 구성: orders ↔ customers, customers ↔ products
#   3. 연결된 컴포넌트 확인: 모두 연결됨
#   4. JOIN 순서 결정: orders → customers → products
#   5. SQL 생성: "FROM orders LEFT JOIN customers ON ... LEFT JOIN products ON ..."
```

### 두 번째로 복잡한 로직: `write_sql`

**복잡도**: ⭐⭐⭐⭐

**이유**:
- 여러 레이어를 CTE로 변환
- JOIN 정보 처리
- BigQuery QUALIFY 절 처리
- FROM 절을 실제 물리 테이블로 변환

### 세 번째로 복잡한 로직: `check_prerequisite_of_agg_layer_and_complete`, `check_prerequisite_of_deriv_layer_and_complete`

**복잡도**: ⭐⭐⭐⭐

**이유**:
- 하위 레이어의 전제조건 확인
- Metric의 expr 내부 컬럼 처리
- Proj 레이어에 컬럼 추가

## 에러 처리

### 1. JoinError
- **원인**: 여러 semantic model을 JOIN할 수 없음
- **위치**: `add_default_join` 함수에서 발생
- **특징**: `model_sets` 속성에 분리된 모델 그룹 정보 포함
- **처리**: 현재 버전에서는 예외가 그대로 전달되어 실패 응답 반환

### 2. ValueError
- **원인**: Semantic manifest에 없는 항목, 잘못된 형식
- **예시**:
  - Metric expr에 잘못된 identifier
  - Agg layer에 있는 column을 semantic manifest에서 찾을 수 없음

### 3. AttributeError
- **원인**: Semantic model에 필요한 속성이 없음
- **예시**: `node_relation`이 없는 경우

## 사용 예시

```python
from service.semantic.composer import SQLComposer

# SQLComposer 생성
composer = SQLComposer(
    semantic_manifest=semantic_manifest,
    dialect="postgres"
)

# 파싱된 SMQ를 SQL로 변환
sql = composer.compose(
    parsed_smq=parsed_smq,
    original_smq=original_smq
)

# 결과: SQL 문자열
# "WITH orders AS (...), agg AS (...) SELECT ... FROM agg ..."
```

## 참고사항

- 파이프라인 단계는 순차적으로 실행되며, 각 단계에서 누락된 항목을 자동으로 보완
- BigQuery dialect인 경우 모든 identifier에 자동으로 backtick이 추가되고, 특수 문자가 `_`로 치환됨
- 윈도우 함수가 포함된 필터는 BigQuery에서 QUALIFY 절로 변환됨
- JOIN이 불가능한 경우 JoinError가 발생하며, 현재는 예외가 그대로 전달됨
- Proj → Agg → Deriv 순서로 하위 레이어부터 처리하여 완전한 SQL 생성

