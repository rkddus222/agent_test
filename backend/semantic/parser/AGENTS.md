# SMQ Parser 구조 분석

## 개요

`service/semantic/parser` 모듈은 Semantic Model Query (SMQ)를 파싱하여 테이블별로 그룹화된 구조로 변환하는 역할을 합니다. 이 문서는 `parser.py`와 각 파서 함수들의 구조와 동작 방식을 설명합니다.

> **참고**: 이 문서는 **SMQ → 파싱된 구조** 변환에 초점을 맞춥니다.  
> 파싱된 구조를 SQL로 변환하는 과정은 `service/semantic/composer/AGENTS.md` 문서를 참고해 주세요.

## 전체 아키텍처

```
SMQ 입력
  ↓
SMQParser.parse()
  ├─ metrics 파싱 → parse_metrics()
  ├─ filters 파싱 → parse_filters()
  ├─ groups 파싱 → parse_groups()
  ├─ orders 파싱 → parse_orders()
  ├─ limit 파싱 → parse_limit()
  └─ joins 파싱 → parse_joins()
  ↓
파싱된 SMQ (테이블별로 그룹화된 구조)
```

## 핵심 설계 원칙

**SMQParser의 설계 원칙** (```25:29:service/semantic/parser/parser.py```):
1. 각 parser는 해당 SMQ 키에 맞는 값만 추가
2. Filter에 있는 값이 select에 없어서 생기는 문제 등은 **composer에서 처리**
3. Parser는 단순히 SMQ를 파싱하여 구조화된 형태로 변환

## SMQParser 클래스

**위치**: `service/semantic/parser/parser.py`

**역할**: SMQ를 파싱하고 테이블별로 그룹화된 결과를 반환

**주요 메서드**:

### `parse(smq: SMQ)`

SMQ의 각 키별로 해당하는 파서 함수를 호출하여 파싱합니다.

**처리 흐름**:
1. 빈 `defaultdict` 구조 생성 (`parsed_smq`)
2. SMQ 키별 파서 함수 매핑:
   - `metrics` → `parse_metrics`
   - `filters` → `parse_filters`
   - `groups` → `parse_groups`
   - `orders` → `parse_orders`
   - `limit` → `parse_limit`
   - `joins` → `parse_joins`
3. 각 SMQ 키에 대해:
   - 값이 비어있으면 스킵
   - 해당 파서 함수 호출
   - 파싱 결과를 `parsed_smq`에 누적

**출력 구조**:
```python
{
    "table_name1": {
        "metrics": [...],
        "filters": [...],
        ...
    },
    "table_name2": {
        "metrics": [...],
        "filters": [...],
        ...
    },
    "agg": {
        "metrics": [...],
        "groups": [...],
        "joins": [...]
    },
    "deriv": {
        "metrics": [...],
        "filters": [...],
        "orders": [...],
        "limit": ...
    }
}
```

## 파서 함수별 상세 분석

### 1. parse_metrics (가장 복잡한 로직)

**위치**: `service/semantic/parser/metrics.py`

**역할**: SMQ의 `metrics` 배열을 파싱하여 적절한 레이어에 추가

**복잡도**: ⭐⭐⭐⭐⭐ (가장 복잡)

**주요 특징**:
- 단일 메트릭 값에 대해 매우 복잡한 분기 처리
- Derived metric 재귀적 처리
- 식(expression) vs 단순 컬럼 구분
- Metric 식 vs dimension 식 구분
- 단일 테이블 vs 다중 테이블 구분
- 여러 레이어(proj, agg, deriv)에 추가하는 로직

#### `_parse_single_value` 함수 흐름

이 함수는 메트릭 파싱의 핵심이며, 다음과 같은 복잡한 분기 처리를 수행합니다:

##### 0단계: Alias 처리 및 Derived Metric 전개

```22:51:service/semantic/parser/metrics.py
def _parse_single_value(parsed_smq, value, semantic_manifest, dialect):

    # 0) 우선 sqlglot으로 파싱하고, alias가 있으면 alias만 따로 떼어 낸다.
    parsed_value = sqlglot.parse_one(value, read=dialect)
    alias = None

    if isinstance(parsed_value, exp.Alias):
        alias = parsed_value.alias
        parsed_value = parsed_value.this

    # 0-1) derived metric인 경우
    while derived_metric_in_expr(parsed_value, semantic_manifest):
        for col in parsed_value.find_all(exp.Column):
            col_name = col.name
            metric = find_metric_by_name(col_name, semantic_manifest)
            if not metric:
                continue
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
                else:
                    col.replace(parsed_expr)
```

- **Alias 추출**: `AS alias` 형태의 alias를 별도로 저장
- **Derived Metric 전개**: 다른 metric을 참조하는 metric을 재귀적으로 전개
  - 예: `revenue_per_order` metric이 `total_revenue / order_count`를 참조하는 경우
  - 내부의 metric 참조를 실제 expr로 치환

##### 1단계: 식(Expression) 여부 판단

```53:114:service/semantic/parser/metrics.py
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
                    ident.replace(exp.Identifier(this=column_name))
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
```

**1-a) Metric의 식인 경우**:
- 식 내부에 metric이 포함되어 있는지 확인
- 식 내부의 각 identifier를 확인:
  - Metric이면 재귀적으로 처리
  - `table__column` 형식이면 해당 테이블의 proj 레이어에 추가
- 최종적으로 **deriv 레이어**에 추가

**1-b) Dimension의 식인 경우**:

```116:172:service/semantic/parser/metrics.py
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
```

- **1-b-ㄱ) 단일 테이블**: 
  - 집계 함수가 있으면 → **agg 레이어**
  - 집계 함수가 없으면 → **해당 테이블의 proj 레이어**
- **1-b-ㄴ) 다중 테이블**:
  - 각 테이블의 proj 레이어에 컬럼 추가
  - **agg 레이어**와 **deriv 레이어** 모두에 추가

##### 2단계: 단순 컬럼인 경우

```174:215:service/semantic/parser/metrics.py
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
```

- **`table__column` 형식**: 해당 테이블의 proj 레이어에 추가
  - `expr`이 있으면 Alias로 추가
  - `expr`이 없으면 단순 Column으로 추가

##### 4단계: Metric인 경우

```217:226:service/semantic/parser/metrics.py
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
```

- Metric 이름으로 semantic manifest에서 찾기
- `_parse_individual_metric_in_metrics_clause` 함수로 재귀적 처리

#### `_parse_individual_metric_in_metrics_clause` 함수

```239:314:service/semantic/parser/metrics.py
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
        ident.replace(exp.Identifier(this=column_name))

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
```

**처리 흐름**:
1. Metric의 `expr` 파싱
2. `expr` 내부의 identifier 확인:
   - Metric이면 재귀적으로 처리
   - `table__column` 형식이면 해당 테이블의 proj 레이어에 추가
3. **agg 레이어**에 metric expr 추가

### 2. parse_filters

**위치**: `service/semantic/parser/filters.py`

**역할**: SMQ의 `filters` 배열을 파싱하여 적절한 레이어에 추가

**복잡도**: ⭐⭐⭐

**주요 특징**:
- Metric이 포함된 필터는 deriv 레이어에 추가
- 여러 테이블이 포함된 필터는 deriv 레이어에 추가
- 단일 테이블 필터는 해당 테이블의 proj 레이어에 추가

#### 처리 흐름

```19:108:service/semantic/parser/filters.py
def _parse_single_value(parsed_smq, value, semantic_manifest, dialect):
    parsed_value = sqlglot.parse_one(value, read=dialect)

    # 1) metric이 있으면 deriv layer에 추가합니다.
    if is_metric_in_expr(parsed_value, semantic_manifest):
        # 1-a) metric이면 deriv layer에 추가합니다.
        # 주의) 만약 Column이 없으면 literal을 찾아서 그 literal 중 this가 str인 걸 column으로 바꿔줍니다.
        if not list(parsed_value.find_all(exp.Column)):
            for literal in parsed_value.this.find_all(exp.Literal):
                if isinstance(literal.this, str):
                    literal.replace(exp.Column(this=exp.Identifier(this=literal.this)))
        parsed_smq = append_node(
            parsed_smq,
            "deriv",
            "filters",
            parsed_value,
        )
    # 2) metric이 아닌데 앞에 model 접두사가 붙어 있지 않고, metric의 alias인 경우 그냥 deriv에 붙여 줍니다.
    elif "__" not in parsed_value.this.sql():
        parsed_smq = append_node(
            parsed_smq,
            "deriv",
            "filters",
            parsed_value,
        )

    # 3) 아니면 proj layer에 추가합니다.
    else:
        idents = _find_all_identifiers_except_subquery_child(parsed_value)

        # 3-1) 우선 parsed_value에 있는 모든 칼럼들을 expr이 있는 경우 expr로 바꿔줍니다. (이 과정에서 table_name_set을 모아서 추후 분기에 활용합니다.)
        table_names_set = set()
        for ident in idents:
            ident_name = ident.name
            try:
                table_name, column_name = ident_name.split("__")
                table_names_set.add(table_name)
            except ValueError:
                raise ValueError(
                    f"필터 식의 식별자 '{ident_name}'이 잘못되었습니다. '테이블명__컬럼명' 형식이어야 합니다."
                )
            dimension = find_dimension_by_name(
                table_name, column_name, semantic_manifest
            )
            measure = find_measure_by_name(table_name, column_name, semantic_manifest)
            if not dimension and not measure:
                # metric인지 확인
                metric = find_metric_by_name(column_name, semantic_manifest)
                if metric:
                    raise ValueError(
                        f"필터 식의 식별자 '{ident_name}'이 semantic manifest에 존재하지 않습니다. "
                        f"참고: '{column_name}'은(는) metric입니다. Metric은 model prefix 없이 단독으로 사용해야 합니다. "
                        f"예: 'table_name__metric_name' 대신 'metric_name'을 사용하세요."
                    )
                else:
                    raise ValueError(
                        f"필터 식의 식별자 '{ident_name}'이 semantic manifest에 존재하지 않습니다. "
                        f"table_name='{table_name}', column_name='{column_name}' "
                        f"(filters 파싱 중, 필터 값: '{value}' 처리 중)"
                    )
            if dimension:
                if dimension.get("expr", None):
                    this_to_replace = sqlglot.parse_one(dimension["expr"], read=dialect)
                else:
                    this_to_replace = exp.Identifier(this=dimension["name"])
            if measure:
                if measure.get("expr", None):
                    this_to_replace = sqlglot.parse_one(measure["expr"], read=dialect)
                else:
                    this_to_replace = exp.Identifier(this=measure["name"])
            ident.replace(this_to_replace)

        # 3-2) 만약 table_name이 2개 이상이면 deriv에 붙입니다.
        if len(table_names_set) > 1:
            parsed_smq = append_node(
                parsed_smq,
                "deriv",
                "filters",
                parsed_value,
            )
        # 3-3) 만약 tasbe_name이 1개이면 해당 table의 proj layer에 붙입니다.
        else:
            parsed_smq = append_node(
                parsed_smq,
                table_name,
                "filters",
                parsed_value,
            )

    return parsed_smq
```

**분기 처리**:
1. **Metric 포함**: deriv 레이어에 추가
2. **Table prefix 없음**: deriv 레이어에 추가 (metric alias일 가능성)
3. **Table prefix 있음**:
   - 여러 테이블: deriv 레이어에 추가
   - 단일 테이블: 해당 테이블의 proj 레이어에 추가

### 3. parse_orders

**위치**: `service/semantic/parser/orders.py`

**역할**: SMQ의 `orders` 배열을 파싱하여 deriv 레이어에 추가

**복잡도**: ⭐⭐⭐

**주요 특징**:
- `-` 접두사로 내림차순 정렬 처리
- Derived metric 전개
- Dimension vs metric 구분

#### 처리 흐름

```13:117:service/semantic/parser/orders.py
def parse_orders(parsed_smq, values, semantic_manifest, dialect):
    for value in values:
        parsed_value = sqlglot.parse_one(value, read=dialect)
        desc = False
        # parsed_value가 Neg인 경우, 앞에 "-"가 붙은 경우이므로 desc=True로 설정
        if isinstance(parsed_value, exp.Neg):
            parsed_value = parsed_value.this
            desc = True

        # 1) 해당 value가 dimension인 경우
        if "__" in parsed_value.name:
            _, column_name = parsed_value.name.split("__")
            value_to_append = exp.Ordered(
                this=exp.Column(this=exp.Identifier(this=column_name)),
                desc=desc,
            )
            parsed_smq = append_node(
                parsed_smq,
                "deriv",
                "orders",
                value_to_append,
            )

        # 2) 해당 value가 metric인 경우
        else:
            derived_metric = False
            # 2-1) derive metric인 경우 먼저 unpack해 줍니다
            while derived_metric_in_expr(parsed_value, semantic_manifest):
                derived_metric = True
                for col in parsed_value.find_all(exp.Column):
                    col_name = col.name
                    metric = find_metric_by_name(col_name, semantic_manifest)
                    if not metric:
                        continue
                    expr = metric.get("expr", None)
                    if not expr:
                        raise AttributeError(
                            f"Metric {col_name}에 expr이 없습니다. 시멘틱 모델을 확인해 주세요."
                        )
                    parsed_expr = sqlglot.parse_one(expr)
                    # 주의) 안에 dimension이 있으면 table_name을 떼 주고, 해당 table의 proj layer에 column을 추가해 줘야 합니다.
                    for ident in parsed_expr.find_all(exp.Identifier):
                        ident_metric = find_metric_by_name(
                            ident.name, semantic_manifest
                        )
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
                                    f"(orders 파싱 중, derived metric의 identifier '{ident.name}' 처리 중)"
                                )
                            expr = column.get("expr", None)
                            expr = sqlglot.parse_one(expr) if expr else None
                            if expr and expr != column_name:
                                expr_with_alias = exp.Alias(
                                    this=expr, alias=exp.Identifier(this=column_name)
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
                                    exp.Column(
                                        this=exp.Identifier(this=column_name)
                                    ),
                                )
                            ident.replace(exp.Identifier(this=column_name))
                    if is_metric_in_expr(parsed_expr, semantic_manifest):
                        if col is parsed_value:
                            parsed_value = parsed_expr
                        else:
                            col.replace(parsed_expr)
            if derived_metric:
                value_to_append = exp.Ordered(
                    this=parsed_value,
                    desc=desc,
                )
            # 2-2) 일반 metric의 경우
            else:
                column_name = parsed_value.this.name
                value_to_append = exp.Ordered(
                    this=exp.Column(this=exp.Identifier(this=column_name)),
                    desc=desc,
                )
            parsed_smq = append_node(
                parsed_smq,
                "deriv",
                "orders",
                value_to_append,
            )
    return parsed_smq
```

**분기 처리**:
1. **Dimension** (`table__column`): deriv 레이어에 추가
2. **Metric**:
   - Derived metric이면 전개 후 deriv 레이어에 추가
   - 일반 metric이면 deriv 레이어에 추가

### 4. parse_groups

**위치**: `service/semantic/parser/groups.py`

**역할**: SMQ의 `groups` 배열을 파싱하여 agg 레이어에 추가

**복잡도**: ⭐

**주요 특징**: 가장 간단한 파서

#### 처리 흐름

```6:27:service/semantic/parser/groups.py
def parse_groups(parsed_smq, values, semantic_manifest, dialect):
    for value in values:
        parsed_value = sqlglot.parse_one(value, read=dialect)
        # 1) 해당 value가 dimension인 경우
        if "__" in parsed_value.name:
            table_name, column_name = parsed_value.name.split("__")
            value_to_append = exp.Column(this=exp.Identifier(this=column_name))
            parsed_smq = append_node(
                parsed_smq,
                "agg",
                "groups",
                value_to_append,
            )
        # 2) 해당 value가 metric인 경우
        else:
            parsed_smq = append_node(
                parsed_smq,
                "agg",
                "groups",
                parsed_value,
            )
    return parsed_smq
```

- Dimension이든 metric이든 모두 **agg 레이어**에 추가

### 5. parse_limit

**위치**: `service/semantic/parser/limit.py`

**역할**: SMQ의 `limit` 값을 파싱하여 deriv 레이어에 추가

**복잡도**: ⭐

**주요 특징**: 가장 간단한 파서

#### 처리 흐름

```5:16:service/semantic/parser/limit.py
def parse_limit(parsed_smq, value, semantic_manifest, dialect):
    if not value:
        return parsed_smq
    if not isinstance(value, int):
        raise ValueError("limit 값은 정수여야 합니다.")
    parsed_smq = append_node(
        parsed_smq,
        "deriv",
        "limit",
        value,
    )
    return parsed_smq
```

- 정수 검증 후 **deriv 레이어**에 추가

### 6. parse_joins

**위치**: `service/semantic/parser/joins.py`

**역할**: SMQ의 `joins` 배열을 파싱하여 agg 레이어에 추가

**복잡도**: ⭐⭐

**주요 특징**:
- JOIN 절의 컬럼들을 각 테이블의 proj 레이어에 추가
- JOIN 정보를 agg 레이어에 추가

#### 처리 흐름

```6:53:service/semantic/parser/joins.py
def parse_joins(parsed_smq, value, semantic_manifest, dialect):
    if not value:
        return parsed_smq
    if len(value) > 1:
        raise ValueError(
            "joins 배열에는 원소가 하나만 가능합니다. 여러 테이블 간 join은 배열의 하나의 원소에 모두 포함하여 작성해주세요. ex) joins = [\"FROM A LEFT JOIN B ON A.id = B.id INNER JOIN C ON A.id = C.id\"]"
        )
    # 주의) join value가 n개인 경우 지원에 대한 추가 개발 필요
    value = value[0]
    parsed_value = sqlglot.parse_one(value, read=dialect)
    for col in parsed_value.find_all(exp.Column):
        col_name = col.name
        if "__" in col_name:
            table_name, column_name = col_name.split("__", 1)
            col.set("this", exp.Identifier(this=column_name))
            col.set("table", exp.Identifier(this=table_name))
        else:
            column_name = col_name
            table_name = col.table
        # measure를 먼저 찾고, 없으면 dimension에서 찾습니다.
        column = find_measure_by_name(
            table_name,
            column_name,
            semantic_manifest,
        )
        if not column:
            column = find_dimension_by_name(
            table_name,
            column_name,
            semantic_manifest,
        )
        node_to_append = exp.Column(this=exp.Identifier(this=column_name))
        if column:
            expr = column.get("expr", None)
            if expr:
                parsed_expr = sqlglot.parse_one(expr, read=dialect)
                if parsed_expr.sql() != column["name"]:
                    node_to_append = exp.Alias(
                        this=parsed_expr, alias=exp.Identifier(this=column_name)
                    )
        parsed_smq = append_node(parsed_smq, table_name, "metrics", node_to_append)
    parsed_smq = append_node(
        parsed_smq,
        "agg",
        "joins",
        parsed_value,
    )
    return parsed_smq
```

**처리 흐름**:
1. JOIN 절의 모든 컬럼을 찾아서:
   - 각 테이블의 proj 레이어에 컬럼 추가
   - `expr`이 있으면 Alias로 추가
2. JOIN 정보를 **agg 레이어**에 추가

## 레이어별 추가 규칙 요약

### Proj 레이어 (테이블명 키)
- Metrics: 해당 테이블의 dimension/measure
- Filters: 단일 테이블 필터

### Agg 레이어 (`"agg"` 키)
- Metrics: Simple metric, 집계 함수가 포함된 dimension 식
- Groups: GROUP BY 컬럼
- Joins: JOIN 정보

### Deriv 레이어 (`"deriv"` 키)
- Metrics: Non-simple metric, 다중 테이블 dimension 식
- Filters: Metric 포함 필터, 다중 테이블 필터
- Orders: 정렬 컬럼
- Limit: 제한 수

## 주요 유틸리티 함수

### `append_node(smq, table, key, node)`
- `parsed_smq`의 특정 테이블/레이어에 노드 추가
- 리스트가 없으면 생성 후 추가

### `is_metric_in_expr(expr, semantic_manifest)`
- 식 내부에 metric이 포함되어 있는지 확인

### `derived_metric_in_expr(expr, semantic_manifest)`
- 식 내부에 derived metric(다른 metric을 참조하는 metric)이 포함되어 있는지 확인

### `find_metric_by_name(name, semantic_manifest)`
- Semantic manifest에서 metric 찾기

### `find_dimension_by_name(table_name, column_name, semantic_manifest)`
- Semantic manifest에서 dimension 찾기

### `find_measure_by_name(table_name, column_name, semantic_manifest)`
- Semantic manifest에서 measure 찾기

## 에러 처리

### 1. ValueError
- **원인**: 잘못된 형식의 입력, semantic manifest에 없는 항목
- **예시**:
  - `table__column` 형식이 아닌 경우
  - Semantic manifest에 없는 metric/dimension/measure
  - Limit 값이 정수가 아닌 경우

### 2. AttributeError
- **원인**: Metric에 `expr`이 없는 경우
- **예시**: Derived metric을 전개할 때 `expr`이 없으면 발생

## 복잡도 분석

### 가장 복잡한 로직: `parse_metrics`

**복잡도**: ⭐⭐⭐⭐⭐

**이유**:
1. **다양한 입력 형식 지원**:
   - 단순 컬럼: `table__column`
   - Metric 이름: `metric_name`
   - 복잡한 식: `table1__col1 + table2__col2`
   - Metric 식: `metric1 / metric2`

2. **재귀적 처리**:
   - Derived metric 전개
   - Metric 내부의 metric 처리

3. **레이어 결정 로직**:
   - 단일 테이블 vs 다중 테이블
   - 집계 함수 포함 여부
   - Metric 포함 여부
   - 식 vs 단순 컬럼

4. **복잡한 분기 처리**:
   - 5단계 이상의 중첩된 분기
   - 각 분기마다 다른 레이어에 추가

**예시 시나리오**:
```python
# 입력: "revenue_per_order"
# 1. Metric 이름으로 찾기
# 2. Metric의 expr 확인: "total_revenue / order_count"
# 3. total_revenue metric 찾기 → 재귀 처리
# 4. order_count metric 찾기 → 재귀 처리
# 5. 각 metric의 expr에서 measure 찾기
# 6. 각 테이블의 proj 레이어에 measure 추가
# 7. Agg 레이어에 각 metric 추가
# 8. Deriv 레이어에 최종 식 추가
```

### 두 번째로 복잡한 로직: `parse_filters`

**복잡도**: ⭐⭐⭐

**이유**:
- Metric 포함 여부 확인
- 여러 테이블 포함 여부 확인
- Dimension/measure의 `expr` 처리

### 세 번째로 복잡한 로직: `parse_orders`

**복잡도**: ⭐⭐⭐

**이유**:
- Derived metric 전개
- Dimension vs metric 구분
- 내림차순 정렬 처리

## 사용 예시

```python
from service.semantic.parser import SMQParser

# SMQParser 생성
parser = SMQParser(
    semantic_manifest=semantic_manifest,
    dialect="postgres"
)

# SMQ 파싱
parsed_smq = parser.parse({
    "metrics": ["total_revenue", "order_count"],
    "groupBy": ["customer_id"],
    "filters": ["order_date >= '2024-01-01'"],
    "orderBy": ["-total_revenue"],
    "limit": 100
})

# 결과 구조
# {
#     "orders": {
#         "metrics": [...],
#         "filters": [...]
#     },
#     "agg": {
#         "metrics": [...],
#         "groups": [...]
#     },
#     "deriv": {
#         "metrics": [...],
#         "filters": [...],
#         "orders": [...],
#         "limit": 100
#     }
# }
```

## 참고사항

- Parser는 **구조화만** 담당하며, 완전성 검증은 composer에서 수행
- 각 파서는 독립적으로 동작하며, 순서는 중요하지 않음
- Derived metric은 재귀적으로 전개되어 최종적으로 base measure/dimension으로 변환됨
- `table__column` 형식은 semantic model의 테이블명과 컬럼명을 구분하는 표준 형식

