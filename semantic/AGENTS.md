# SMQ to SQL 변환 구조 분석

## 개요

`smq_to_sql` 함수는 Semantic Model Query (SMQ)를 SQL 쿼리로 변환하는 핵심 기능을 제공합니다. 이 문서는 `service/semantic/semantic_service.py`의 `smq_to_sql` 함수 구조와 동작 방식을 설명합니다.

> **시멘틱 모델 파싱/린팅 관련 문서 위치**
>
> 이 문서는 **SMQ → SQL 변환 런타임 경로**(parser/composer 파이프라인)에 초점을 맞춥니다.  
> 시멘틱 모델 YAML(sources.yml, semantic_models/*.yml, metrics) 파싱과 **정적 린트 규칙(중복 이름, 타입 검증, DDL 컬럼 검증 등)** 에 대한 구조와 동작 방식은  
> `service/semantic/model_manager/AGENTS.md` 문서를 참고해 주세요.

## 전체 아키텍처

```
SMQ 입력
  ↓
prepare_smq_to_sql (준비 단계)
  ├─ Semantic Models 로드
  └─ Metrics 로드
  ↓
smq_to_sql (변환 단계)
  ├─ 입력 검증
  ├─ SMQParser 생성 및 실행
  │   ├─ metrics 파싱
  │   ├─ filters 파싱
  │   ├─ groups 파싱
  │   ├─ orders 파싱
  │   ├─ limit 파싱
  │   └─ join 파싱
  ├─ SQLComposer 생성 및 실행 (14단계 파이프라인)
  │   ├─ [임시] Dimension 식을 deriv 레이어로 이동
  │   ├─ [Deriv] Agg 함수를 agg 레이어로 push down
  │   ├─ [임시] GroupBy를 Metrics에 추가
  │   ├─ [전체] SMQ 완전성 검증 및 보완
  │   ├─ [전체] Anonymous 노드를 legit 노드로 변환
  │   ├─ [전체] Subquery의 FROM 절을 실제 테이블로 변환
  │   ├─ [Deriv] Deriv 레이어 전제조건 확인 및 완성
  │   ├─ [Agg] Group/Select 일치 확인 및 완성
  │   ├─ [Agg] Agg 레이어 전제조건 확인 및 완성
  │   ├─ [Agg] Default JOIN 추가 (가장 복잡한 로직)
  │   ├─ [전체] Alias 추가 (식에 alias가 없는 경우)
  │   ├─ [전체] BigQuery backtick 추가 (BigQuery인 경우)
  │   ├─ [전체] BigQuery special char 치환 (BigQuery인 경우)
  │   └─ [전체] SQL 작성 (윈도우 함수 처리 포함)
  ├─ CTE를 인라인 뷰로 변환 (선택)
  └─ 메타데이터 수집
  ↓
SQL 출력 + 메타데이터
```

## 주요 함수

### 1. `prepare_smq_to_sql`

**위치**: `service/semantic/semantic_service.py:56-182`

**역할**: SMQ를 SQL로 변환하기 위한 준비 작업을 수행합니다.

**주요 처리 흐름**:
1. Manifest 파싱 및 검증
2. Metrics 로드
3. `smq_to_sql` 호출
4. 결과 형식 변환 및 반환

### 2. `smq_to_sql`

**위치**: `service/semantic/semantic_service.py:185-281`

**역할**: SMQ를 SQL로 변환하는 핵심 함수입니다.

**주요 처리 흐름**:

#### 2.1 SMQ 설정 및 정규화

#### 2.2 SMQParser 생성 및 실행
- `SMQParser`는 SMQ를 파싱하여 `defaultdict` 구조로 변환
- 각 SMQ 키(metrics, filters, groups, orders, limit, joins)별로 파서 함수 실행
- 결과는 테이블명을 키로 하는 딕셔너리 구조
- **상세 내용**: 각 파서 함수의 처리 로직은 `service/semantic/parser/AGENTS.md`의 **"파서 함수별 상세 분석"** 섹션 참고

#### 2.3 SQLComposer 생성 및 실행
- `SQLComposer`는 파싱된 SMQ를 SQL로 변환
- `compose()` 메서드가 파이프라인 방식으로 14단계를 거쳐 SQL 생성
- **상세 내용**: 각 파이프라인 단계의 처리 로직은 `service/semantic/composer/AGENTS.md`의 **"파이프라인 단계별 상세 분석"** 섹션 참고

#### 2.4 CTE를 인라인 뷰로 변환 (199-203줄)
- `cte=False`인 경우 CTE를 인라인 서브쿼리로 변환

#### 2.5 메타데이터 수집 (205-244줄)
- 생성된 SQL의 SELECT 절에서 메타데이터 추출

**반환 형식**:
```python
{
    "success": True,
    "sql": "WITH ... SELECT ...",
    "metadata": [...]
}
```

## 핵심 컴포넌트

### SMQParser
**위치**: `service/semantic/parser/parser.py`

**역할**: SMQ를 파싱하여 테이블별로 그룹화된 결과를 반환

> **상세 내용**: SMQParser의 구조와 각 파서 함수의 동작 방식에 대한 상세한 설명은  
> `service/semantic/parser/AGENTS.md` 문서의 **"SMQParser 클래스"** 및 **"파서 함수별 상세 분석"** 섹션을 참고해 주세요.

**출력 구조**:
```python
{
    "table_name": {
        "metrics": [...],
        "filters": [...],
        "groups": [...],
        "orders": [...],
        "limit": ...
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

### SQLComposer
**위치**: `service/semantic/composer/composer.py`

**역할**: 파싱된 SMQ를 SQL 문으로 변환

> **상세 내용**: SQLComposer의 구조와 각 파이프라인 단계의 동작 방식에 대한 상세한 설명은  
> `service/semantic/composer/AGENTS.md` 문서의 **"SQLComposer 클래스"** 및 **"파이프라인 단계별 상세 분석"** 섹션을 참고해 주세요.

**파이프라인 단계** (총 14단계):

1. **Dimension 식 이동** (임시 처리): `move_dimension_expr_to_deriv_layer_if_deriv_exists`
   - Deriv 레이어가 있는 경우, agg 레이어의 dimension 식을 deriv 레이어로 이동

2. **Agg 함수 push down** (Deriv 레이어): `push_down_agg_from_deriv_layer`
   - Deriv 레이어에 있는 집계 함수를 agg 레이어로 push down

3. **GroupBy를 Metrics에 추가** (임시 처리): `move_groups_to_metrics`
   - GroupBy에 있는 항목을 최상위 레이어의 Metrics에도 추가

4. **SMQ 완전성 검증**: `check_if_original_smq_included_and_complete_if_not`
   - SMQ 상의 모든 항목이 파싱된 결과에 포함되어 있는지 확인
   - 누락된 항목이 있으면 자동으로 추가

5. **Anonymous 노드 변환**: `transform_anonymous_node_into_legit_one`
   - Anonymous 노드(예: `AVG()`, `SUM()`)를 legit한 노드(예: `exp.Avg`, `exp.Sum`)로 변환

6. **Subquery FROM 절 처리**: `replace_from_with_real_table_in_subqueries`
   - 서브쿼리 안의 FROM 절이 deriv/agg가 아니면 실제 물리 테이블로 변환

7. **Deriv 레이어 전제조건 확인**: `check_prerequisite_of_deriv_layer_and_complete`
   - Deriv 레이어에 필요한 하위 항목들이 모두 포함되어 있는지 확인
   - 누락된 항목을 agg 레이어에서 찾아 추가

8. **Group/Select 일치 확인**: `check_group_select_parity_and_complete`
   - Agg 레이어에서 GROUP BY와 SELECT 절이 일치하는지 확인
   - 집계 함수가 아닌 SELECT 항목은 모두 GROUP BY에 포함되어야 함

9. **Agg 레이어 전제조건 확인**: `check_prerequisite_of_agg_layer_and_complete`
   - Agg 레이어에 필요한 하위 항목들이 모두 포함되어 있는지 확인
   - 누락된 항목을 proj 레이어에서 찾아 추가

10. **Default JOIN 추가**: `add_default_join` - **가장 복잡한 로직** (복잡도: ⭐⭐⭐⭐⭐)
    - Proj 레이어가 2개 이상인데 agg 레이어에 JOIN이 없으면 자동으로 JOIN 추가
    - JOIN 컬럼이 proj 레이어에 없으면 자동으로 추가
    - **JoinError 발생 가능**: 여러 모델을 JOIN할 수 없는 경우
    - 그래프 알고리즘을 사용하여 모델 간 JOIN 관계를 표현하고, BFS로 JOIN 순서 결정

11. **Alias 추가**: `add_alias_is_uppermost_select_is_statements_without_alias`
    - 최상위 레이어의 SELECT 절에 식이 있는데 alias가 없으면, 그 식을 str으로 바꿔서 alias로 추가

12. **BigQuery backtick 추가**: `add_backtick_if_bigquery`
    - BigQuery dialect인 경우 모든 identifier에 backtick 추가

13. **BigQuery special char 치환**: `replace_special_char_for_bigquery`
    - BigQuery dialect인 경우 모든 identifier의 특수 문자를 `_`로 치환

14. **SQL 작성**: `write_sql` (복잡도: ⭐⭐⭐⭐)
    - 파싱된 SMQ를 실제 SQL 문으로 변환
    - CTE와 최종 SELECT 절 생성
    - **윈도우 함수 처리**: BigQuery dialect에서 agg 레이어에 윈도우 함수가 있을 경우 WHERE 절 대신 QUALIFY 절 사용

## Proj / Agg / Deriv 레이어 구조

SQL 쿼리는 **3단계 레이어 구조**로 생성됩니다. 각 레이어는 `parsed_smq` 딕셔너리의 키로 구분됩니다.

### 레이어 개요

```
원본 테이블들
  ↓
[Proj 레이어] - 각 모델별 CTE 생성
  ├─ model1 CTE: 원본 테이블에서 필요한 컬럼 projection
  ├─ model2 CTE: 원본 테이블에서 필요한 컬럼 projection
  └─ WHERE 절 적용 (모델별 필터링)
  ↓
[Agg 레이어] - JOIN 및 집계 CTE 생성
  ├─ Proj CTE들을 JOIN
  ├─ GROUP BY 적용
  ├─ 집계 함수 적용 (SUM, COUNT, AVG 등)
  └─ "agg" CTE로 생성
  ↓
[Deriv 레이어] - 최종 SELECT
  ├─ Agg CTE의 컬럼들을 참조
  ├─ 파생 메트릭 계산식 적용
  └─ 최종 결과 반환
```

### 1. Proj 레이어 (Projection Layer)

**특징**:
- 각 semantic model별로 하나의 키 생성 (테이블명이 키)
- 각 모델별로 하나의 CTE 생성
- CTE 이름은 모델 이름과 동일

**포함 항목**:
- Dimensions의 `expr` (컬럼 또는 계산식)
- Measures의 `expr` (컬럼 또는 계산식)
- GROUP BY에 사용되는 dimensions
- WHERE 절에 사용되는 dimensions/measures
- JOIN에 사용되는 컬럼들

**WHERE 절 적용**:
- 각 모델별 CTE에 해당 모델의 WHERE 조건 적용
- 예: `model1` CTE에는 `model1`에 속한 필터만 적용

**예시**:
```sql
WITH model1 AS (
  SELECT 
    customer_id,
    order_date,
    revenue
  FROM orders
  WHERE order_date >= '2024-01-01'  -- model1의 WHERE 조건
),
model2 AS (
  SELECT 
    customer_id,
    customer_name
  FROM customers
  WHERE status = 'active'  -- model2의 WHERE 조건
)
```

### 2. Agg 레이어 (Aggregation Layer)

**특징**:
- `"agg"` 키로 관리
- Proj CTE들을 JOIN하여 하나의 결과셋 생성
- GROUP BY 적용
- 집계 함수(SUM, COUNT, AVG, MIN, MAX 등) 적용
- "agg"라는 이름의 단일 CTE로 생성

**포함 항목**:
- **Dimensions**: GROUP BY에 사용되는 dimensions
  - Proj 레이어에서 projection된 컬럼 참조
- **Metrics**: 집계 함수가 적용된 메트릭
  - Simple metric: 직접 집계 함수 적용
  - Non-simple metric: deriv 레이어로 이동
- **Joins**: Proj CTE들 간의 JOIN 정보

**JOIN 처리**:
- `joins` 필드를 기반으로 Proj CTE들을 JOIN
- JOIN 타입: INNER, LEFT, RIGHT, FULL, CROSS
- Default JOIN: 여러 proj 레이어가 있는데 JOIN이 없으면 자동으로 추가

**예시**:
```sql
agg AS (
  SELECT 
    model1.customer_id,
    model2.customer_name,
    SUM(model1.revenue) AS total_revenue,
    COUNT(DISTINCT model1.order_id) AS order_count
  FROM model1
  LEFT JOIN model2 
    ON model1.customer_id = model2.customer_id
  GROUP BY 
    model1.customer_id,
    model2.customer_name
)
```

### 3. Deriv 레이어 (Derived Layer)

**목적**: Agg CTE의 결과를 기반으로 파생 메트릭을 계산

**특징**:
- `"deriv"` 키로 관리
- 최종 SELECT 절에 추가
- Agg CTE의 컬럼들을 참조하는 계산식
- 다른 메트릭을 참조하는 복잡한 메트릭 계산

**포함 항목**:
- **Non-simple metrics**: 다른 메트릭이나 계산식을 포함하는 메트릭
  - 예: `revenue_per_order = total_revenue / order_count`
  - `expr`에 계산식 포함 (SQL Expression 객체)
- **Metric WHERE 절**: 메트릭 레벨의 필터링
  - Agg CTE의 컬럼을 참조하는 WHERE 조건

**의존성 처리**:
- 파이프라인에서 의존하는 메트릭이 먼저 계산되도록 보장
- Deriv 레이어의 전제조건 확인 단계에서 처리

**예시**:
```sql
SELECT 
  customer_id,
  customer_name,
  total_revenue,           -- agg 레이어에서 온 컬럼
  order_count,              -- agg 레이어에서 온 컬럼
  total_revenue / order_count AS revenue_per_order  -- deriv 레이어 계산식
FROM agg
WHERE total_revenue > 1000  -- metric WHERE 절
ORDER BY total_revenue DESC
LIMIT 100
```

### 레이어별 Level 결정 규칙

#### Proj 레이어 (테이블명 키)
- 각 semantic model의 이름이 키
- Dimensions의 `expr`
- Measures의 `expr`
- GROUP BY에 사용되는 dimensions
- WHERE 절에 사용되는 dimensions/measures
- JOIN에 필요한 컬럼들

#### Agg 레이어 (`"agg"` 키)
- GROUP BY에 사용되는 dimensions
- Simple metrics (직접 집계 함수 적용 가능한 메트릭)
- Metric이 `metric_type == "simple"`인 경우

#### Deriv 레이어 (`"deriv"` 키)
- Non-simple metrics (`metric_type != "simple"`)
- 다른 메트릭을 참조하는 계산식이 있는 메트릭
- `expr`에 복잡한 계산식이 포함된 메트릭

### 전체 쿼리 구조 예시

```sql
-- Proj 레이어: 각 모델별 CTE
WITH orders_proj AS (
  SELECT 
    customer_id,
    order_date,
    revenue
  FROM orders
  WHERE order_date >= '2024-01-01'
),
customers_proj AS (
  SELECT 
    customer_id,
    customer_name
  FROM customers
  WHERE status = 'active'
),

-- Agg 레이어: JOIN 및 집계
agg AS (
  SELECT 
    orders_proj.customer_id,
    customers_proj.customer_name,
    SUM(orders_proj.revenue) AS total_revenue,
    COUNT(DISTINCT orders_proj.order_id) AS order_count
  FROM orders_proj
  LEFT JOIN customers_proj 
    ON orders_proj.customer_id = customers_proj.customer_id
  GROUP BY 
    orders_proj.customer_id,
    customers_proj.customer_name
)

-- Deriv 레이어: 최종 SELECT
SELECT 
  customer_id,
  customer_name,
  total_revenue,                    -- agg 레이어
  order_count,                       -- agg 레이어
  total_revenue / order_count AS revenue_per_order  -- deriv 레이어 계산
FROM agg
WHERE total_revenue > 1000
ORDER BY total_revenue DESC
LIMIT 100
```

### 레이어별 WHERE 절 처리

1. **Proj 레이어 WHERE**: 각 모델별 CTE에 해당 모델의 필터만 적용
   - `parsed_smq["model1"]["filters"]`의 조건만 `model1` CTE에 적용

2. **Agg 레이어 WHERE**: JOIN 후 집계 전 필터링 (현재 미구현)

3. **Deriv 레이어 WHERE**: 최종 SELECT에서 메트릭 레벨 필터링
   - `parsed_smq["deriv"]["filters"]`의 조건만 적용
   - Agg CTE의 컬럼을 참조

## 에러 처리

### 1. RecursionError
- **원인**: Metrics 간 순환 의존성
- **처리**: 에러 메시지와 함께 실패 응답 반환

### 2. JoinError
- **원인**: 여러 semantic model이 join될 수 없음
- **위치**: `add_default_join` 함수에서 발생
- **특징**: `model_sets` 속성에 분리된 모델 그룹 정보 포함
- **처리**: 현재 버전에서는 예외가 그대로 전달되어 실패 응답 반환
  - 향후 재귀적 쿼리 분리 기능 추가 가능

### 3. ValueError
- **원인**: 입력 검증 실패, Metric 없음 등
- **처리**: 명확한 에러 메시지와 함께 실패 응답 반환

### 4. 일반 Exception
- **처리**: 상세한 로그와 함께 에러 메시지 반환


## 윈도우 함수 지원

`smq_to_sql`은 윈도우 함수(Window Function)를 지원합니다. 윈도우 함수가 포함된 메트릭이나 SQL 식을 SMQ에 포함할 수 있으며, 시스템이 자동으로 처리합니다.

### 윈도우 함수 처리 방식

#### 1. 파싱 단계
- **SMQParser**: `sqlglot.parse_one()`을 사용하여 메트릭을 파싱
- 윈도우 함수가 포함된 SQL 식도 자동으로 파싱되어 `exp.Window` 노드로 변환
- 예: `SUM(revenue) OVER (PARTITION BY customer_id ORDER BY order_date)` 형태의 메트릭 지원

#### 2. SQL 작성 단계
- **BigQuery Dialect 특별 처리**: `write_sql.py`의 `_build_select` 함수에서 BigQuery dialect이고 agg 레이어에 윈도우 함수가 있을 경우, WHERE 절 대신 **QUALIFY 절**을 사용합니다.
  - 위치: `service/semantic/composer/pipeline/write_sql.py:163-168`
  - 이유: BigQuery에서 윈도우 함수 결과에 대한 필터링은 QUALIFY 절을 사용해야 합니다.

**예시**:
```sql
-- BigQuery에서 윈도우 함수와 필터가 함께 사용되는 경우
SELECT 
  customer_id,
  order_date,
  SUM(revenue) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_total
FROM agg
QUALIFY running_total > 1000  -- WHERE 대신 QUALIFY 사용
```

#### 3. 메트릭 타입 지원
- **MetricTypeParams**: `window` 필드를 통해 윈도우 함수 정보를 메트릭 정의에 포함할 수 있습니다.
- **MetricRef**: `offset_window` 필드를 통해 윈도우 오프셋 정보를 포함할 수 있습니다.
- 위치: `service/semantic/types/metric_type.py`

### 윈도우 함수 사용 예시

```python
# SMQ에 윈도우 함수가 포함된 메트릭 사용
smq = {
    "metrics": [
        "SUM(revenue) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total"
    ],
    "groupBy": ["customer_id", "order_date"],
    "filters": ["order_date >= '2024-01-01'"]
}

# BigQuery dialect로 변환 시
# QUALIFY 절이 자동으로 사용됨 (필터가 agg 레이어에 있는 경우)
```