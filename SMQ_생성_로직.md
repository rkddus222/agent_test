# SMQ 생성 로직 정리

## 개요

SMQ(Semantic Model Query)는 시멘틱 모델 기반의 구조화된 쿼리 언어로, 사용자의 자연어 질의나 요구사항을 기반으로 생성되어 최종적으로 SQL로 변환됩니다.

## SMQ 구조

SMQ는 다음 JSON 형식의 구조를 가집니다:

```json
{
  "metrics": ["메트릭명1", "메트릭명2"],
  "groupBy": ["모델명__차원명1", "모델명__차원명2"],
  "filters": ["필터조건1", "필터조건2"],
  "orderBy": ["정렬기준1", "-정렬기준2"],  // '-'는 내림차순
  "limit": 100,  // 또는 null
  "joins": ["조인경로1"]
}
```

### 주요 필드 설명

- **metrics**: 최종 SELECT 대상. YML 파일의 최상위 `metrics` 섹션에서 정의된 메트릭 이름만 사용. 윈도우 함수가 포함된 SQL 식도 직접 사용 가능 (예: `SUM(revenue) OVER (PARTITION BY customer_id ORDER BY order_date)`)
- **groupBy**: 집계 기준. `모델명__차원명` 형식으로 작성
- **filters**: WHERE 절 조건 (SQL의 WHERE 절에 해당)
- **orderBy**: 정렬 기준. 오름차순은 그대로, 내림차순은 앞에 `-` 추가
- **limit**: 조회 개수 제한. 명시되지 않았으면 `null`
- **joins**: 조인 경로. SQL JOIN 문법을 문자열로 작성 (배열에는 원소 하나만 가능). 비어있으면 자동으로 조인 경로 생성

## SMQ 생성 방식

프로젝트에서는 총 3가지 방식으로 SMQ를 생성합니다.

### 1. SemanticAgent 기반 생성

**위치**: `backend/semantic_agent.py`

**특징**:
- YML 파일 내용과 사용자 질의를 직접 입력받아 SMQ 생성
- LLM을 사용하여 프롬프트 기반으로 SMQ 생성
- 테스트 케이스 생성 시 주로 사용

**프로세스**:
```
1. YML 파일 내용 읽기
2. LLM에 프롬프트 전달 (YML 내용 + 사용자 질의)
3. LLM이 JSON 형식의 SMQ 생성
4. 생성된 SMQ를 SQL로 변환
```

**주요 메서드**: `_generate_smq_and_convert()`

**프롬프트 핵심 규칙**:
- 메트릭은 반드시 최상위 `metrics` 섹션의 `name` 필드 값만 사용
- 집계 함수(SUM, COUNT 등)를 메트릭 이름에 포함하지 않음
- 차원은 `semantic_model_name__dimension_name` 형식으로 작성
- semantic model 이름, entity 이름, measure 이름을 메트릭으로 사용하지 않음

### 2. SMQAgent 기반 생성 (Tool Calling)

**위치**: `backend/smq_agent.py`

**특징**:
- LLM이 Tool을 호출하는 방식으로 SMQ 생성
- SemanticModelSelector, SemanticModelQuery Tool 사용
- 대화형 인터페이스에서 주로 사용

**프로세스**:
```
1. SemanticModelSelector.selectSemanticModelFiles 호출
   - 사용자 질문 기반으로 관련 YML 파일 자동 선택 및 내용 읽기
   
2. LLM이 YML 내용 분석하여 필요한 요소 추출
   - metrics, dimensions, measures 파악
   
3. SemanticModelQuery.convertSmqToSql 호출
   - 생성한 SMQ를 SQL로 변환
   
4. (에러 발생 시) SemanticModelQuery.editSmq 호출
   - SMQ 수정 후 재변환
```

**주요 Tool**:

#### Tool 1: SemanticModelSelector.selectSemanticModelFiles
- **목적**: 사용자 질문과 관련 있는 시멘틱 모델 파일 선택 및 내용 읽기
- **입력**: `userQuery` (사용자 질문)
- **출력**: 선택된 파일 목록과 YML 파일 내용
- **선택 로직**: 키워드 매칭 기반으로 관련 파일 자동 선택

#### Tool 2: SemanticModelQuery.convertSmqToSql
- **목적**: SMQ를 SQL로 변환
- **입력**: `smq` (SMQ 배열)
- **출력**: `smqState` (SMQ와 변환된 SQL 결과)

#### Tool 3: SemanticModelQuery.editSmq
- **목적**: 기존 SMQ 수정
- **입력**: `smqEdits` (수정 사항)
- **수정 방식**: `add`, `remove`, `set` 작업 지원

**프롬프트 파일**: `backend/prompts/smq_prompt.txt`

### 3. LangGraphAgent 기반 생성

**위치**: `backend/langgraph_agent.py`

**특징**:
- LangGraph를 사용한 멀티 스텝 에이전트
- 질의 분석 → 정보 추출 → SMQ 생성 → SQL 변환 파이프라인
- 구조화된 워크플로우

**프로세스**:
```
1. 질의 분리 (Split Questions)
   - 사용자 질의를 여러 개의 하위 질의로 분리

2. 모델 선택 (Model Selection)
   - 질의에 맞는 시멘틱 모델 선택

3. 정보 추출 (Information Extraction)
   - metrics, dimensions, filters, orderBy, limit 추출

4. SMQ 생성 (Manipulation Node)
   - 추출된 정보를 바탕으로 SMQ 생성
   - _manipulation_node() 메서드에서 수행

5. SQL 변환 (SMQ2SQL Node)
   - 생성된 SMQ를 SQL로 변환
```

**주요 노드**:

#### _manipulation_node()
- 추출된 정보를 조합하여 SMQ 생성
- LLM 프롬프트 기반 생성
- `dimensions` 필드를 `groupBy`로 변환

## SMQ 생성 규칙

### 1. 메트릭(Metrics) 규칙

**✅ 올바른 사용**:
- YML 파일의 최상위 `metrics` 섹션에 정의된 `name` 필드 값만 사용
- 예: `"metrics": ["total_brn_cnt"]`

**❌ 잘못된 사용**:
- `"metrics": ["SUM(total_brn_cnt)"]` - 단순 집계 함수 (메트릭명만 사용해야 함)
- `"metrics": ["부점코드"]` - semantic model 이름 사용
- `"metrics": ["wapco_brncd_cn"]` - entity 이름 사용
- `"metrics": ["brn_cnt"]` - measure 이름 사용

**✅ 윈도우 함수 사용**:
- 윈도우 함수가 포함된 SQL 식은 직접 `metrics`에 사용 가능
- 예: `"metrics": ["SUM(revenue) OVER (PARTITION BY customer_id ORDER BY order_date)"]`
- 예: `"metrics": ["ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC)"]`
- 예: `"metrics": ["SUM(revenue) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"]`

### 2. 차원(Dimensions) 규칙

**형식**: `{semantic_model_name}__{dimension_name}`

**예시**:
- Semantic model: "부점코드", Dimension: "brn_stcd"
- 올바른 표현: `"groupBy": ["부점코드__brn_stcd"]`
- 잘못된 표현: `"groupBy": ["brn_stcd"]`

### 3. 그룹화(Group By) 규칙

- "분류별", "카테고리별", "그룹별", "타입별", "지역별" 등의 표현이 있으면 해당 dimension을 `groupBy`에 포함
- `groupBy`에는 dimensions 값만 사용 가능

### 4. 정렬(Order By) 규칙

- 오름차순: `"orderBy": ["차원명"]`
- 내림차순: `"orderBy": ["-차원명"]` (앞에 `-` 추가)

### 5. 제한(Limit) 규칙

- 사용자가 명시하지 않으면 반드시 `null`
- 개수를 명시했을 때만 숫자 값 사용

### 6. 조인(Joins) 규칙

**형식**: SQL JOIN 문법을 문자열로 작성

**중요 사항**:
- `joins` 배열에는 **원소가 하나만** 가능합니다
- 여러 테이블 간 조인은 배열의 하나의 원소에 모두 포함하여 작성해야 합니다
- SQL 문법으로 작성하며, `sqlglot.parse_one()`으로 파싱됩니다

**✅ 올바른 사용**:

1. **기본 조인 (FROM 절 포함)**:
```json
{
  "joins": ["FROM 부점코드 LEFT JOIN 고객기본 ON 부점코드__brn_cd = 고객기본__brn_cd"]
}
```

2. **여러 테이블 조인 (하나의 문자열에 모두 포함)**:
```json
{
  "joins": ["FROM 부점코드 LEFT JOIN 고객기본 ON 부점코드__brn_cd = 고객기본__brn_cd INNER JOIN 계좌정보 ON 고객기본__cust_no = 계좌정보__cust_no"]
}
```

3. **컬럼 참조 형식**:
   - `모델명__컬럼명` 형식 사용 가능 (예: `부점코드__brn_cd`)
   - 또는 테이블 alias 사용 (예: `A.brn_cd`)

**❌ 잘못된 사용**:
- `"joins": ["gds_cd"]` - SQL 문법이 아님
- `"joins": ["부점코드", "고객기본"]` - 배열에 여러 원소 사용 불가
- `"joins": ["LEFT JOIN B ON A.id = B.id"]` - FROM 절 없음

**자동 조인 생성**:
- `joins` 필드가 비어있거나 없으면, 시스템이 자동으로 조인 경로를 생성합니다
- Primary key와 Foreign key 관계를 확인하여 자동으로 LEFT JOIN 생성
- 여러 모델이 사용된 경우 자동으로 조인 경로를 찾아 생성
- 자동 조인 생성이 실패하면 `JoinError` 발생

**조인 타입**:
- 자동 생성 시: 항상 LEFT JOIN 사용
- 수동 작성 시: LEFT JOIN, INNER JOIN, RIGHT JOIN 등 모든 조인 타입 사용 가능

## SMQ → SQL 변환 흐름

SMQ 생성 후 SQL 변환은 다음 단계로 진행됩니다:

```
1. SMQ 검증
   - metrics 존재 여부 확인
   - 사용 가능한 metrics인지 확인

2. SMQParser.parse()
   - SMQ를 AST(Abstract Syntax Tree)로 파싱

3. SQLComposer.compose()
   - AST를 SQL Expression으로 변환

4. SQL 문자열 변환
   - Expression 객체를 SQL 문자열로 변환 (dialect별)

5. 메타데이터 수집
   - 생성된 SQL에서 메타데이터 추출
```

**주요 파일**:
- `backend/semantic/services/smq2sql_service.py`: SMQ → SQL 변환 서비스
- `backend/semantic/parser/parser.py`: SMQ 파서
- `backend/semantic/composer/composer.py`: SQL 컴포저

### 조인 처리 흐름

조인은 다음 단계로 처리됩니다:

```
1. 조인 필드 확인
   - original_smq에 "joins" 필드가 있으면 수동 조인 사용
   - 없으면 자동 조인 생성

2. 수동 조인 (joins 필드가 있는 경우)
   - parse_joins() 함수로 SQL 문법 파싱
   - JOIN 절의 모든 컬럼을 찾아 각 테이블의 proj 레이어에 추가
   - agg 레이어에 조인 정보 추가

3. 자동 조인 (joins 필드가 없는 경우)
   - add_default_join() 함수 실행
   - 사용된 모델들 간의 조인 경로 찾기 (find_join_path)
   - Primary key와 Foreign key 관계 확인
   - BFS를 사용하여 조인 순서 결정
   - 모든 조인은 LEFT JOIN으로 생성
   - 조인 컬럼을 proj 레이어에 자동 추가

4. 조인 실패 처리
   - 여러 모델을 조인할 수 없는 경우 JoinError 발생
   - JoinError 발생 시 SMQ를 여러 개로 분배 (distribute_smq_with_designated_models)
```

**조인 경로 찾기 로직**:
- `find_join_path()` 함수가 두 모델 간의 조인 경로를 찾습니다
- Foreign key와 Primary key 관계를 확인합니다
- 예: 모델 A의 entity가 `type: "foreign"`이고 이름이 모델 B의 Primary key와 일치하면 조인 가능

## 윈도우 함수 지원

SMQ는 윈도우 함수(Window Function)를 완전히 지원합니다. 윈도우 함수가 포함된 메트릭이나 SQL 식을 SMQ에 직접 포함할 수 있으며, 시스템이 자동으로 처리합니다.

### 윈도우 함수 처리 방식

#### 1. 파싱 단계

- **SMQParser**: `sqlglot.parse_one()`을 사용하여 메트릭을 파싱
- 윈도우 함수가 포함된 SQL 식도 자동으로 파싱되어 `exp.Window` 노드로 변환
- 예: `SUM(revenue) OVER (PARTITION BY customer_id ORDER BY order_date)` 형태의 메트릭 지원
- 메트릭 이름이 아닌 SQL 식인 경우 자동으로 식으로 인식하여 처리

#### 2. SQL 작성 단계

- **BigQuery Dialect 특별 처리**: `write_sql.py`의 `_build_select` 함수에서 BigQuery dialect이고 agg 레이어에 윈도우 함수가 있을 경우, WHERE 절 대신 **QUALIFY 절**을 사용합니다.
  - 위치: `backend/semantic/composer/pipeline/write_sql.py:161-178`
  - 이유: BigQuery에서 윈도우 함수 결과에 대한 필터링은 QUALIFY 절을 사용해야 합니다.
  - 윈도우 함수가 포함된 필터 조건은 자동으로 QUALIFY 절로 변환됩니다.

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
- 위치: `backend/semantic/types/metric_type.py`

### 윈도우 함수 사용 방법

#### 방법 1: SQL 식으로 직접 사용

SMQ의 `metrics` 필드에 윈도우 함수가 포함된 SQL 식을 직접 문자열로 작성합니다.

```json
{
  "metrics": [
    "SUM(revenue) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total"
  ],
  "groupBy": ["고객기본__customer_id", "주문__order_date"],
  "filters": ["order_date >= '2024-01-01'"]
}
```

#### 방법 2: YML 메트릭 정의에서 윈도우 함수 사용

YML 파일의 메트릭 정의에 `window` 필드를 포함하여 윈도우 함수를 정의할 수 있습니다.

```yaml
metrics:
  - name: running_total_revenue
    description: 고객별 누적 매출
    metric_type: simple
    type_params:
      window:
        partition_by: ["customer_id"]
        order_by: ["order_date"]
        frame:
          type: ROWS
          start: UNBOUNDED PRECEDING
          end: CURRENT ROW
```

### 윈도우 함수 사용 시 주의사항

1. **BigQuery Dialect**: 윈도우 함수 결과에 대한 필터링은 자동으로 QUALIFY 절로 변환됩니다.
2. **파싱 검증**: 윈도우 함수가 포함된 메트릭은 `sqlglot.parse_one()`으로 파싱 가능해야 합니다.
3. **PARTITION BY와 GROUP BY**: `groupBy` 필드와 윈도우 함수의 `PARTITION BY`는 별개입니다.
   - `groupBy`: 집계 기준
   - `PARTITION BY`: 윈도우 함수의 파티셔닝 기준

### 지원하는 윈도우 함수

표준 SQL 윈도우 함수 모두 지원:
- 집계 함수: `SUM()`, `AVG()`, `COUNT()`, `MAX()`, `MIN()` 등
- 순위 함수: `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `PERCENT_RANK()` 등
- 분석 함수: `FIRST_VALUE()`, `LAST_VALUE()`, `LAG()`, `LEAD()`, `NTH_VALUE()` 등

### 윈도우 프레임 지정

윈도우 프레임도 지원합니다:
- `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
- `RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING`
- `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING`

## 프롬프트 기반 생성 상세

### SemanticAgent 프롬프트 핵심 내용

```
1. YML 파일 구조 설명
   - semantic_models 섹션 구조
   - metrics 섹션 위치 및 사용법

2. 메트릭 선택 규칙
   - 오직 metrics 섹션의 name 필드만 사용
   - 집계 함수 추가 금지

3. 차원 선택 규칙
   - semantic_model_name__dimension_name 형식
   - 접두어 필수

4. SMQ JSON 형식 예시
```

### SMQAgent 프롬프트 핵심 내용

**프롬프트 파일**: `backend/prompts/smq_prompt.txt`

주요 지침:
- Tool 사용 순서 강제
- YML 파일 분석 방법
- SMQ 작성 유의사항
- 에러 발생 시 수정 방법

**Tool 제약사항**:
- ✅ 사용 가능: SemanticModelSelector.selectSemanticModelFiles, SemanticModelQuery.convertSmqToSql, SemanticModelQuery.editSmq
- ❌ 사용 불가: getRandomYmlFile, generateSmqAndConvert

## 에러 처리 및 수정

### 일반적인 에러 유형

1. **메트릭 없음**: `No metrics specified in request`
2. **존재하지 않는 메트릭**: `Metric 'XXX' not found`
3. **조인 에러**: Join 조건이 부족한 경우 `JoinError` 발생
   - 여러 모델을 조인할 수 없는 경우
   - Primary key와 Foreign key 관계가 없는 경우
   - 연결되지 않은 모델 컴포넌트가 있는 경우
4. **순환 참조**: 메트릭 간 순환 의존성 (`RecursionError`)
5. **조인 문법 오류**: `joins` 배열에 여러 원소가 있거나 SQL 문법이 잘못된 경우

### 에러 발생 시 대응

1. **SMQAgent**: `editSmq` Tool을 사용하여 SMQ 수정
2. **JoinError**: SMQ를 여러 개로 분배 (`distribute_smq_with_designated_models`)
   - 여러 모델이 조인 불가능한 경우, 모델을 그룹으로 나누어 각각 SMQ 생성
3. **메트릭 에러**: YML 파일 확인 후 올바른 메트릭 이름 사용
4. **조인 에러**: 
   - 수동 조인 작성 시 SQL 문법 확인
   - 자동 조인 실패 시 YML 파일의 entities에서 Primary key와 Foreign key 관계 확인
   - `joins` 필드를 명시적으로 작성하여 조인 경로 지정

## 사용 예시

### 예시 1: 단순 집계 쿼리

**사용자 질의**: "부점별 총 부점 수를 조회해줘"

**생성된 SMQ**:
```json
{
  "metrics": ["total_brn_cnt"],
  "groupBy": ["부점코드__brn_stcd"],
  "filters": [],
  "orderBy": [],
  "limit": null,
  "joins": []
}
```

### 예시 2: 필터링 및 정렬 포함

**사용자 질의**: "2024년 고객별 계좌 수를 많은 순으로 상위 10개만"

**생성된 SMQ**:
```json
{
  "metrics": ["account_count"],
  "groupBy": ["고객기본__cust_no"],
  "filters": ["date >= '2024-01-01'"],
  "orderBy": ["-account_count"],
  "limit": 10,
  "joins": []
}
```

### 예시 3: 윈도우 함수 사용

**사용자 질의**: "고객별 주문일자 순으로 누적 매출을 구해줘"

**생성된 SMQ**:
```json
{
  "metrics": [
    "SUM(amount) OVER (PARTITION BY 고객기본__cust_no ORDER BY 주문__order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total"
  ],
  "groupBy": ["고객기본__cust_no", "주문__order_date"],
  "filters": [],
  "orderBy": ["고객기본__cust_no", "주문__order_date"],
  "limit": null,
  "joins": []
}
```

### 예시 4: 윈도우 함수와 필터 조합 (BigQuery)

**사용자 질의**: "고객별 누적 매출이 1000을 넘는 주문만 조회"

**생성된 SMQ**:
```json
{
  "metrics": [
    "SUM(amount) OVER (PARTITION BY 고객기본__cust_no ORDER BY 주문__order_date) AS running_total"
  ],
  "groupBy": ["고객기본__cust_no", "주문__order_date"],
  "filters": ["SUM(amount) OVER (PARTITION BY 고객기본__cust_no ORDER BY 주문__order_date) > 1000"],
  "orderBy": ["고객기본__cust_no", "주문__order_date"],
  "limit": null,
  "joins": []
}
```

**변환된 SQL (BigQuery)**:
```sql
SELECT 
  customer_id,
  order_date,
  SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_total
FROM agg
QUALIFY running_total > 1000  -- WHERE 대신 QUALIFY 사용
```

### 예시 5: 조인 사용

**사용자 질의**: "부점별 고객 수와 계좌 수를 함께 조회"

**생성된 SMQ (자동 조인)**:
```json
{
  "metrics": ["customer_count", "account_count"],
  "groupBy": ["부점코드__brn_cd"],
  "filters": [],
  "orderBy": [],
  "limit": null,
  "joins": []
}
```

**생성된 SMQ (수동 조인)**:
```json
{
  "metrics": ["customer_count", "account_count"],
  "groupBy": ["부점코드__brn_cd"],
  "filters": [],
  "orderBy": [],
  "limit": null,
  "joins": ["FROM 부점코드 LEFT JOIN 고객기본 ON 부점코드__brn_cd = 고객기본__brn_cd LEFT JOIN 계좌정보 ON 고객기본__cust_no = 계좌정보__cust_no"]
}
```

**변환된 SQL**:
```sql
SELECT 
  부점코드.brn_cd,
  COUNT(DISTINCT 고객기본.cust_no) AS customer_count,
  COUNT(DISTINCT 계좌정보.acct_no) AS account_count
FROM 부점코드
LEFT JOIN 고객기본 ON 부점코드.brn_cd = 고객기본.brn_cd
LEFT JOIN 계좌정보 ON 고객기본.cust_no = 계좌정보.cust_no
GROUP BY 부점코드.brn_cd
```

## 주요 파일 위치

```
backend/
├── semantic_agent.py          # SemanticAgent (방식 1)
├── smq_agent.py               # SMQAgent (방식 2)
├── langgraph_agent.py         # LangGraphAgent (방식 3)
├── prompts/
│   └── smq_prompt.txt         # SMQ 생성 프롬프트
└── semantic/
    ├── services/
    │   └── smq2sql_service.py # SMQ → SQL 변환 서비스
    ├── parser/
    │   └── parser.py          # SMQ 파서
    └── composer/
        └── composer.py        # SQL 컴포저
```

## 참고사항

- SMQ는 join 문제만 없다면 원칙적으로 하나의 SQL 쿼리로 변환됩니다
- Metrics는 시멘틱 모델로부터 독립적이므로 prefix 없이 사용
- Dimensions/Measures는 시멘틱 모델에 종속적이므로 `모델명__요소명` 형식 필수
- 모든 집계 함수는 메트릭 정의 시점에 포함되어 있으므로 SMQ에는 메트릭명만 사용
- **윈도우 함수**: 윈도우 함수가 포함된 SQL 식은 `metrics` 필드에 직접 사용 가능하며, 시스템이 자동으로 파싱하고 처리합니다
- **BigQuery QUALIFY 절**: BigQuery dialect에서는 윈도우 함수 결과에 대한 필터링이 자동으로 QUALIFY 절로 변환됩니다
- **조인 자동 생성**: `joins` 필드가 없으면 Primary key와 Foreign key 관계를 기반으로 자동으로 조인 경로를 생성합니다
- **조인 문법**: `joins` 배열에는 SQL JOIN 문법을 하나의 문자열로 작성해야 하며, `모델명__컬럼명` 형식으로 컬럼 참조 가능
