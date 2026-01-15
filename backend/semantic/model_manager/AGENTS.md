# 시멘틱 모델 파싱 & 린터 구조

## 개요

`service/semantic/model_manager` 디렉터리는 **시멘틱 모델/메트릭 YAML 및 DDL 정보**를 파싱하고,  
정적 린트 규칙을 통해 **스키마 일관성·타입·참조 관계**를 검증하는 레이어입니다.
또한 **DDL SQL 파일로부터 시멘틱 모델 초안(draft)을 자동 생성**하는 기능도 제공합니다.

- **런타임 SMQ → SQL 변환**(SMQParser, SQLComposer 등)은 `service/semantic/AGENTS.md`에서 설명합니다.
- 이 문서는 그보다 앞단에서 동작하는 **시멘틱 모델 파싱·정규화·린팅·draft 생성 파이프라인**에 초점을 둡니다.

## 전체 아키텍처

### 파싱 & 린팅 파이프라인

```
semantic 프로젝트 루트(base_dir)
  ├─ ddl.sql
  ├─ sources.yml
  └─ semantic_models/
       ├─ *.yml (semantic_models + metrics 정의)
       └─ ...

        │
        │ 1) 파일 로딩 & 파싱
        ↓
  file_loader.py / semantic_parser.py
        │
        │ 2) 타입/구조 검증 유틸
        ↓
  parsing_validation.py
        │
        │ 3) 린트 이슈 포맷 정의
        ↓
  lint_types.py
        │
        │ 4) 필드명 오타 검사 (ERROR)
        ↓
  yaml_field_linter.py
        │
        │ 5) 이름 관련 검사
        ↓
  name_linter.py
        │
        │ 6) 필드명 오타 검사 (WARN, 유사도 기반)
        ↓
  field_typo_linter.py
        │
        │ 7) expr/DDL 기반 상세 룰
        ↓
  expr_linter.py
        │
        │ 8) 전체 orchestration
        ↓
  semantic_linter.lint_semantic_models(base_dir)
        │
        ↓
  SemanticLintResult (success, issues, error_count, warning_count)
```

### Draft 생성 파이프라인

```
DDL SQL 파일 (ddl.sql)
  │
  │ 1) DDL dialect 감지 및 파싱
  ↓
  ddl_parser.py → Dict[str, TableInfo]
        │
        │ 2) Draft 생성
        ↓
  draft_generator.py
        │
        ↓
  DraftResponse (path, generatedAt, proposals[])
```

## 핵심 엔트리 포인트

### `lint_semantic_models(base_dir: str)` (`semantic_linter.py`)

**역할**: `base_dir` 하위의 시멘틱 정의 파일들을 종합적으로 검사합니다.

- **검사 항목**:
  1. **필드명 오타 검사 (ERROR)**: 최상위 레벨 필드명, metric 필드명이 유효한 필드 목록에 있는지 검증
  2. **필수 필드 누락 (ERROR)**: semantic model, entity, dimension, measure의 필수 필드 존재 여부 검증
     - 필수 필드가 없을 경우, 실제 필드명 중에서 필수 필드와 유사한 것이 있으면 오타 제안 포함
  3. **선택 필드 검증 (WARN)**: semantic model, entity, dimension, measure, metric 내부 선택 필드명 검증
     - 선택 필드와 정확히 일치: OK
     - 선택 필드와 유사도가 높음 (임계값 0.6 이상): WARN + 오타 제안 (`SEM502_TYPO_IN_FIELD_NAME`)
     - 선택 필드와 유사도가 낮음: WARN (`SEM503_INVALID_OPTIONAL_FIELD`)
     - 필수 필드는 `yaml_field_linter.py`에서 검증되므로 건너뜀
  4. **이름 중복**: dimension/measure 이름 중복, metric 이름 전역 중복 검증
  5. **파일명과 모델 이름 일치 여부 (WARN)**: 파일명에서 추출한 후보와 semantic model name 일치 여부 검증
  6. **Entity 검증**:
     - Entity type 값 범위 검증 (ERROR): "primary" 또는 "foreign"만 허용
     - Foreign entity와 primary entity 매칭 검증 (WARN): foreign entity의 name이 전체 semantic models의 primary entity name과 매칭되는지 검증
  7. **타입 값 범위 검증**: dimension/measure의 DataType, metric의 metric_type/DataType, entity의 EntityType 범위 검증
  8. **Metric expr 참조 유효성**: metric expr/type_params 내 참조가 실제 measures/dimensions/metrics에 존재하는지 검증
  9. **DDL 컬럼 존재 여부**: dimensions/measures expr에서 사용된 컬럼이 DDL에 실제 존재하는지 검증
  10. **사용되지 않은 DDL 컬럼 (WARN)**: DDL에는 있지만 어떤 dimension/measure에서도 사용되지 않은 컬럼 경고
  11. **Sources.yml 매핑 유효성**: 테이블 레퍼런스와 sources.yml 매핑 유효성 검증

**반환 값**: `SemanticLintResult`

```python
{
    "success": bool,
    "issues": [SemanticLintIssue, ...],
    "error_count": int,
    "warning_count": int,
}
```

**에러 코드 체계**:
- **ERROR**: `SEM000` ~ `SEM021` (0xx 범위)
  - `SEM000`: DDL 파일 없음
  - `SEM001`: sources.yml 파싱 실패
  - `SEM002`: Metric 이름 중복
  - `SEM003`: Metric metric_type 값 범위 초과
  - `SEM004`: Metric DataType 값 범위 초과
  - `SEM006`: DDL에 없는 컬럼 사용
  - `SEM007`: Dimension 이름 중복
  - `SEM008`: Measure 이름 중복
  - `SEM009`: Dimension/Measure 이름 충돌
  - `SEM010`: 잘못된 테이블 레퍼런스
  - `SEM011`: sources.yml에 정의되지 않은 소스
  - `SEM012`: Dimension DataType 값 범위 초과
  - `SEM013`: Measure DataType 값 범위 초과
  - `SEM014`: 필수 필드 누락
  - `SEM015`: 최상위 레벨 필드명 오타
  - `SEM020`: Metric 필드명 오타
  - `SEM021`: Entity EntityType 값 범위 초과
- **WARN**: `SEM500` ~ `SEM600`
  - `SEM501`: 파일명과 모델 이름 불일치
  - `SEM502`: 선택 필드명 오타 (유사도 기반, 임계값 0.6), Foreign entity와 primary entity 매칭 실패
  - `SEM503`: 유효하지 않은 선택 필드 (선택 필드와 유사하지 않은 필드명)
  - `SEM600`: 사용되지 않은 DDL 컬럼

### `assemble_manifest(base_dir: str)` (`parser/semantic_parser.py`)

**역할**: `base_dir` 하위의 모든 시멘틱 정의 파일을 읽어 런타임에서 사용할 수 있는 manifest 구조를 생성합니다.

- `sources.yml`, `semantic_models/*.yml`, `date.yml`, `time_spine_daily.sql`을 읽어 통합
- Semantic model/metric 이름 중복 검증
- 내부 포맷으로 변환 및 정규화

**반환 값**: `Dict[str, Any]` (semantic_manifest.json 구조)

### `generate_draft(parsed_tables: Dict[str, TableInfo], source_name: str = "default", ddl_path: str = "")` (`draft/draft_generator.py`)

**역할**: 파싱된 테이블 정보를 기반으로 `sources.yml`과 `semantic_models/*.yml` 파일들의 초안을 생성합니다.

- Primary Key → entities (type: "primary")
- Foreign Key → entities (type: "foreign")
- 숫자 타입 컬럼 → measures
- 그 외 컬럼 → dimensions

**반환 값**: `DraftResponse` (path, generatedAt, proposals[])

### `parse_ddl(ddl_path: str, dbms: Optional[str] = None)` (`utils/ddl_parser.py`)

**역할**: DDL SQL 파일을 파싱하여 테이블 정보를 추출합니다.

- 파일 첫머리의 `-- dialect` 주석을 통해 자동 감지
- 지원 dialect: `mysql`, `postgres`, `postgresql`, `sqlite`, `oracle`, `tsql`, `mssql`, `bigquery`, `snowflake`, `duckdb`

**반환 값**: `Dict[str, TableInfo]`

## 모듈별 역할

### 파싱 & 검증

- **`semantic_parser.py`**: YAML을 런타임용 내부 구조로 변환 (`assemble_manifest`, `transform_semantic_model`, `normalize_metrics` 등)
- **`file_loader.py`**: YAML/DDL 파일 로딩 및 컨텍스트 제공 (`load_semantic_models_with_files`, `load_metrics_with_files`, `parse_ddl_tables`)
- **`parsing_validation.py`**: 타입 및 이름 검증 로직 (파서와 린터에서 공통 사용)
- **`field_schema.py`**: 필수 필드와 선택 필드 정의
  - Semantic Model: 필수 `{"name", "table"}`, 선택 `{"description", "entities", "dimensions", "measures", "label", "config", "node_relation", "primary_entity"}`
  - Entity: 필수 `{"name", "type"}`, 선택 `{"expr", "description", "role", "label"}`
  - Dimension: 필수 `{"name", "type"}`, 선택 `{"label", "description", "expr", "type_params"}`
  - Measure: 필수 `{"name", "type"}`, 선택 `{"label", "description", "expr", "agg"}`
  - Metric: 필수 `{"name", "metric_type"}`, 선택 `{"description", "type", "label", "expr", "type_params"}`

### 린터 모듈

- **`lint_types.py`**: 이슈/결과 타입 정의 및 헬퍼 (`SemanticLintIssue`, `SemanticLintResult`, `make_error`, `make_warn` 등), 타입 값 범위 검증 (`lint_semantic_model_types`, `lint_metric_type_enums`, `lint_entity_types`)
- **`yaml_field_linter.py`**: 필드명 오타 검사 (ERROR) 및 필수 필드 누락 검사
  - `lint_top_level_field_names`: 최상위 레벨 필드명 오타 검사
  - `lint_metric_field_names`: metric 필드명 오타 검사
  - `lint_semantic_model_required_fields`: semantic model, entity, dimension, measure의 필수 필드 누락 검사
    - 필수 필드가 없을 경우, 실제 필드명 중에서 필수 필드와 유사한 것이 있으면 오타 제안 포함
    - `field_typo_linter.py`의 `_check_typo` 함수를 재사용하여 유사도 계산
- **`field_typo_linter.py`**: 선택 필드명 유사도 기반 오타 검사 (WARN)
  - `_check_typo`: 유사도 계산 함수 (difflib.SequenceMatcher 사용, 임계값 0.6)
  - `lint_field_typos_in_semantic_models`: semantic model, entity, dimension, measure 내부 선택 필드명 검증
    - 필수 필드는 건너뛰고 선택 필드만 검증
    - 선택 필드와 유사도가 높으면 `SEM502_TYPO_IN_FIELD_NAME` (오타 제안 포함)
    - 선택 필드와 유사도가 낮으면 `SEM503_INVALID_OPTIONAL_FIELD`
  - `lint_field_typos_in_metrics`: metric 내부 선택 필드명 검증 (동일한 로직)
- **`name_linter.py`**: 이름 중복 및 파일명 일치 검사, entity 매칭 검증
  - `lint_semantic_model_name_uniqueness`: dimension/measure 이름 중복 검사
  - `lint_metric_uniqueness`: metric 이름 전역 중복 검사
  - `lint_filename_model_name_consistency`: 파일명과 모델 이름 일치 여부 검사
  - `lint_foreign_entity_primary_match`: foreign entity와 primary entity name 매칭 검증
- **`expr_linter.py`**: SQL 표현식 수준의 컬럼/이름 참조 유효성 검사 (`sqlglot` 사용)
- **`semantic_linter.py`**: 모든 린터 모듈을 조합하여 전체 린팅 플로우 orchestration

### Draft 생성

- **`draft/ddl_parser.py`**: DDL 파일 파싱 (dialect 자동 감지)
- **`draft/ddl_parsers/`**: DBMS별 DDL 파서 구현 (PostgreSQL, MySQL, Oracle, BigQuery, MSSQL)
- **`draft/draft_generator.py`**: 테이블 정보를 기반으로 `sources.yml`과 `semantic_models/*.yml` 생성
- **`draft/type_mapping.py`**: DBMS 타입을 DataType으로 매핑
- **`draft/name_converter.py`**: 테이블명/컬럼명을 시멘틱 모델 네이밍 규칙에 맞게 변환

## SMQ → SQL 파이프라인과의 관계

- `model_manager` 레이어는 **시멘틱 정의 파일의 품질을 사전에 보장**하는 역할을 합니다.
- 런타임에서 SMQ를 받아 SQL을 생성하는 파이프라인 구조는 `service/semantic/AGENTS.md` 문서를 참고하세요.
- 일반적인 흐름:
  1. **초기 설정**: DDL → `generate_draft()` → `sources.yml`, `semantic_models/*.yml` 초안 생성
  2. **개발**: Draft 수동 수정/보완
  3. **검증**: `lint_semantic_models(base_dir)` 실행
  4. **배포**: 검증 통과한 정의만 프로덕션 반영
  5. **런타임**: 검증된 정의를 바탕으로 SMQ → SQL 변환 수행
