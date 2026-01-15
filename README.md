# Semantic Model Query (SMQ) to SQL 변환 시스템

시멘틱 모델 기반 쿼리(SMQ)를 SQL 쿼리로 자동 변환하는 시스템입니다. 자연어 또는 SMQ 형식의 요청을 받아서 최적화된 SQL 쿼리를 생성합니다.

## 주요 기능

1. **시멘틱 모델 관리**
   - YAML 형식의 시멘틱 모델 정의 (sources.yml, semantic_models/*.yml)
   - DDL로부터 시멘틱 모델 초안 자동 생성
   - 시멘틱 모델 린팅 및 검증

2. **SMQ to SQL 변환**
   - Semantic Model Query (SMQ)를 SQL로 변환
   - 다양한 SQL dialect 지원 (BigQuery, PostgreSQL, MySQL, Oracle, MSSQL 등)
   - 메트릭, 필터, 그룹화, 정렬, 조인 등을 지원하는 복잡한 쿼리 생성

3. **에이전트 기반 인터페이스**
   - Semantic Agent: 시멘틱 모델 관리 및 YAML 파일 편집
   - SMQ Agent: 자연어를 SMQ로 변환
   - Evaluation Agent: NL2SQL 시스템 평가

4. **웹 기반 UI**
   - React 기반 프론트엔드
   - 실시간 채팅 인터페이스
   - 시멘틱 모델 파일 탐색 및 편집
   - 프롬프트 편집 및 관리

## 프로젝트 구조

```
agent_test/
├── backend/                    # Python 백엔드
│   ├── semantic/              # 시멘틱 모델 핵심 로직
│   │   ├── services/         # 서비스 레이어
│   │   │   ├── semantic_model_service.py
│   │   │   └── smq2sql_service.py
│   │   ├── parser/           # SMQ 파서
│   │   ├── composer/         # SQL 컴포저
│   │   ├── model_manager/    # 모델 관리 (파싱, 린팅, draft 생성)
│   │   ├── types/            # 타입 정의
│   │   └── utils/            # 유틸리티 함수
│   ├── prompts/              # 프롬프트 파일
│   │   ├── system_prompt.txt
│   │   ├── smq_prompt.txt
│   │   ├── yml_management_prompt.txt
│   │   └── evaluation_prompt.txt
│   ├── playground/           # 작업 디렉토리
│   │   ├── semantic_models/  # 시멘틱 모델 YAML 파일
│   │   ├── sources.yml
│   │   ├── ddl.sql
│   │   └── semantic_manifest.json
│   ├── app.py                # FastAPI 메인 애플리케이션
│   ├── semantic_agent.py     # Semantic Agent
│   ├── smq_agent.py          # SMQ Agent
│   └── tools.py              # 도구 함수들
├── frontend/                  # React 프론트엔드
│   ├── src/
│   │   ├── pages/           # 페이지 컴포넌트
│   │   ├── components/      # 재사용 컴포넌트
│   │   └── utils/           # 유틸리티
│   └── package.json
└── README.md
```

## 설치 및 실행

### 사전 요구사항

- Python 3.8 이상
- Node.js 18 이상
- OpenAI API Key

### 1. 백엔드 설정

```bash
# 가상환경 생성 및 활성화
python -m venv venv

# Windows:
venv\Scripts\activate

# Mac/Linux:
source venv/bin/activate

# 패키지 설치
cd backend
pip install -r requirements.txt
```

### 2. 환경 변수 설정

프로젝트 루트 디렉토리에 `.env` 파일을 생성하고 OpenAI API Key를 설정하세요:

```env
OPENAI_API_KEY=sk-your-api-key-here
```

**중요**: `.env` 파일은 Git에 커밋하지 마세요. 이미 `.gitignore`에 포함되어 있습니다.

### 3. 프론트엔드 설정

```bash
cd frontend
npm install
```

### 4. 실행

**백엔드 서버 실행:**
```bash
cd backend
uvicorn app:app --reload --port 8000
```

**프론트엔드 개발 서버 실행:**
```bash
cd frontend
npm run dev
```

브라우저에서 `http://localhost:5173` (또는 Vite가 할당한 포트)로 접속하세요.

## 주요 사용법

### 시멘틱 모델 생성

1. `backend/playground/ddl.sql` 파일에 DDL 스크립트 작성
2. 웹 UI에서 "DDL에서 생성" 기능 사용
3. 또는 API 엔드포인트 `/api/ddl/create` 사용

### SMQ to SQL 변환

**API 사용:**
```bash
POST /api/smq/convert
{
  "smq": {
    "metrics": ["total_revenue"],
    "groupBy": ["customer_id"],
    "filters": ["order_date >= '2024-01-01'"],
    "orderBy": ["-total_revenue"],
    "limit": 100
  },
  "dialect": "bigquery"
}
```

**에이전트 사용:**
- 웹 UI에서 SMQ Agent 탭 선택
- 자연어로 요청하면 SMQ를 생성하고 SQL로 변환

### 시멘틱 모델 린팅

```bash
POST /api/lint
{
  "path": "backend/playground"
}
```

## 기술 스택

### 백엔드
- **FastAPI**: 웹 프레임워크
- **OpenAI API**: LLM 기반 에이전트
- **SQLGlot**: SQL 파싱 및 변환
- **PyYAML**: YAML 파일 처리
- **Pydantic**: 데이터 검증

### 프론트엔드
- **React**: UI 라이브러리
- **Vite**: 빌드 도구
- **Axios**: HTTP 클라이언트
- **React Markdown**: 마크다운 렌더링

## 개발 가이드

### 코드 구조

- **`backend/semantic/services/`**: 비즈니스 로직 서비스
  - `semantic_model_service.py`: 시멘틱 모델 파싱, 린팅, draft 생성
  - `smq2sql_service.py`: SMQ to SQL 변환

- **`backend/semantic/parser/`**: SMQ 파서
  - SMQ를 파싱하여 테이블별로 그룹화된 구조로 변환

- **`backend/semantic/composer/`**: SQL 컴포저
  - 파싱된 SMQ를 SQL 쿼리로 변환 (14단계 파이프라인)

- **`backend/semantic/model_manager/`**: 모델 관리
  - 파싱, 린팅, draft 생성

### 문서

자세한 구조 설명은 다음 문서를 참고하세요:
- `backend/semantic/AGENTS.md`: SMQ to SQL 변환 전체 구조
- `backend/semantic/parser/AGENTS.md`: SMQ Parser 구조
- `backend/semantic/composer/AGENTS.md`: SQL Composer 구조
- `backend/semantic/model_manager/AGENTS.md`: 모델 관리 구조

## 라이선스

이 프로젝트는 내부 사용을 위한 프로젝트입니다.
