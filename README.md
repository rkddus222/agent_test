# Agent Debugger PoC

이 프로젝트는 에이전트 개발자가 로그 파일이나 툴 호출 이력을 LLM에게 제공하여 문제 원인을 분석하고 해결책을 제안받는 "에이전트 디버깅" 개념을 검증하기 위한 초기 PoC입니다.

## 기능

1. **로그 분석**: 로그 파일의 텍스트를 분석하여 에러 위치, 원인, 해결책을 제안합니다.
2. **툴 히스토리 분석**: 툴 호출 이력(JSON)을 분석하여 실패한 툴 호출과 그 원인을 식별합니다.

## 설치 및 실행

### 1. 사전 준비
- Python 3.8 이상
- OpenAI API Key

### 2. 가상환경 및 패키지 설치
```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

pip install -r requirements.txt
```

### 3. API 키 설정
프로젝트 루트 디렉토리에 `.env` 파일을 생성하고 OpenAI API Key를 입력하세요:

**방법 1: .env.example을 복사**
```bash
# Windows PowerShell:
Copy-Item .env.example .env

# Mac/Linux:
cp .env.example .env
```

**방법 2: 직접 생성**
프로젝트 루트 디렉토리에 `.env` 파일을 만들고 다음 내용을 입력:
```
OPENAI_API_KEY=sk-your-api-key-here
```

그 다음 `.env` 파일을 열어서 `your-api-key-here` 부분을 실제 OpenAI API Key로 교체하세요.

**중요**: `.env` 파일은 Git에 커밋하지 마세요. 이미 `.gitignore`에 포함되어 있습니다.

### 4. 실행
```bash
# 로그 분석 테스트
python main.py --logs

# 툴 히스토리 분석 테스트
python main.py --tools
```
