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
`.env.example`을 `.env`로 변경하고 키 입력:
```
OPENAI_API_KEY=sk-xxxx
```

### 4. 실행
```bash
# 로그 분석 테스트
python main.py --logs

# 툴 히스토리 분석 테스트
python main.py --tools
```
