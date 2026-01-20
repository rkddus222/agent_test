import os
import json
import yaml
import asyncio

# vendor 디렉토리 경로 설정 (가장 먼저 실행되어야 함)
import vendor_setup  # noqa: F401

from openai import AsyncOpenAI
from dotenv import load_dotenv
from tools import read_file, edit_file, get_random_yml_file, convert_smq_to_sql

load_dotenv()

class SemanticAgent:
    def __init__(self, llm_config: dict = None):
        """
        Args:
            llm_config: LLM 설정 딕셔너리 (vLLM 사용 시)
                - url: vLLM 서버 URL
                - model_name: 모델 이름
                - model_type: 'vllm'
                - temperature: temperature 설정
                - max_tokens: 최대 토큰 수
        """
        self.llm_config = llm_config
        
        if llm_config and llm_config.get("model_type") == "vllm":
            # vLLM 사용
            base_url = llm_config.get("url", "http://localhost:8000/v1")
            if not base_url.endswith("/v1"):
                base_url = base_url.rstrip("/") + "/v1"
            # vLLM은 OpenAI 호환 API를 제공하므로 base_url만 설정하면 됨
            # API 키는 필요 없지만, OpenAI 클라이언트가 요구하므로 더미 값 사용
            api_key = os.getenv("OPENAI_API_KEY", "dummy-key-for-vllm")
            self.client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        else:
            # OpenAI 사용
            self.api_key = os.getenv("OPENAI_API_KEY")
            self.client = AsyncOpenAI(api_key=self.api_key) if self.api_key else None
        
        self.tool_history = []

    def get_directory_structure(self):
        # Scan playground directory
        base_dir = os.path.join(os.path.dirname(__file__), 'playground')
        files = []
        for root, _, filenames in os.walk(base_dir):
            for filename in filenames:
                rel_path = os.path.relpath(os.path.join(root, filename), base_dir)
                files.append(rel_path)
        return "\n".join(files)

    async def run(self, user_request, system_prompt, max_turns=5, request_type=None):
        if not self.client:
            yield {"type": "error", "content": "OPENAI_API_KEY not set."}
            return

        self.tool_history = [] # Reset history for new request
        # 테스트 케이스 생성 결과 추적
        test_case_data = {
            "user_query": None,
            "yml_file": None,
            "smq": None,
            "sql": None,
            "metadata": None,
            "validation": None
        }
        
        for turn in range(max_turns):
            directory_structure = self.get_directory_structure()
            tool_history_str = json.dumps(self.tool_history, indent=2, ensure_ascii=False)
            
            # request_type이 제공되지 않으면 기본값 사용
            request_type_str = request_type if request_type else "기타"
            
            prompt = system_prompt.replace("{directory_structure}", directory_structure)\
                                  .replace("{user_request}", user_request)\
                                  .replace("{tool_history}", tool_history_str)\
                                  .replace("{request_type}", request_type_str)
            
            try:
                # LLM 설정에서 model_name과 temperature 가져오기
                model_name = "gpt-4o"
                temperature = 0.0
                max_tokens = None
                if self.llm_config:
                    model_name = self.llm_config.get("model_name", model_name)
                    temperature = self.llm_config.get("temperature", temperature)
                    max_tokens = self.llm_config.get("max_tokens")
                
                create_params = {
                    "model": model_name,
                    "messages": [
                        {"role": "system", "content": "You are a helpful Semantic Model Engineer."},
                        {"role": "user", "content": prompt}
                    ],
                    "response_format": {"type": "json_object"}
                }
                if temperature is not None:
                    create_params["temperature"] = temperature
                if max_tokens is not None:
                    create_params["max_tokens"] = max_tokens
                
                response = await self.client.chat.completions.create(**create_params)
            except asyncio.CancelledError:
                yield {"type": "error", "content": "작업이 취소되었습니다."}
                return
            except Exception as e:
                yield {"type": "error", "content": f"LLM 호출 오류: {str(e)}"}
                break
            
            content = response.choices[0].message.content
            if not content:
                yield {"type": "error", "content": "Empty response from LLM."}
                break
                
            try:
                data = json.loads(content)
            except json.JSONDecodeError:
                yield {"type": "thought", "content": f"Raw output (JSON parse failed): {content}"}
                break
            
            reasoning = data.get("reasoning") or data.get("<reasoning>")
            tool_call_data = data.get("tool_call") or data.get("<tool_call>")
            
            if not tool_call_data and "tool_call" in data:
                tool_call_data = data["tool_call"]
            
            if reasoning:
                yield {"type": "thought", "content": reasoning}
            
            if tool_call_data:
                # Execute tool
                tool_name = tool_call_data.get("tool")
                args = tool_call_data.get("arguments", {})
                
                yield {
                    "type": "tool_call", 
                    "content": f"Calling {tool_name}",
                    "tool": tool_name,
                    "args": args
                }
                
                result = None
                if tool_name == "readFile":
                    result = read_file(args.get("path"))
                elif tool_name == "editFile":
                    result = edit_file(args.get("proposals"))
                elif tool_name == "getRandomYmlFile":
                    result = get_random_yml_file()
                    if result.get("path"):
                        test_case_data["yml_file"] = result.get("path")
                elif tool_name == "generateSmqAndConvert":
                    user_query = args.get("user_query", "")
                    test_case_data["user_query"] = user_query
                    result = await self._generate_smq_and_convert(
                        args.get("yml_file_content"),
                        user_query,
                        args.get("manifest_path")
                    )
                    if result.get("success"):
                        test_case_data["smq"] = result.get("smq")
                        test_case_data["sql"] = result.get("sql")
                        test_case_data["metadata"] = result.get("metadata")
                elif tool_name == "validateQuery":
                    result = await self._validate_query(
                        args.get("sql_query"),
                        args.get("user_query")
                    )
                    if result.get("is_valid") is not None:
                        test_case_data["validation"] = {
                            "is_valid": result.get("is_valid"),
                            "score": result.get("score", 0),
                            "reason": result.get("reason", ""),
                            "issues": result.get("issues", [])
                        }
                else:
                    result = {"error": f"Unknown tool: {tool_name}"}
                
                # Log to history
                self.tool_history.append({
                    "request": tool_call_data,
                    "response": result
                })
                
                yield {"type": "tool_result", "content": json.dumps(result, indent=2, ensure_ascii=False)}
                
                # 검증이 성공적으로 완료된 경우
                if tool_name == "validateQuery" and result.get("is_valid") and result.get("score", 0) >= 80:
                    # 테스트 케이스 결과 포맷팅
                    success_content = self._format_test_case_result(test_case_data)
                    yield {"type": "success", "content": success_content}
                    return
                
                # validateQuery가 실패했지만 generateSmqAndConvert는 성공한 경우
                if tool_name == "validateQuery" and result.get("is_valid") is not None:
                    # 검증 결과와 함께 결과 표시
                    success_content = self._format_test_case_result(test_case_data)
                    yield {"type": "success", "content": success_content}
                    return
                
                if tool_name == "editFile" and result.get("success") and result.get("error_count", 0) == 0:
                    yield {"type": "success", "content": "Changes applied successfully without errors."}
                    return

            else:
                # No tool call? Maybe finished or asking clarification?
                # 더 이상 툴 호출이 없고, generateSmqAndConvert가 성공했다면 결과 표시
                if test_case_data.get("sql"):
                    # SQL이 생성되었으면 결과 표시 (검증 여부와 관계없이)
                    success_content = self._format_test_case_result(test_case_data)
                    yield {"type": "success", "content": success_content}
                    return
                
                yield {"type": "message", "content": content}
                return
        
        # 루프가 끝났는데도 결과가 표시되지 않았다면 (max_turns 도달 등)
        if test_case_data.get("sql"):
            success_content = self._format_test_case_result(test_case_data)
            yield {"type": "success", "content": success_content}
    
    def _format_test_case_result(self, test_case_data):
        """
        테스트 케이스 생성 결과를 포맷팅합니다.
        """
        lines = []
        lines.append("## 테스트 케이스 생성 완료\n")
        
        if test_case_data.get("yml_file"):
            lines.append(f"**YML 파일**: `{test_case_data['yml_file']}`\n")
        
        if test_case_data.get("user_query"):
            lines.append(f"**생성된 질문**: {test_case_data['user_query']}\n")
        
        if test_case_data.get("smq"):
            lines.append("**SMQ (Semantic Model Query)**:")
            lines.append("```json")
            lines.append(json.dumps(test_case_data["smq"], indent=2, ensure_ascii=False))
            lines.append("```\n")
        
        if test_case_data.get("sql"):
            lines.append("**생성된 SQL 쿼리**:")
            lines.append("```sql")
            lines.append(test_case_data["sql"])
            lines.append("```\n")
        
        if test_case_data.get("metadata"):
            lines.append("**메타데이터**:")
            lines.append("```json")
            lines.append(json.dumps(test_case_data["metadata"], indent=2, ensure_ascii=False))
            lines.append("```\n")
        
        if test_case_data.get("validation"):
            validation = test_case_data["validation"]
            lines.append("**검증 결과**:")
            lines.append(f"- **검증 통과**: {'✅ 통과' if validation.get('is_valid') else '❌ 실패'}")
            lines.append(f"- **점수**: {validation.get('score', 0)}/100")
            lines.append(f"- **평가 이유**: {validation.get('reason', '')}")
            if validation.get("issues"):
                lines.append(f"- **발견된 문제점**:")
                for issue in validation["issues"]:
                    lines.append(f"  - {issue}")
            lines.append("")
        
        return "\n".join(lines)

    async def _generate_smq_and_convert(self, yml_file_content: str, user_query: str, manifest_path: str = None):
        """
        YML 파일 내용과 사용자 질의를 기반으로 SMQ를 생성하고 SQL로 변환합니다.
        """
        try:
            # LLM을 사용하여 SMQ 생성
            smq_generation_prompt = f"""다음 YML 파일 내용과 사용자 질의를 기반으로 SMQ(Semantic Model Query)를 생성하세요.

YML 파일 내용:
{yml_file_content}

사용자 질의:
{user_query}

**YML 파일 구조 이해**:
YML 파일은 다음과 같은 구조를 가집니다:
- `semantic_models`: 시멘틱 모델 정의 배열
  - `name`: 시멘틱 모델 이름 (예: "부점코드", "일수신계좌실적내역")
  - `entities`: 엔티티 정의 배열 (메트릭으로 사용하지 마세요!)
  - `dimensions`: 차원 정의 배열
  - `measures`: 측정값 정의 배열 (메트릭으로 사용하지 마세요!)
- `metrics`: 메트릭 정의 배열 (여기서만 메트릭을 가져오세요!)

**중요한 규칙**:
1. **메트릭 이름**: 반드시 YML 파일의 최상위 `metrics` 섹션에 정의된 각 메트릭의 `name` 필드 값만 사용하세요.
   - 집계 함수(SUM, COUNT, COUNT_DISTINCT 등)를 메트릭 이름 앞에 추가하지 마세요.
   - 예: YML에 `metrics: - name: total_brn_cnt`가 정의되어 있다면, `"metrics": ["total_brn_cnt"]`로 사용하세요.
   - 잘못된 예들:
     - `"metrics": ["SUM(total_brn_cnt)"]` ❌ (집계 함수 추가)
     - `"metrics": ["부점코드"]` ❌ (semantic model 이름 사용)
     - `"metrics": ["wapco_brncd_cn"]` ❌ (entity 이름 사용)
     - `"metrics": ["brn_cnt"]` ❌ (measure 이름 사용)
   - **절대로 다음을 메트릭으로 사용하지 마세요:**
     - semantic_models의 `name` 필드 값
     - entities의 `name` 필드 값
     - measures의 `name` 필드 값
     - dimensions의 `name` 필드 값
   - **오직 `metrics` 섹션의 `name` 필드 값만 메트릭으로 사용하세요!**

2. **차원 이름**: YML 파일의 `semantic_models` 내 `dimensions` 섹션에 정의된 각 차원의 `name` 필드 값을 사용하세요.
   - 차원 이름은 `semantic_model_name__dimension_name` 형식이어야 합니다.
   - 예: semantic_model name이 "부점코드"이고 dimension name이 "brn_stcd"인 경우 → `"groupBy": ["부점코드__brn_stcd"]` ✅
   - 잘못된 예: `"groupBy": ["brn_stcd"]` ❌ (semantic model 이름 접두어 없음)

3. **YML 파일 파싱 단계**:
   - 1단계: 최상위 `metrics` 배열을 찾으세요.
   - 2단계: `metrics` 배열의 각 항목에서 `name` 필드 값을 추출하세요 → 이것이 메트릭 이름입니다.
   - 3단계: `semantic_models` 배열을 순회하세요.
   - 4단계: 각 semantic_model의 `dimensions` 배열에서 `name` 필드 값을 추출하세요.
   - 5단계: 차원 이름을 사용할 때는 `{{semantic_model_name}}__{{dimension_name}}` 형식으로 조합하세요.

SMQ는 다음 형식의 JSON이어야 합니다:
{{
  "metrics": ["metric_name_from_metrics_section_only"],
  "groupBy": ["semantic_model_name__dimension_name"],
  "filters": [],
  "orderBy": [],
  "limit": 100,
  "joins": []
}}

**최종 확인 사항**:
- 메트릭은 반드시 최상위 `metrics` 섹션에서만 가져왔는가?
- semantic model 이름, entity 이름, measure 이름을 메트릭으로 사용하지 않았는가?
- 차원 이름에 semantic model 이름 접두어를 붙였는가?
- 집계 함수를 메트릭 이름에 추가하지 않았는가?

**반드시 YML 파일의 `metrics` 섹션에 정의된 메트릭의 `name` 필드 값만 사용하세요. 다른 섹션의 이름을 메트릭으로 사용하면 오류가 발생합니다.** JSON 형식으로만 응답하세요."""

            try:
                response = await self.client.chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {"role": "system", "content": "You are a Semantic Model Query generator. Generate SMQ in JSON format only."},
                        {"role": "user", "content": smq_generation_prompt}
                    ],
                    response_format={"type": "json_object"}
                )
            except asyncio.CancelledError:
                return {"error": "작업이 취소되었습니다."}
            
            smq_json = response.choices[0].message.content
            if not smq_json:
                return {"error": "Failed to generate SMQ from LLM"}
            
            # SMQ를 SQL로 변환
            result = convert_smq_to_sql(smq_json, manifest_path, dialect="bigquery")
            
            if result.get("success"):
                return {
                    "success": True,
                    "smq": json.loads(smq_json),
                    "sql": result.get("sql", ""),
                    "metadata": result.get("metadata", []),
                    "all_queries": result.get("all_queries", [])
                }
            else:
                return {
                    "error": result.get("error", "Failed to convert SMQ to SQL"),
                    "smq": json.loads(smq_json) if smq_json else None
                }
        
        except Exception as e:
            import traceback
            return {
                "error": str(e),
                "traceback": traceback.format_exc()
            }

    async def _validate_query(self, sql_query: str, user_query: str):
        """
        SQL 쿼리와 사용자 질의를 비교하여 검증합니다.
        """
        try:
            # 메타 요청인지 확인
            meta_requests = ["테스트 케이스", "테스트 케이스 생성", "테스트 케이스 출력", "테스트 케이스 하나"]
            is_meta_request = any(meta in user_query for meta in meta_requests)
            
            if is_meta_request:
                validation_prompt = f"""다음 SQL 쿼리가 의미 있고 올바른 테스트 케이스인지 평가하세요.

생성된 SQL 쿼리:
{sql_query}

**중요: 생성된 SQL 쿼리를 정확히 읽고 평가하세요. GROUP BY 절에 실제로 집계 함수가 있는지 확인하세요.**

사용자가 "테스트 케이스 생성"을 요청했으므로, 다음 기준으로 평가하세요:
1. **SQL 문법 오류**: 쿼리가 문법적으로 올바른가? 
   - **GROUP BY 절 확인**: GROUP BY 절에 실제로 집계 함수(COUNT, SUM, COUNT_DISTINCT 등)가 직접 사용되었는지 확인하세요.
   - SELECT 절에 집계 함수가 있고 GROUP BY 절에 컬럼만 있는 것은 정상입니다.
   - 예: `SELECT COUNT_DISTINCT(col) FROM table GROUP BY region` - 이것은 정상입니다 (GROUP BY에 region만 있음)
   - 예: `SELECT col FROM table GROUP BY COUNT_DISTINCT(col)` - 이것은 문법 오류입니다 (GROUP BY에 집계 함수가 있음)
2. **실행 가능성**: 쿼리가 실행 가능하고 결과를 반환할 수 있는가?
3. **의미 있는 결과**: 의미 있는 메트릭과 차원이 포함되어 있는가?

**중요한 평가 원칙**:
- **SQL 문법 오류가 있으면 반드시 실패로 평가하세요**:
  - **GROUP BY 절을 정확히 확인하세요**: GROUP BY 절에 실제로 집계 함수(COUNT, SUM, COUNT_DISTINCT 등)가 직접 사용되었는지 확인하세요.
  - SELECT 절에 집계 함수가 있고 GROUP BY 절에 컬럼만 있는 것은 완전히 정상입니다.
  - GROUP BY 절에 집계 함수가 직접 사용된 경우만 문법 오류입니다.
  - 예시: `GROUP BY region` - 정상 (컬럼만 있음)
  - 예시: `GROUP BY COUNT_DISTINCT(col)` - 문법 오류 (집계 함수가 GROUP BY에 있음)
- 쿼리가 사용자 의도를 반영하고 실행 가능하면 통과로 평가하세요
- 단순히 "복잡도가 낮다"거나 "더 간단하게 할 수 있다"는 이유만으로 실패로 평가하지 마세요

**절대 문제로 지적하지 말아야 할 것들 (이것들은 정상적인 SQL 패턴입니다)**:
- **COUNT_DISTINCT 사용**: COUNT_DISTINCT는 완전히 유효한 SQL 집계 함수입니다. COUNT_DISTINCT를 COUNT로 바꾸라고 제안하거나 문제로 지적하지 마세요. 데이터 모델에 따라 COUNT_DISTINCT가 올바른 선택일 수 있습니다.
- **WITH 절 사용**: WITH 절(CTE)은 정상적인 SQL 패턴입니다. "불필요한 WITH 구문"이나 "간단하게 할 수 있다"는 제안을 문제점으로 지적하지 마세요.
- **서브쿼리 사용**: 서브쿼리도 정상적인 SQL 패턴입니다.

**평가 기준**:
- COUNT vs COUNT_DISTINCT는 데이터 모델과 요구사항에 따라 둘 다 올바를 수 있습니다. 문법 오류가 없으면 통과로 평가하세요.
- "COUNT_DISTINCT 대신 COUNT를 사용해야 한다"는 제안은 절대 issues에 포함하지 마세요.
- "COUNT_DISTINCT 사용의 부적절성"이나 "불필요한 WITH 구문" 같은 내용은 문제점이 아니라 단순 제안일 뿐이므로 issues에 포함하지 마세요.
- 실제 SQL 문법 오류(GROUP BY에 집계 함수 사용 등)만 문제로 지적하세요.

응답은 다음 JSON 형식으로 반환하세요:
{{
  "is_valid": true/false,
  "score": 0-100,
  "reason": "평가 이유",
  "issues": ["문제점1", "문제점2"]
}}

is_valid가 true이고 score가 80 이상이면 검증 성공으로 간주합니다."""
            else:
                validation_prompt = f"""다음 SQL 쿼리가 사용자 질의의 의도를 정확히 반영하는지 평가하세요.

사용자 질의:
{user_query}

생성된 SQL 쿼리:
{sql_query}

**중요: 생성된 SQL 쿼리를 정확히 읽고 평가하세요. GROUP BY 절에 실제로 집계 함수가 있는지 확인하세요.**

다음 기준으로 평가하세요:
1. **SQL 문법 오류**: 쿼리가 문법적으로 올바른가?
   - **GROUP BY 절 확인**: GROUP BY 절에 실제로 집계 함수(COUNT, SUM, COUNT_DISTINCT 등)가 직접 사용되었는지 확인하세요.
   - SELECT 절에 집계 함수가 있고 GROUP BY 절에 컬럼만 있는 것은 정상입니다.
   - 예: `SELECT COUNT_DISTINCT(col) FROM table GROUP BY region` - 이것은 정상입니다 (GROUP BY에 region만 있음)
   - 예: `SELECT col FROM table GROUP BY COUNT_DISTINCT(col)` - 이것은 문법 오류입니다 (GROUP BY에 집계 함수가 있음)
2. **의도 반영**: SQL 쿼리가 사용자 질의의 의도를 정확히 반영하는가?
3. **필수 요소**: 필요한 메트릭과 차원이 모두 포함되었는가?
4. **필터 조건**: 필터 조건이 올바르게 적용되었는가?

**중요한 평가 원칙**:
- **SQL 문법 오류가 있으면 반드시 실패로 평가하세요**:
  - **GROUP BY 절을 정확히 확인하세요**: GROUP BY 절에 실제로 집계 함수(COUNT, SUM, COUNT_DISTINCT 등)가 직접 사용되었는지 확인하세요.
  - SELECT 절에 집계 함수가 있고 GROUP BY 절에 컬럼만 있는 것은 완전히 정상입니다.
  - GROUP BY 절에 집계 함수가 직접 사용된 경우만 문법 오류입니다.
  - 예시: `GROUP BY region` - 정상 (컬럼만 있음)
  - 예시: `GROUP BY COUNT_DISTINCT(col)` - 문법 오류 (집계 함수가 GROUP BY에 있음)
- 쿼리가 사용자 의도를 반영하고 실행 가능하면 통과로 평가하세요
- "더 간단하게 할 수 있다"는 이유만으로 실패로 평가하지 마세요

**절대 문제로 지적하지 말아야 할 것들 (이것들은 정상적인 SQL 패턴입니다)**:
- **COUNT_DISTINCT 사용**: COUNT_DISTINCT는 완전히 유효한 SQL 집계 함수입니다. COUNT_DISTINCT를 COUNT로 바꾸라고 제안하거나 문제로 지적하지 마세요. 데이터 모델에 따라 COUNT_DISTINCT가 올바른 선택일 수 있습니다.
- **WITH 절 사용**: WITH 절(CTE)은 정상적인 SQL 패턴입니다. "불필요한 WITH 구문"이나 "간단하게 할 수 있다"는 제안을 문제점으로 지적하지 마세요.
- **서브쿼리 사용**: 서브쿼리도 정상적인 SQL 패턴입니다.

**평가 기준**:
- COUNT vs COUNT_DISTINCT는 데이터 모델과 요구사항에 따라 둘 다 올바를 수 있습니다. 문법 오류가 없으면 통과로 평가하세요.
- "COUNT_DISTINCT 대신 COUNT를 사용해야 한다"는 제안은 절대 issues에 포함하지 마세요.
- "COUNT_DISTINCT 사용의 부적절성"이나 "불필요한 WITH 구문" 같은 내용은 문제점이 아니라 단순 제안일 뿐이므로 issues에 포함하지 마세요.
- 실제 SQL 문법 오류(GROUP BY에 집계 함수 사용 등)만 문제로 지적하세요.

응답은 다음 JSON 형식으로 반환하세요:
{{
  "is_valid": true/false,
  "score": 0-100,
  "reason": "평가 이유",
  "issues": ["문제점1", "문제점2"]
}}

is_valid가 true이고 score가 80 이상이면 검증 성공으로 간주합니다."""

            try:
                response = await self.client.chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {"role": "system", "content": "You are a SQL query validator. Evaluate if the SQL query accurately reflects the user's intent."},
                        {"role": "user", "content": validation_prompt}
                    ],
                    response_format={"type": "json_object"}
                )
            except asyncio.CancelledError:
                return {"error": "작업이 취소되었습니다.", "is_valid": False}
            
            result = json.loads(response.choices[0].message.content)
            return result
        
        except Exception as e:
            import traceback
            return {
                "is_valid": False,
                "score": 0,
                "reason": f"Validation error: {str(e)}",
                "issues": [f"Validation failed: {str(e)}"],
                "error": str(e),
                "traceback": traceback.format_exc()
            }
