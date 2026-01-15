"""
LangGraph 기반 에이전트
상태 머신을 사용하여 에이전트 워크플로우를 관리합니다.

노드 구성:
1. classifyJoy - 질문 분류
2. splitQuestion - 질문 분할
3. modelSelector - 모델 선택
4. extractMetrics - 메트릭 추출
5. dateFilter - 날짜 필터
6. otherFilter - 기타 필터
7. manipulation - 조작/변환
8. smq2sql - SMQ를 SQL로 변환
9. respondent - 응답 생성
"""
import os
import json
import uuid
import re
import logging
from datetime import datetime
from typing import TypedDict, Annotated, Sequence, Optional
from typing_extensions import Literal

from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv

# tools.py에서 도구 함수들 import
from tools import read_file, convert_smq_to_sql
from backend.utils.logger import setup_logger

load_dotenv()

# 로거 설정
logger = setup_logger("langgraph_agent")


class AgentState(TypedDict):
    """에이전트 상태 정의"""
    messages: Annotated[Sequence[BaseMessage], add_messages]
    user_query: str
    current_step: str
    classified_intent: Optional[str]
    split_questions: Optional[list]
    selected_models: Optional[list]
    extracted_metrics: Optional[list]
    extracted_dimensions: Optional[list]
    extracted_filters: Optional[list]
    extracted_order_by: Optional[list]
    extracted_limit: Optional[int]
    date_filters: Optional[dict]
    other_filters: Optional[dict]
    smq: Optional[dict]
    sql_query: Optional[str]
    sql_result: Optional[dict]
    query_result: Optional[dict]
    final_response: Optional[str]
    error: Optional[str]
    processed_prompt: Optional[str]  # 변수 치환된 프롬프트 저장


class LangGraphAgent:
    """LangGraph를 사용한 에이전트"""
    
    def __init__(self, model_name: str = "gpt-4o", temperature: float = 0.0):
        """
        Args:
            model_name: 사용할 LLM 모델명
            temperature: LLM temperature 설정
        """
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY가 설정되지 않았습니다.")
        
        self.llm = ChatOpenAI(model=model_name, temperature=temperature, api_key=api_key)
        self.playground_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'playground'))
        
        # LangGraph 워크플로우 생성
        self.app = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """LangGraph 워크플로우 생성"""
        workflow = StateGraph(AgentState)
        
        # 노드 추가
        workflow.add_node("classifyJoy", self._classify_joy_node)
        workflow.add_node("killjoy", self._killjoy_node)
        workflow.add_node("splitQuestion", self._split_question_node)
        workflow.add_node("modelSelector", self._model_selector_node)
        workflow.add_node("extractMetrics", self._extract_metrics_node)
        workflow.add_node("extractFilters", self._extract_filters_node)
        workflow.add_node("extractOrderByAndLimit", self._extract_order_by_and_limit_node)
        workflow.add_node("manipulation", self._manipulation_node)
        workflow.add_node("smq2sql", self._smq2sql_node)
        workflow.add_node("executeQuery", self._execute_query_node)
        workflow.add_node("respondent", self._respondent_node)
        
        # 엣지 추가
        workflow.set_entry_point("classifyJoy")
        
        workflow.add_conditional_edges(
            "classifyJoy",
            self._should_continue_after_classify,
            {
                "continue": "splitQuestion",
                "killjoy": "killjoy"
            }
        )
        workflow.add_edge("killjoy", END)
        workflow.add_edge("splitQuestion", "modelSelector")
        workflow.add_edge("modelSelector", "extractMetrics")
        workflow.add_edge("extractMetrics", "extractFilters")
        workflow.add_edge("extractFilters", "extractOrderByAndLimit")
        workflow.add_edge("extractOrderByAndLimit", "manipulation")
        workflow.add_edge("manipulation", "smq2sql")
        workflow.add_edge("smq2sql", "executeQuery")
        workflow.add_edge("executeQuery", "respondent")
        workflow.add_edge("respondent", END)
        
        return workflow.compile()
    
    def _should_continue_after_classify(self, state: AgentState) -> Literal["continue", "killjoy"]:
        """classifyJoy 이후 계속 진행할지 결정"""
        intent = state.get("classified_intent", "")
        # YES면 계속, NO면 killjoy 노드로
        if intent == "YES":
            return "continue"
        return "killjoy"
    
    def _get_all_semantic_models_info(self) -> str:
        """semantic_manifest.json에서 모든 semantic_models 정보 추출"""
        manifest_path = os.path.join(self.playground_dir, "semantic_manifest.json")
        if not os.path.exists(manifest_path):
            return ""
        
        try:
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = json.load(f)
        except Exception as e:
            return ""
        
        semantic_models = manifest.get("semantic_models", [])
        entities_info = []
        
        for model in semantic_models:
            model_name = model.get("name", "")
            model_description = model.get("description", "")
            entities_info.append(f"- {model_name}: {model_description}")
        
        return "\n".join(entities_info)
    
    def _get_semantic_manifest_info(self, entities: list) -> dict:
        """semantic_manifest.json에서 entities, metrics, dimensions 정보 추출"""
        manifest_path = os.path.join(self.playground_dir, "semantic_manifest.json")
        
        try:
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = json.load(f)
        except Exception as e:
            return {
                "entities": [],
                "metrics": {},
                "dimensions": {}
            }
        
        semantic_models = manifest.get("semantic_models", [])
        all_metrics = manifest.get("metrics", [])
        
        # entities에 해당하는 semantic models 찾기 (이름으로 매칭)
        matched_models = []
        for entity in entities:
            # entity 이름으로 semantic model 찾기 (대소문자 무시)
            for model in semantic_models:
                model_name = model.get("name", "").lower()
                if entity.lower() in model_name or model_name in entity.lower():
                    matched_models.append(model)
                    break
        
        # 매칭된 모델이 없으면 모든 모델 사용
        if not matched_models:
            matched_models = semantic_models
        
        # entities 정보 수집
        entities_info = []
        for model in matched_models:
            model_name = model.get("name", "")
            entities_info.append(f"- {model_name}: {model.get('description', '')}")
        
        # entity별 metrics 정보 수집
        # metric의 expr에서 참조하는 semantic model을 찾아서 해당 모델에 할당
        entity_metrics = {}
        matched_model_names = {model.get("name", "").lower() for model in matched_models}
        
        for model in matched_models:
            model_name = model.get("name", "")
            model_metrics = []
            
            # metrics에서 해당 모델을 참조하는 metric 찾기
            for metric in all_metrics:
                metric_name = metric.get("name", "")
                metric_description = metric.get("description", "")
                metric_expr = metric.get("expr", "")
                
                # metric의 expr에서 semantic model 참조 확인
                # expr에 "model_name__" 패턴이 있으면 해당 모델을 참조하는 것으로 간주
                if metric_expr:
                    # "model_name__" 패턴 찾기
                    pattern = r'(\w+)__'
                    referenced_models = set(re.findall(pattern, metric_expr))
                    referenced_models_lower = {m.lower() for m in referenced_models}
                    
                    # 참조하는 모델 중 하나라도 matched_models에 있으면 추가
                    if referenced_models_lower & matched_model_names:
                        # description이 있으면 포함, 없으면 name만 사용
                        if metric_description:
                            model_metrics.append(f"  - {metric_name}: {metric_description}")
                        else:
                            model_metrics.append(f"  - {metric_name}")
            
            entity_metrics[model_name] = model_metrics
        
        # dimensions 수집 (타입 정보 포함)
        dimensions_info = []
        for model in matched_models:
            model_name = model.get("name", "")
            dimensions = model.get("dimensions", [])
            
            for dim in dimensions:
                dim_name = dim.get("name", "")
                dim_description = dim.get("description", "")
                dim_type = dim.get("type", "varchar")
                # 날짜 타입인 경우 명시적으로 표시
                type_marker = " [날짜]" if dim_type == "date" else ""
                # description이 있으면 포함, 없으면 name만 사용
                if dim_description:
                    dimensions_info.append(f"- {model_name}__{dim_name}: {dim_description}{type_marker}")
                else:
                    dimensions_info.append(f"- {model_name}__{dim_name}{type_marker}")
        
        return {
            "entities": "\n".join(entities_info) if entities_info else "없음",
            "metrics": "\n".join([f"{k}:\n" + "\n".join(v) for k, v in entity_metrics.items()]) if entity_metrics else "없음",
            "dimensions": "\n".join(dimensions_info) if dimensions_info else "없음"
        }
    
    def _load_classify_joy_prompt(self) -> str:
        """classifyJoy 프롬프트 파일 로드"""
        prompt_file = os.path.join(os.path.dirname(__file__), 'prompts', 'classify_joy_prompt.txt')
        if os.path.exists(prompt_file):
            with open(prompt_file, 'r', encoding='utf-8') as f:
                return f.read()
        return ""
    
    def _load_killjoy_prompt(self) -> str:
        """killjoy 프롬프트 파일 로드"""
        prompt_file = os.path.join(os.path.dirname(__file__), 'prompts', 'killjoy_prompt.txt')
        if os.path.exists(prompt_file):
            with open(prompt_file, 'r', encoding='utf-8') as f:
                return f.read()
        return ""
    
    def _load_split_question_prompt(self) -> str:
        """splitQuestion 프롬프트 파일 로드"""
        prompt_file = os.path.join(os.path.dirname(__file__), 'prompts', 'split_question_prompt.txt')
        if os.path.exists(prompt_file):
            with open(prompt_file, 'r', encoding='utf-8') as f:
                return f.read()
        return ""
    
    def _load_entity_selector_prompt(self) -> str:
        """entitySelector 프롬프트 파일 로드"""
        prompt_file = os.path.join(os.path.dirname(__file__), 'prompts', 'entity_selector_prompt.txt')
        if os.path.exists(prompt_file):
            with open(prompt_file, 'r', encoding='utf-8') as f:
                return f.read()
        return ""
    
    def _load_extract_metrics_prompt(self) -> str:
        """extractMetrics 프롬프트 파일 로드"""
        prompt_file = os.path.join(os.path.dirname(__file__), 'prompts', 'extract_metrics_prompt.txt')
        if os.path.exists(prompt_file):
            with open(prompt_file, 'r', encoding='utf-8') as f:
                return f.read()
        return ""
    
    def _load_extract_filters_prompt(self) -> str:
        """extractFilters 프롬프트 파일 로드"""
        prompt_file = os.path.join(os.path.dirname(__file__), 'prompts', 'extract_filters_prompt.txt')
        if os.path.exists(prompt_file):
            with open(prompt_file, 'r', encoding='utf-8') as f:
                return f.read()
        return ""
    
    def _load_extract_order_by_and_limit_prompt(self) -> str:
        """extractOrderByAndLimit 프롬프트 파일 로드"""
        prompt_file = os.path.join(os.path.dirname(__file__), 'prompts', 'extract_order_by_and_limit_prompt.txt')
        if os.path.exists(prompt_file):
            with open(prompt_file, 'r', encoding='utf-8') as f:
                return f.read()
        return ""
    
    def _classify_joy_node(self, state: AgentState) -> AgentState:
        """질문 분류 노드 - YES/NO 판단"""
        user_query = state.get("user_query", "")
        previous_context = state.get("messages", [])
        
        # 이전 대화 맥락 추출 (최근 3개 메시지)
        context_messages = []
        for msg in previous_context[-3:]:
            if isinstance(msg, HumanMessage):
                context_messages.append(f"human: {msg.content}")
            elif isinstance(msg, AIMessage):
                context_messages.append(f"ai: {msg.content}")
        
        previous_context_str = "\n".join(context_messages) if context_messages else ""
        
        # 프롬프트 파일 로드
        prompt_template = self._load_classify_joy_prompt()
        if not prompt_template:
            # 프롬프트 파일이 없으면 기본 프롬프트 사용
            prompt = f"""당신은 대화 흐름의 타당성을 평가하는 분석 시스템입니다.

전체 주제: 과제 목록(과제리스트), 과제·계좌 분석, 연구과제 수주·집행 현황 및 연구비·계좌 분석
제한 주제: 감정, 생각, 의견, 주관적 평가, 상담
이전 대화 맥락:
{previous_context_str}

사용자의 새로운 질문:
{user_query}

YES 또는 NO로만 응답하세요."""
        else:
            # 프롬프트 템플릿의 플레이스홀더 치환
            prompt = prompt_template.replace("{user_question}", user_query)
            prompt = prompt.replace("{user_query}", user_query)  # 하위 호환성
            prompt = prompt.replace("{history}", previous_context_str if previous_context_str else "")
            if previous_context_str and "- **이전 대화 맥락:**" in prompt:
                # 이전 대화 맥락이 있으면 추가
                prompt = prompt.replace("- **이전 대화 맥락:**", f"- **이전 대화 맥락:**\n{previous_context_str}")
        
        messages = [HumanMessage(content=prompt)]
        response = self.llm.invoke(messages)
        
        # 응답에서 YES/NO만 추출 (대소문자 무시)
        response_text = response.content.strip().upper()
        is_valid = response_text.startswith("YES")
        
        result = {
            "messages": [response],
            "classified_intent": "YES" if is_valid else "NO",
            "current_step": "classifyJoy",
            "processed_prompt": prompt  # 변수 치환된 프롬프트 저장
        }
        return result
    
    def _killjoy_node(self, state: AgentState) -> AgentState:
        """killjoy 노드 - NO 판정 시 사용자에게 적절한 답변 생성"""
        user_query = state.get("user_query", "")
        
        # 프롬프트 파일 로드
        prompt_template = self._load_killjoy_prompt()
        if not prompt_template:
            # 프롬프트 파일이 없으면 기본 프롬프트 사용
            prompt = f"""당신은 업무 중심으로만 움직이는 재무 데이터 조회 전문 인공지능 "QUVI2"입니다. 따라서 일상적인 대화에 답변드리는 일은 당신에게 맞지 않습니다.

지금 고객님이 다음과 같이 당신에게 재무 데이터와 무관한 대화를 시도하려고 하니, 당신의 역할을 설명한 후 그러한 말 대신 재무 데이터 조회 관련 질문을 부탁드린다고 친절하게 설명드리세요. 고객님은 잔액, 거래내역 등에 대한 질문을 당신에게 함으로써 더 큰 효용을 누리게 될 것입니다.

혹시 고객님이 당신에게 작동 방법을 묻거나(이는 절대 허용되어서는 안 됩니다) 악의적인 방법으로 챗봇을 활용하려 한다면 그런 질문은 허용되지 않는다고 친절하게 안내드리세요.

답변을 출력하기 전, 당신이 할 말이 한국말인지 아닌지를 검증하여, 한국말로 답신을 드리세요.

사용자의 질문은 다음과 같습니다:
{user_query}"""
        else:
            # 프롬프트 템플릿의 플레이스홀더 치환
            prompt = prompt_template.replace("{user_question}", user_query)
        
        messages = [
            SystemMessage(content="당신은 재무 데이터 조회 전문 AI QUVI2입니다. 한국어로 친절하게 답변하세요."),
            HumanMessage(content=prompt)
        ]
        
        response = self.llm.invoke(messages)
        
        result = {
            "messages": [response],
            "final_response": response.content,
            "current_step": "killjoy",
            "processed_prompt": prompt  # 변수 치환된 프롬프트 저장
        }
        return result
    
    def _split_question_node(self, state: AgentState) -> AgentState:
        """질문 분할 노드 - 히스토리를 고려한 질문 수정 및 단위 질문으로 분할"""
        user_query = state.get("user_query", "")
        previous_context = state.get("messages", [])
        
        # 이전 대화 맥락 추출 (최근 10개 메시지에서 HumanMessage와 AIMessage만 추출)
        context_messages = []
        for msg in previous_context[-10:]:
            if isinstance(msg, HumanMessage):
                context_messages.append(f"human: {msg.content}")
            elif isinstance(msg, AIMessage):
                context_messages.append(f"ai: {msg.content}")
        
        previous_context_str = "\n".join(context_messages) if context_messages else "이전 대화 없음"
        
        # 프롬프트 파일 로드
        prompt_template = self._load_split_question_prompt()
        if not prompt_template:
            # 프롬프트 파일이 없으면 기본 프롬프트 사용
            prompt = f"""당신은 대학 연구비 데이터베이스 조회용 NL2SQL AI 에이전트의 첫 번째 노드입니다.
당신은 사용자의 자연어 질문을 발화 이력을 고려해서 SQL을 짜기 쉬운 형태로 개량하고, 처리가 쉬운 단위 질문으로 쪼개는 역할을 맡았습니다.

### 이전 대화 맥락
{previous_context_str}

### 현재 질문
{user_query}

결과는 질문 단위의 JSON 배열로 출력합니다. 설명 없이 배열만 출력하세요.
[Question1, Question2, ...]"""
        else:
            # 프롬프트 템플릿의 플레이스홀더 치환
            prompt = prompt_template.replace("{previous_context}", previous_context_str)
            prompt = prompt.replace("{current_query}", user_query)
        
        messages = [
            SystemMessage(content="당신은 자연어 질문을 분석하여 SQL을 짜기 쉬운 형태로 개량하고 단위 질문으로 분할하는 전문가입니다. 결과는 JSON 배열 형식으로만 출력하세요."),
            HumanMessage(content=prompt)
        ]
        
        response = self.llm.invoke(messages)
        
        try:
            # LLM 응답에서 JSON 배열 추출 시도
            response_text = response.content.strip()
            
            # JSON 배열로 감싸져 있지 않으면 배열로 감싸기
            if not response_text.startswith('['):
                # JSON 형식으로 감싸져 있는지 확인 (예: {"questions": [...]})
                if response_text.startswith('{'):
                    result = json.loads(response_text)
                    questions = result.get("questions", [user_query])
                else:
                    # 단순 텍스트인 경우 배열로 감싸기
                    questions = [user_query]
            else:
                # JSON 배열인 경우
                questions = json.loads(response_text)
            
            # 배열이 비어있거나 유효하지 않은 경우 원본 질문 사용
            if not questions or not isinstance(questions, list):
                questions = [user_query]
            
            result = {
                "messages": [response],
                "split_questions": questions,
                "current_step": "splitQuestion",
                "processed_prompt": prompt  # 변수 치환된 프롬프트 저장
            }
            return result
        except json.JSONDecodeError:
            # JSON 파싱 실패 시 원본 질문 반환
            result = {
                "messages": [response],
                "split_questions": [user_query],
                "current_step": "splitQuestion",
                "processed_prompt": prompt  # 변수 치환된 프롬프트 저장
            }
            return result
    
    def _model_selector_node(self, state: AgentState) -> AgentState:
        """모델 선택 노드 - entity 추출"""
        user_query = state.get("user_query", "")
        
        # 모든 semantic_models 정보 가져오기
        entities_info = self._get_all_semantic_models_info()
        
        # 프롬프트 파일 로드
        prompt_template = self._load_entity_selector_prompt()
        if not prompt_template:
            # 프롬프트 파일이 없으면 기본 프롬프트 사용
            prompt = f"""사용자의 질문으로부터 관련된 entity를 선택하세요.

사용 가능한 entities:
{entities_info if entities_info else "- 사용 가능한 entity 정보를 불러올 수 없습니다."}

사용자 질문: {user_query}

다음 JSON 형식으로 응답하세요:
{{
    "entities": ["entity1", "entity2", ...]
}}"""
        else:
            # 프롬프트 템플릿의 플레이스홀더 치환
            prompt = prompt_template.replace("{entities}", entities_info if entities_info else "- 사용 가능한 entity 정보를 불러올 수 없습니다.")
            prompt = prompt.replace("{user_query}", user_query)
        
        messages = [
            SystemMessage(content="당신은 사용자의 질문으로부터 관련된 entity를 선택하는 전문가입니다. JSON 형식으로만 응답하세요."),
            HumanMessage(content=prompt)
        ]
        
        response = self.llm.invoke(messages)
        
        try:
            # LLM 응답에서 JSON 추출
            response_text = response.content.strip()
            
            # JSON 코드 블록이 있으면 제거
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            
            result = json.loads(response_text)
            entities = result.get("entities", [])
            
            # entities를 selected_models로 저장 (하위 호환성 유지)
            output = {
                "messages": [response],
                "selected_models": entities,
                "current_step": "modelSelector",
                "processed_prompt": prompt  # 변수 치환된 프롬프트 저장
            }
            return output
        except json.JSONDecodeError as e:
            # JSON 파싱 실패 시 빈 리스트 반환
            output = {
                "messages": [response],
                "selected_models": [],
                "current_step": "modelSelector"
            }
            return output
        except Exception as e:
            # 기타 예외 발생 시 빈 리스트 반환
            output = {
                "messages": [response],
                "selected_models": [],
                "current_step": "modelSelector"
            }
            return output
    
    def _extract_metrics_node(self, state: AgentState) -> AgentState:
        """메트릭 추출 노드 - metrics와 group_by 추출"""
        user_query = state.get("user_query", "")
        selected_models = state.get("selected_models", [])  # entities 리스트
        
        # semantic_manifest.json에서 정보 추출
        manifest_info = self._get_semantic_manifest_info(selected_models)
        
        # 프롬프트 파일 로드
        prompt_template = self._load_extract_metrics_prompt()
        if not prompt_template:
            # 프롬프트 파일이 없으면 기본 프롬프트 사용
            prompt = f"""사용자의 질문으로부터 데이터베이스 조회에 필요한 metrics와 group_by 항목을 추출하세요.

활용할 수 있는 entities:
{manifest_info['entities']}

entity별 활용할 수 있는 metrics:
{manifest_info['metrics']}

활용할 수 있는 group_by dimensions:
{manifest_info['dimensions']}

사용자 질문: {user_query}

다음 JSON 형식으로 응답하세요:
{{
    "metrics": ["metric1", "metric2"],
    "group_by": ["entity_id__dimension1", "entity_id__dimension2"]
}}"""
        else:
            # 프롬프트 템플릿의 플레이스홀더 치환
            prompt = prompt_template.replace("{entities}", manifest_info['entities'])
            prompt = prompt.replace("{metrics}", manifest_info['metrics'])
            prompt = prompt.replace("{dimensions}", manifest_info['dimensions'])
            prompt = prompt.replace("{user_question}", user_query)
        
        messages = [
            SystemMessage(content="당신은 데이터 분석 질의어 생성 AI입니다. JSON 형식으로만 응답하세요. 반드시 'metrics'와 'group_by' 필드만 사용하고, 'dimensions' 필드는 절대 생성하지 마세요."),
            HumanMessage(content=prompt)
        ]
        
        response = self.llm.invoke(messages)
        
        try:
            # LLM 응답에서 JSON 추출
            response_text = response.content.strip()
            
            # JSON 코드 블록이 있으면 제거
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            
            result = json.loads(response_text)
            # dimensions 필드가 있으면 제거 (LLM이 실수로 생성한 경우)
            if "dimensions" in result:
                del result["dimensions"]
            metrics = result.get("metrics", [])
            group_by = result.get("group_by", [])
            
            output = {
                "messages": [response],
                "extracted_metrics": metrics,
                "extracted_dimensions": group_by,  # group_by를 dimensions로도 저장
                "current_step": "extractMetrics",
                "processed_prompt": prompt  # 변수 치환된 프롬프트 저장
            }
            return output
        except json.JSONDecodeError as e:
            # JSON 파싱 실패 시 빈 리스트 반환
            output = {
                "messages": [response],
                "extracted_metrics": [],
                "extracted_dimensions": [],
                "current_step": "extractMetrics"
            }
            return output
        except Exception as e:
            # 기타 예외 발생 시 빈 리스트 반환
            output = {
                "messages": [response],
                "extracted_metrics": [],
                "extracted_dimensions": [],
                "current_step": "extractMetrics"
            }
            return output
    
    def _extract_filters_node(self, state: AgentState) -> AgentState:
        """필터 추출 노드 - filters (WHERE 조건) 추출"""
        user_query = state.get("user_query", "")
        extracted_metrics = state.get("extracted_metrics", [])
        extracted_dimensions = state.get("extracted_dimensions", [])
        selected_models = state.get("selected_models", [])
        
        # 이전 단계 정보 (metrics와 group_by) 구성
        smq_info = {
            "metrics": extracted_metrics,
            "group_by": extracted_dimensions
        }
        
        # semantic_manifest.json에서 dimensions 정보 추출
        manifest_info = self._get_semantic_manifest_info(selected_models)
        
        # 프롬프트 파일 로드
        prompt_template = self._load_extract_filters_prompt()
        if not prompt_template:
            # 프롬프트 파일이 없으면 기본 프롬프트 사용
            prompt = f"""사용자의 질문으로부터 데이터베이스 조회에 필요한 filters (WHERE 조건)를 추출하세요.

이전 단계 정보:
{json.dumps(smq_info, ensure_ascii=False, indent=2)}

활용 가능한 dimensions:
{manifest_info['dimensions']}

사용자의 질문: {user_query}

다음 JSON 형식으로 응답하세요:
{{
    "filters": ["condition1", "condition2"]
}}"""
        else:
            # 오늘 날짜 계산 (YYYY-MM-DD 형식)
            today = datetime.now().strftime("%Y-%m-%d")
            
            # 프롬프트 템플릿의 플레이스홀더 치환
            prompt = prompt_template.replace("{smq}", json.dumps(smq_info, ensure_ascii=False, indent=2))
            prompt = prompt.replace("{dimensions}", manifest_info['dimensions'])
            prompt = prompt.replace("{today}", today)
            prompt = prompt.replace("{user_question}", user_query)
        
        messages = [
            SystemMessage(content="당신은 데이터 분석 질의어 생성 AI입니다. JSON 형식으로만 응답하세요."),
            HumanMessage(content=prompt)
        ]
        
        response = self.llm.invoke(messages)
        
        try:
            # LLM 응답에서 JSON 추출
            response_text = response.content.strip()
            
            # JSON 코드 블록이 있으면 제거
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            
            result = json.loads(response_text)
            filters = result.get("filters", [])
            
            output = {
                "messages": [response],
                "extracted_filters": filters,
                "current_step": "extractFilters",
                "processed_prompt": prompt  # 변수 치환된 프롬프트 저장
            }
            return output
        except json.JSONDecodeError as e:
            # JSON 파싱 실패 시 빈 리스트 반환
            output = {
                "messages": [response],
                "extracted_filters": [],
                "current_step": "extractFilters"
            }
            return output
        except Exception as e:
            # 기타 예외 발생 시 빈 리스트 반환
            output = {
                "messages": [response],
                "extracted_filters": [],
                "current_step": "extractFilters"
            }
            return output
    
    def _date_filter_node(self, state: AgentState) -> AgentState:
        """날짜 필터 노드 (deprecated - extractFilters로 대체됨)"""
        # 하위 호환성을 위해 유지하되, extractFilters를 호출
        return self._extract_filters_node(state)
    
    def _other_filter_node(self, state: AgentState) -> AgentState:
        """기타 필터 노드 (deprecated - extractFilters로 대체됨)"""
        # 하위 호환성을 위해 유지하되, extractFilters를 호출
        return self._extract_filters_node(state)
    
    def _extract_order_by_and_limit_node(self, state: AgentState) -> AgentState:
        """order_by와 limit 추출 노드"""
        user_query = state.get("user_query", "")
        extracted_metrics = state.get("extracted_metrics", [])
        extracted_dimensions = state.get("extracted_dimensions", [])
        extracted_filters = state.get("extracted_filters", [])
        
        # 현재 작업 중인 SMQ 구성 (metrics, group_by, filters)
        current_smq = {
            "metrics": extracted_metrics,
            "groupBy": extracted_dimensions,
            "filters": extracted_filters
        }
        
        # 프롬프트 파일 로드
        prompt_template = self._load_extract_order_by_and_limit_prompt()
        if not prompt_template:
            # 프롬프트 파일이 없으면 기본 프롬프트 사용
            prompt = f"""사용자의 질문으로부터 데이터베이스 조회를 위한 order_by (정렬 기준)와 LIMIT (출력 개수 제한)을 추출하세요.

현재 작업 중인 smq:
{json.dumps(current_smq, ensure_ascii=False, indent=2)}

사용자의 질문: {user_query}

다음 JSON 형식으로 응답하세요:
{{
    "order_by": ["item1", "item2"],
    "limit": number | "None"
}}"""
        else:
            # 프롬프트 템플릿의 플레이스홀더 치환
            prompt = prompt_template.replace("{smq}", json.dumps(current_smq, ensure_ascii=False, indent=2))
            prompt = prompt.replace("{user_question}", user_query)
        
        messages = [
            SystemMessage(content="당신은 데이터 분석 질의어 생성 AI입니다. JSON 형식으로만 응답하세요."),
            HumanMessage(content=prompt)
        ]
        
        response = self.llm.invoke(messages)
        
        try:
            # LLM 응답에서 JSON 추출
            response_text = response.content.strip()
            
            # JSON 코드 블록이 있으면 제거
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            
            result = json.loads(response_text)
            order_by = result.get("order_by", [])
            limit = result.get("limit", "None")
            
            # limit이 "None" 문자열이면 None으로 변환, 숫자면 그대로 유지
            if limit == "None" or limit is None:
                limit_value = None
            elif isinstance(limit, str) and limit.isdigit():
                limit_value = int(limit)
            elif isinstance(limit, (int, float)):
                limit_value = int(limit)
            else:
                limit_value = None
            
            output = {
                "messages": [response],
                "extracted_order_by": order_by,
                "extracted_limit": limit_value,
                "current_step": "extractOrderByAndLimit",
                "processed_prompt": prompt  # 변수 치환된 프롬프트 저장
            }
            return output
        except json.JSONDecodeError as e:
            # JSON 파싱 실패 시 기본값 반환
            output = {
                "messages": [response],
                "extracted_order_by": [],
                "extracted_limit": None,
                "current_step": "extractOrderByAndLimit"
            }
            return output
        except Exception as e:
            # 기타 예외 발생 시 기본값 반환
            output = {
                "messages": [response],
                "extracted_order_by": [],
                "extracted_limit": None,
                "current_step": "extractOrderByAndLimit"
            }
            return output
    
    def _manipulation_node(self, state: AgentState) -> AgentState:
        """조작/변환 노드 - SMQ 생성"""
        questions = state.get("split_questions", [])
        selected_models = state.get("selected_models", [])
        metrics = state.get("extracted_metrics", [])
        extracted_dimensions = state.get("extracted_dimensions", [])
        extracted_filters = state.get("extracted_filters", [])
        extracted_order_by = state.get("extracted_order_by", [])
        extracted_limit = state.get("extracted_limit", None)
        # 하위 호환성을 위해 기존 필터도 확인
        date_filters = state.get("date_filters", {})
        other_filters = state.get("other_filters", [])
        
        # extracted_filters가 있으면 우선 사용, 없으면 기존 필터 사용
        filters = extracted_filters if extracted_filters else ([date_filters] + other_filters if date_filters else other_filters)
        
        prompt = f"""추출된 정보를 바탕으로 SMQ (Semantic Model Query)를 생성하세요.

질문들: {json.dumps(questions, ensure_ascii=False)}
선택된 모델: {json.dumps(selected_models, ensure_ascii=False)}
메트릭: {json.dumps(metrics, ensure_ascii=False)}
그룹바이: {json.dumps(extracted_dimensions, ensure_ascii=False)}
필터: {json.dumps(filters, ensure_ascii=False)}
정렬: {json.dumps(extracted_order_by, ensure_ascii=False)}
제한: {extracted_limit if extracted_limit is not None else "None"}

다음 JSON 형식으로 SMQ를 생성하세요:
{{
    "model": "모델명",
    "metrics": ["메트릭1", "메트릭2"],
    "dimensions": ["차원1", "차원2"],
    "filters": [...],
    "order_by": [...],
    "limit": ...
}}"""
        
        messages = [
            SystemMessage(content="당신은 SMQ를 생성하는 전문가입니다."),
            HumanMessage(content=prompt)
        ]
        
        response = self.llm.invoke(messages)
        
        try:
            result = json.loads(response.content)
            # dimensions 필드가 있으면 제거하고 group_by로 변환
            if "dimensions" in result:
                if "group_by" not in result:
                    result["group_by"] = result["dimensions"]
                del result["dimensions"]
            # extracted_dimensions가 있으면 group_by로 설정
            if extracted_dimensions and "group_by" not in result:
                result["group_by"] = extracted_dimensions
            # extracted_order_by와 extracted_limit이 있으면 SMQ에 추가
            if extracted_order_by:
                result["order_by"] = extracted_order_by
            if extracted_limit is not None:
                result["limit"] = extracted_limit
            output = {
                "messages": [response],
                "smq": result,
                "current_step": "manipulation",
                "processed_prompt": prompt  # 변수 치환된 프롬프트 저장
            }
            return output
        except:
            # JSON 파싱 실패 시 기본 SMQ 구조 생성
            smq = {
                "model": selected_models[0] if selected_models else "",
                "metrics": metrics,
                "filters": filters
            }
            if extracted_dimensions:
                smq["group_by"] = extracted_dimensions
            if extracted_order_by:
                smq["order_by"] = extracted_order_by
            if extracted_limit is not None:
                smq["limit"] = extracted_limit
            output = {
                "messages": [response],
                "smq": smq,
                "current_step": "manipulation"
            }
            return output
    
    def _smq2sql_node(self, state: AgentState) -> AgentState:
        """SMQ를 SQL로 변환하는 노드"""
        smq = state.get("smq", {})
        
        if not smq:
            result = {
                "messages": [],
                "error": "SMQ가 생성되지 않았습니다.",
                "current_step": "smq2sql"
            }
            return result
        
        try:
            smq_json = json.dumps(smq, ensure_ascii=False)
            # convert_smq_to_sql은 상대 경로를 기대하므로 None으로 전달하면 기본 경로 사용
            result = convert_smq_to_sql(
                smq_json=smq_json,
                manifest_path=None,  # None이면 기본 경로(semantic_manifest.json) 사용
                dialect="bigquery"
            )
            
            if result.get("success"):
                sql_query = result.get("sql", "")
                metadata = result.get("metadata", [])
                
                # SQL을 한 줄로 변환 (공백 정리)
                final_sql_query = re.sub(r'\s+', ' ', sql_query.strip()) if sql_query else ""
                
                # columnMetadata 변환
                column_metadata = []
                if metadata:  # metadata가 None이 아닌 경우에만 처리
                    for meta in metadata:
                        column_metadata.append({
                            "type": meta.get("type", "varchar"),
                            "label": meta.get("label", meta.get("column", "")),
                            "column": meta.get("column", ""),
                            "hidden": False,
                            "displayOrder": None
                        })
                
                # usedDateFields 추출 (filters에서 날짜 필드 찾기)
                filters = smq.get("filters") or []  # None인 경우 빈 리스트로 처리
                used_date_fields = []
                if filters:  # filters가 None이 아니고 비어있지 않은 경우에만 처리
                    for filter_str in filters:
                        # 필터 문자열에서 날짜 필드 패턴 찾기 (예: prj_info__rch_st_year)
                        if filter_str:  # filter_str이 None이 아닌 경우에만 처리
                            match = re.search(r'(\w+__\w+)', str(filter_str))
                            if match:
                                field = match.group(1)
                                if field not in used_date_fields:
                                    used_date_fields.append(field)
                
                # splitedQuestion 가져오기
                split_questions = state.get("split_questions", [])
                splited_question = split_questions[0] if split_questions else state.get("user_query", "")
                
                # selectedEntities 가져오기
                selected_entities = state.get("selected_models", [])
                
                # UUID 생성
                node_id = str(uuid.uuid4())
                session_id = str(uuid.uuid4())
                workflow_id = str(uuid.uuid4())
                
                # 출력 형식 구성
                output = {
                    "smq": smq,
                    "nodeId": node_id,
                    "sqlQuery": sql_query,  # 포맷팅된 SQL
                    "userInfo": {
                        "dbms": None,
                        "prodCd": None,
                        "userId": "rerpadmin",
                        "companyId": "",
                        "inttBizNo": None,
                        "linkBizNo": "",
                        "useInttId": None,
                        "linkUserId": None,
                        "inttCntrctId": None,
                        "linkCntrctId": None,
                        "additionalFields": {}
                    },
                    "sessionId": session_id,
                    "workflowId": workflow_id,
                    "finalSqlQuery": final_sql_query,  # 한 줄 SQL
                    "columnMetadata": column_metadata,
                    "usedDateFields": used_date_fields,
                    "splitedQuestion": splited_question,
                    "selectedEntities": selected_entities
                }
                
                result_output = {
                    "messages": [],
                    "sql_query": sql_query,  # 기존 호환성을 위해 유지
                    "sql_result": output,  # 전체 결과 저장
                    "current_step": "smq2sql"
                }
                return result_output
            else:
                result_output = {
                    "messages": [],
                    "error": result.get("error", "SQL 변환 실패"),
                    "current_step": "smq2sql"
                }
                return result_output
        except Exception as e:
            result_output = {
                "messages": [],
                "error": f"SQL 변환 오류: {str(e)}",
                "current_step": "smq2sql"
            }
            return result_output
    
    def _execute_query_node(self, state: AgentState) -> AgentState:
        """쿼리 실행 노드 - LLM을 사용하여 예시 데이터 생성"""
        sql_query = state.get("sql_query", "")
        user_query = state.get("user_query", "")
        sql_result = state.get("sql_result", {})
        
        if not sql_query:
            return {
                "messages": [],
                "error": "SQL 쿼리가 생성되지 않았습니다.",
                "current_step": "executeQuery"
            }
        
        # columnMetadata에서 컬럼 정보 추출
        column_metadata = sql_result.get("columnMetadata", [])
        columns = []
        columns_info = []
        if column_metadata:
            for col in column_metadata:
                col_name = col.get("column", "")
                col_type = col.get("type", "varchar")
                col_label = col.get("label", col_name)
                if col_name:
                    columns.append(col_name)
                    columns_info.append({
                        "column": col_name,
                        "label": col_label,
                        "type": col_type
                    })
        
        # LLM에게 예시 데이터 생성 요청
        prompt = f"""당신은 SQL 쿼리 실행 결과를 시뮬레이션하는 전문가입니다.

사용자 질문: {user_query}

실행할 SQL 쿼리:
{sql_query}

컬럼 정보 (컬럼명, 라벨, 타입):
{json.dumps(columns_info, ensure_ascii=False, indent=2) if columns_info else "컬럼 정보 없음"}

위 SQL 쿼리를 실행했을 때 나올 수 있는 결과 데이터를 생성해주세요.
- 데이터는 질문에 답변하기에 충분한 최소한의 행만 생성하세요 (보통 3-10개 행 정도)
- 각 컬럼의 데이터 타입(type)과 의미(label)를 고려하여 현실적인 데이터를 생성하세요
- 숫자 타입(varchar가 아닌 경우)은 적절한 범위의 숫자 값을 사용하세요
- 날짜 타입은 적절한 형식(YYYY-MM-DD 등)으로 생성하세요
- 문자열 타입은 컬럼의 의미(label)에 맞는 현실적인 값을 생성하세요
- NULL 값은 필요시 포함할 수 있습니다
- 컬럼명은 반드시 위에 제공된 컬럼명을 그대로 사용하세요

다음 JSON 형식으로 응답하세요:
{{
    "rows": [
        {{"column1": "value1", "column2": 123, ...}},
        {{"column1": "value2", "column2": 456, ...}},
        ...
    ],
    "columns": ["column1", "column2", ...]
}}

설명 없이 JSON만 출력하세요."""
        
        messages = [
            SystemMessage(content="당신은 SQL 쿼리 실행 결과를 시뮬레이션하는 전문가입니다. JSON 형식으로만 응답하세요."),
            HumanMessage(content=prompt)
        ]
        
        try:
            response = self.llm.invoke(messages)
            
            # LLM 응답에서 JSON 추출
            response_text = response.content.strip()
            
            # JSON 코드 블록이 있으면 제거
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            
            query_result = json.loads(response_text)
            
            # columns가 없으면 rows에서 추출
            if not query_result.get("columns") and query_result.get("rows"):
                if query_result["rows"]:
                    query_result["columns"] = list(query_result["rows"][0].keys())
            
            return {
                "messages": [response],
                "query_result": query_result,
                "current_step": "executeQuery",
                "processed_prompt": prompt  # 변수 치환된 프롬프트 저장
            }
        except json.JSONDecodeError as e:
            # JSON 파싱 실패 시 빈 결과 반환
            logger.warning(f"예시 데이터 생성 실패 (JSON 파싱 오류): {str(e)}")
            query_result = {
                "rows": [],
                "columns": columns if columns else []
            }
            return {
                "messages": [],
                "query_result": query_result,
                "current_step": "executeQuery"
            }
        except Exception as e:
            # 기타 예외 발생 시 빈 결과 반환
            logger.error(f"예시 데이터 생성 오류: {str(e)}")
            query_result = {
                "rows": [],
                "columns": columns if columns else []
            }
            return {
                "messages": [],
                "query_result": query_result,
                "current_step": "executeQuery"
            }
    
    def _respondent_node(self, state: AgentState) -> AgentState:
        """응답 생성 노드"""
        user_query = state.get("user_query", "")
        query_result = state.get("query_result", {})
        sql_query = state.get("sql_query", "")
        sql_result = state.get("sql_result", {})
        
        prompt = f"""사용자 질문에 대한 최종 응답을 생성하세요.

사용자 질문: {user_query}
실행된 SQL: {sql_query}
쿼리 결과: {json.dumps(query_result, ensure_ascii=False, indent=2)}

사용자가 이해하기 쉽고 자연스러운 언어로 응답을 작성하세요."""
        
        messages = [
            SystemMessage(content="당신은 데이터 분석 결과를 사용자에게 설명하는 전문가입니다."),
            HumanMessage(content=prompt)
        ]
        
        response = self.llm.invoke(messages)
        
        result = {
            "messages": [response],
            "final_response": response.content,
            "query_result": query_result,  # executeQuery에서 생성한 데이터 저장
            "sql_result": sql_result,  # sql_result도 함께 저장
            "current_step": "respondent",
            "processed_prompt": prompt  # 변수 치환된 프롬프트 저장
        }
        return result
    
    async def run(self, user_query: str, system_prompt: str = ""):
        """
        에이전트 실행
        
        Args:
            user_query: 사용자 질문
            system_prompt: 시스템 프롬프트 (선택사항)
        
        Yields:
            이벤트 딕셔너리 (type, content, step 등)
        """
        # 초기 상태 설정
        initial_state: AgentState = {
            "messages": [HumanMessage(content=user_query)],
            "user_query": user_query,
            "current_step": "start",
            "classified_intent": None,
            "split_questions": None,
            "selected_models": None,
            "extracted_metrics": None,
            "extracted_dimensions": None,
            "extracted_filters": None,
            "extracted_order_by": None,
            "extracted_limit": None,
            "date_filters": None,
            "other_filters": None,
            "smq": None,
            "sql_query": None,
            "sql_result": None,
            "query_result": None,
            "final_response": None,
            "error": None
        }
        
        try:
            killjoy_executed = False
            async for state in self.app.astream(initial_state):
                # 각 노드의 상태 업데이트를 이벤트로 전송
                for node_name, node_state in state.items():
                    if isinstance(node_state, dict) and "current_step" in node_state:
                        step = node_state.get("current_step", "")
                        
                        # 각 단계별 이벤트 전송
                        # 프롬프트 이벤트 먼저 전송
                        processed_prompt = node_state.get('processed_prompt')
                        if processed_prompt:
                            yield {
                                "type": "prompt",
                                "content": processed_prompt,
                                "step": step
                            }
                        
                        if step == "classifyJoy":
                            intent = node_state.get('classified_intent', '')
                            if intent == "NO":
                                yield {
                                    "type": "thought",
                                    "content": f"질문 분류 완료: {intent} (재무 데이터와 무관한 질문으로 판단됨)",
                                    "step": step
                                }
                            else:
                                yield {
                                    "type": "thought",
                                    "content": f"질문 분류 완료: {intent} (유효한 질문으로 판단됨)",
                                    "step": step
                                }
                        elif step == "killjoy":
                            killjoy_executed = True
                            final_response = node_state.get("final_response", "")
                            yield {
                                "type": "success",
                                "content": final_response,
                                "step": step
                            }
                            # killjoy 실행 후 complete 이벤트 전송하고 종료
                            yield {
                                "type": "complete",
                                "content": final_response
                            }
                            return  # killjoy 실행 후 즉시 종료
                        elif step == "splitQuestion":
                            split_questions = node_state.get('split_questions', [])
                            yield {
                                "type": "thought",
                                "content": f"질문 분할 완료: {len(split_questions)}개",
                                "step": step,
                                "details": {
                                    "split_questions": split_questions
                                }
                            }
                        elif step == "modelSelector":
                            selected_models = node_state.get('selected_models', [])
                            yield {
                                "type": "thought",
                                "content": f"모델 선택 완료: {', '.join(selected_models) if selected_models else '없음'}",
                                "step": step,
                                "details": {
                                    "selected_models": selected_models
                                }
                            }
                        elif step == "extractMetrics":
                            extracted_metrics = node_state.get('extracted_metrics', [])
                            extracted_dimensions = node_state.get('extracted_dimensions', [])
                            yield {
                                "type": "thought",
                                "content": f"메트릭 추출 완료: {len(extracted_metrics)}개",
                                "step": step,
                                "details": {
                                    "metrics": extracted_metrics,
                                    "group_by": extracted_dimensions
                                }
                            }
                        elif step == "extractFilters":
                            extracted_filters = node_state.get('extracted_filters', [])
                            yield {
                                "type": "thought",
                                "content": f"필터 추출 완료: {len(extracted_filters)}개",
                                "step": step,
                                "details": {
                                    "filters": extracted_filters
                                }
                            }
                        elif step == "extractOrderByAndLimit":
                            order_by = node_state.get('extracted_order_by', [])
                            limit = node_state.get('extracted_limit')
                            limit_str = str(limit) if limit is not None else "None"
                            yield {
                                "type": "thought",
                                "content": f"정렬 및 제한 추출 완료: order_by={len(order_by)}개, limit={limit_str}",
                                "step": step,
                                "details": {
                                    "order_by": order_by,
                                    "limit": limit
                                }
                            }
                        elif step == "dateFilter":
                            yield {
                                "type": "thought",
                                "content": "날짜 필터 추출 완료",
                                "step": step
                            }
                        elif step == "otherFilter":
                            yield {
                                "type": "thought",
                                "content": "기타 필터 추출 완료",
                                "step": step
                            }
                        elif step == "manipulation":
                            smq = node_state.get("smq", {})
                            yield {
                                "type": "thought",
                                "content": "SMQ 생성 완료",
                                "step": step,
                                "details": {
                                    "smq": smq
                                }
                            }
                        elif step == "smq2sql":
                            if node_state.get("error"):
                                yield {
                                    "type": "error",
                                    "content": node_state.get("error"),
                                    "step": step
                                }
                            else:
                                sql_result = node_state.get("sql_result", {})
                                yield {
                                    "type": "tool_result",
                                    "content": f"SQL 변환 완료:\n{node_state.get('sql_query', '')}",
                                    "step": step,
                                    "sql": node_state.get("sql_query"),
                                    "sql_result": sql_result  # 전체 결과 포함
                                }
                        elif step == "executeQuery":
                            yield {
                                "type": "tool_result",
                                "content": "쿼리 실행 완료",
                                "step": step,
                                "result": node_state.get("query_result")
                            }
                        elif step == "respondent":
                            yield {
                                "type": "success",
                                "content": node_state.get("final_response", ""),
                                "step": step,
                                "query_result": node_state.get("query_result"),  # executeQuery에서 생성한 데이터 포함
                                "sql_result": node_state.get("sql_result"),  # SQL 변환 결과
                                "sql_query": node_state.get("sql_query"),  # 생성된 SQL 쿼리
                                "smq": node_state.get("smq")  # 생성된 SMQ
                            }
            
            # killjoy가 실행되지 않은 경우에만 최종 상태 가져오기
            if not killjoy_executed:
                # 최종 상태 가져오기
                final_state = await self.app.ainvoke(initial_state)
                
                if final_state.get("error"):
                    yield {
                        "type": "error",
                        "content": final_state["error"]
                    }
                elif final_state.get("final_response"):
                    # 구조화된 데이터와 자연어 응답 모두 반환
                    yield {
                        "type": "complete",
                        "content": final_state["final_response"],  # respondent에서 생성한 최종 답변
                        "query_result": final_state.get("query_result"),  # executeQuery에서 생성한 예시 데이터
                        "sql_result": final_state.get("sql_result"),  # 구조화된 데이터 포함
                        "smq": final_state.get("smq"),  # SMQ도 포함
                        "sql_query": final_state.get("sql_query")  # SQL 쿼리도 포함
                    }
            
        except Exception as e:
            import traceback
            error_traceback = traceback.format_exc()
            logger.error(f"[에이전트 실행 오류] {str(e)}\n{error_traceback}")
            yield {
                "type": "error",
                "content": f"에이전트 실행 오류: {str(e)}"
            }


# 편의 함수
async def create_langgraph_agent(model_name: str = "gpt-4o"):
    """LangGraph 에이전트 생성"""
    return LangGraphAgent(model_name=model_name)
