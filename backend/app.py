import os
import sys
import glob
import json
import asyncio

# 상위 디렉토리를 Python path에 추가하여 backend 모듈을 찾을 수 있도록 함
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# vendor 디렉토리 경로 설정 (가장 먼저 실행되어야 함)
import vendor_setup  # noqa: F401

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import tempfile
import shutil
from openai import AsyncOpenAI
from dotenv import load_dotenv

from backend.semantic_agent import SemanticAgent
from backend.smq_agent import SMQAgent
from backend.langgraph_agent import LangGraphAgent
from backend.tools import parse_semantic_models, read_file, edit_file, convert_smq_to_sql

load_dotenv()

app = FastAPI(title="Semantic Agent API")

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 정적 파일 서빙 (React 빌드 파일)
frontend_build_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'frontend', 'build'))
if os.path.exists(frontend_build_path):
    # Vite 빌드 결과물을 통째로 서빙 (index.html 및 assets 포함)
    app.mount("/static", StaticFiles(directory=frontend_build_path), name="static")

# Playground 디렉토리 경로
PLAYGROUND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), 'playground'))
PROMPT_FILE = os.path.join(os.path.dirname(__file__), 'prompts', 'system_prompt.txt')
YML_MANAGEMENT_PROMPT_FILE = os.path.join(os.path.dirname(__file__), 'prompts', 'yml_management_prompt.txt')

# Helper functions
def get_playground_dir():
    return PLAYGROUND_DIR

def load_prompt(prompt_type: str = "test"):
    """
    프롬프트 파일을 로드합니다.
    Args:
        prompt_type: "test" (테스트 케이스 생성용), "yml" (YML 파일 관리용),
                     "split_question" (질문 분할용), "classify_joy" (질문 분류용),
                     "entity_selector" (Entity 선택용), "extract_metrics" (Metrics 추출용),
                     "extract_filters" (Filters 추출용), "extract_order_by_and_limit" (Order by & Limit 추출용),
                     "postprocess" (후처리 프롬프트)
    """
    prompts_dir = os.path.join(os.path.dirname(__file__), 'prompts')
    
    if prompt_type == "yml":
        prompt_file = YML_MANAGEMENT_PROMPT_FILE
    elif prompt_type == "split_question":
        prompt_file = os.path.join(prompts_dir, 'split_question_prompt.txt')
    elif prompt_type == "classify_joy":
        prompt_file = os.path.join(prompts_dir, 'classify_joy_prompt.txt')
    elif prompt_type == "entity_selector":
        prompt_file = os.path.join(prompts_dir, 'entity_selector_prompt.txt')
    elif prompt_type == "extract_metrics":
        prompt_file = os.path.join(prompts_dir, 'extract_metrics_prompt.txt')
    elif prompt_type == "extract_filters":
        prompt_file = os.path.join(prompts_dir, 'extract_filters_prompt.txt')
    elif prompt_type == "extract_order_by_and_limit":
        prompt_file = os.path.join(prompts_dir, 'extract_order_by_and_limit_prompt.txt')
    elif prompt_type == "postprocess":
        prompt_file = os.path.join(prompts_dir, 'postprocess_prompt.txt')
    elif prompt_type == "test":
        prompt_file = PROMPT_FILE
    else:
        # 알 수 없는 타입인 경우 빈 문자열 반환
        return ""
    
    if os.path.exists(prompt_file):
        with open(prompt_file, 'r', encoding='utf-8') as f:
            content = f.read()
            return content
    else:
        return ""

def save_prompt(content: str, prompt_type: str = "test"):
    """
    프롬프트 파일을 저장합니다.
    Args:
        content: 저장할 프롬프트 내용
        prompt_type: "test" (테스트 케이스 생성용), "yml" (YML 파일 관리용),
                     "split_question" (질문 분할용), "classify_joy" (질문 분류용),
                     "entity_selector" (Entity 선택용), "extract_metrics" (Metrics 추출용),
                     "extract_filters" (Filters 추출용), "extract_order_by_and_limit" (Order by & Limit 추출용),
                     "postprocess" (후처리 프롬프트)
    """
    prompts_dir = os.path.join(os.path.dirname(__file__), 'prompts')
    
    if prompt_type == "yml":
        prompt_file = YML_MANAGEMENT_PROMPT_FILE
    elif prompt_type == "split_question":
        prompt_file = os.path.join(prompts_dir, 'split_question_prompt.txt')
    elif prompt_type == "classify_joy":
        prompt_file = os.path.join(prompts_dir, 'classify_joy_prompt.txt')
    elif prompt_type == "entity_selector":
        prompt_file = os.path.join(prompts_dir, 'entity_selector_prompt.txt')
    elif prompt_type == "extract_metrics":
        prompt_file = os.path.join(prompts_dir, 'extract_metrics_prompt.txt')
    elif prompt_type == "extract_filters":
        prompt_file = os.path.join(prompts_dir, 'extract_filters_prompt.txt')
    elif prompt_type == "extract_order_by_and_limit":
        prompt_file = os.path.join(prompts_dir, 'extract_order_by_and_limit_prompt.txt')
    elif prompt_type == "postprocess":
        prompt_file = os.path.join(prompts_dir, 'postprocess_prompt.txt')
    else:
        prompt_file = PROMPT_FILE
    
    # 디렉토리가 없으면 생성
    os.makedirs(os.path.dirname(prompt_file), exist_ok=True)
    
    with open(prompt_file, 'w', encoding='utf-8') as f:
        f.write(content)

# Pydantic models
class FileContent(BaseModel):
    path: str
    content: str

class FileEdit(BaseModel):
    file: str
    edits: List[Dict[str, str]]

class EditRequest(BaseModel):
    proposals: List[FileEdit]

class CreateFileRequest(BaseModel):
    filename: str
    content: Optional[str] = None

class RenameFileRequest(BaseModel):
    old_path: str
    new_filename: str

class DeleteFileRequest(BaseModel):
    file_path: str

class ChatMessage(BaseModel):
    role: str
    content: str
    steps: Optional[List[Dict[str, Any]]] = None

class ChatRequest(BaseModel):
    message: str

class DDLRequest(BaseModel):
    dialect: str
    ddl_text: str
    filename: Optional[str] = None

class SMQConvertRequest(BaseModel):
    smq: str
    manifest_path: Optional[str] = None
    dialect: str = "bigquery"

class EvaluationResult(BaseModel):
    success: bool
    result: Optional[str] = None
    error: Optional[str] = None

class PostProcessTestRequest(BaseModel):
    dataframe_result: str
    user_question: str
    llm_config: Optional[Dict[str, Any]] = None

class ExecuteSQLRequest(BaseModel):
    table_name: str
    table_data: List[Dict[str, Any]]
    sql: str

class ExecuteSQLResult(BaseModel):
    success: bool
    columns: Optional[List[str]] = None
    rows: Optional[List[List[Any]]] = None
    error: Optional[str] = None

class ExecutePostgreSQLRequest(BaseModel):
    sql: str

class ExecutePostgreSQLResult(BaseModel):
    success: bool
    columns: Optional[List[str]] = None
    rows: Optional[List[List[Any]]] = None
    row_count: Optional[int] = None
    error: Optional[str] = None

# 평가용 도구 함수들
def read_file_for_evaluation(base_dir, path):
    """평가용 파일 읽기 - 임시 디렉토리 기반"""
    target_path = os.path.join(base_dir, path)
    if not os.path.exists(target_path):
        return {"error": f"File not found: {path}"}
    
    try:
        # 엑셀 파일 처리 (.xlsx, .xls)
        if path.lower().endswith(('.xlsx', '.xls')):
            try:
                import pandas as pd
                # 엑셀 파일을 읽어서 모든 시트를 처리
                excel_file = pd.ExcelFile(target_path)
                content_parts = []
                
                for sheet_name in excel_file.sheet_names:
                    df = pd.read_excel(excel_file, sheet_name=sheet_name)
                    content_parts.append(f"=== 시트: {sheet_name} ===\n")
                    # DataFrame을 문자열로 변환 (인덱스 포함)
                    content_parts.append(df.to_string(index=True))
                    content_parts.append("\n\n")
                
                content = "".join(content_parts)
                # 파일이 너무 큰 경우 일부만 읽기
                if len(content) > 100000:
                    content = content[:100000] + "\n\n... (파일이 너무 커서 일부만 표시됩니다) ..."
                return {"content": content}
            except ImportError:
                return {"error": "엑셀 파일을 읽기 위해 pandas와 openpyxl이 필요합니다. 'pip install pandas openpyxl' 명령을 실행해주세요."}
            except Exception as e:
                return {"error": f"엑셀 파일 읽기 오류: {str(e)}"}
        # JSON 파일 처리
        elif path.lower().endswith('.json'):
            with open(target_path, 'r', encoding='utf-8', errors='ignore') as f:
                try:
                    data = json.load(f)
                    content = json.dumps(data, indent=2, ensure_ascii=False)
                    return {"content": content}
                except json.JSONDecodeError:
                    with open(target_path, 'r', encoding='utf-8', errors='ignore') as f2:
                        content = f2.read()
                    return {"content": content}
        # 일반 텍스트 파일 처리
        else:
            with open(target_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                # 파일이 너무 큰 경우 일부만 읽기
                if len(content) > 100000:
                    content = content[:100000] + "\n\n... (파일이 너무 커서 일부만 표시됩니다) ..."
            return {"content": content}
    except Exception as e:
        return {"error": str(e)}

class EvaluationAgent(SemanticAgent):
    """평가 전용 에이전트 - 임시 디렉토리 기반 파일 읽기 지원"""
    def __init__(self, evaluation_base_dir):
        super().__init__()
        self.evaluation_base_dir = evaluation_base_dir
        self.final_report = None
    
    async def run_evaluation(self, system_prompt, user_request, max_turns=10):
        """평가 실행 - readFile과 submitReport 도구 지원"""
        if not self.client:
            yield {"type": "error", "content": "OPENAI_API_KEY not set."}
            return
        
        self.tool_history = []
        
        for turn in range(max_turns):
            # 디렉토리 구조 생성
            directory_structure = self._get_evaluation_directory_structure()
            tool_history_str = json.dumps(self.tool_history, indent=2, ensure_ascii=False)
            
            prompt = system_prompt.replace("{directory_structure}", directory_structure)\
                                  .replace("{user_request}", user_request)\
                                  .replace("{tool_history}", tool_history_str)
            
            try:
                response = await self.client.chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {"role": "system", "content": "You are a helpful Audit Agent for NL2SQL systems."},
                        {"role": "user", "content": prompt}
                    ],
                    response_format={"type": "json_object"}
                )
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
                yield {"type": "error", "content": f"JSON 파싱 실패: {content}"}
                break
            
            # Reasoning 출력
            reasoning = data.get("reasoning") or data.get("<reasoning>")
            if reasoning:
                yield {"type": "thought", "content": reasoning}
            
            # Tool 호출 처리
            tool_call_data = data.get("tool_call") or data.get("<tool_call>")
            if tool_call_data:
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
                    file_path = args.get("path")
                    if not file_path:
                        result = {"error": "파일 경로가 제공되지 않았습니다."}
                    else:
                        result = read_file_for_evaluation(self.evaluation_base_dir, file_path)
                elif tool_name == "submitReport":
                    # 최종 리포트 저장
                    final_analysis = args.get("final_analysis", {})
                    score = args.get("score", 0)
                    self.final_report = {
                        "final_analysis": final_analysis,
                        "score": score
                    }
                    result = {
                        "success": True,
                        "message": "평가 리포트가 제출되었습니다.",
                        "score": score,
                        "analysis": final_analysis
                    }
                    # 리포트 제출 후 종료
                    self.tool_history.append({
                        "request": tool_call_data,
                        "response": result
                    })
                    yield {"type": "tool_result", "content": json.dumps(result, indent=2, ensure_ascii=False)}
                    yield {"type": "success", "content": json.dumps(result, indent=2, ensure_ascii=False)}
                    return
                else:
                    result = {"error": f"Unknown tool: {tool_name}"}
                
                # Tool 실행 결과 기록
                self.tool_history.append({
                    "request": tool_call_data,
                    "response": result
                })
                yield {"type": "tool_result", "content": json.dumps(result, indent=2, ensure_ascii=False)}
            else:
                # Tool 호출이 없는 경우 - submitReport가 없었다면 에러
                if turn == max_turns - 1:
                    yield {"type": "error", "content": "최대 턴 수에 도달했지만 submitReport가 호출되지 않았습니다. 평가를 완료하려면 submitReport를 호출해야 합니다."}
                else:
                    # 다음 턴으로 진행
                    continue
    
    def _get_evaluation_directory_structure(self):
        """평가 디렉토리 구조 반환"""
        files = []
        for root, _, filenames in os.walk(self.evaluation_base_dir):
            for filename in filenames:
                rel_path = os.path.relpath(os.path.join(root, filename), self.evaluation_base_dir)
                files.append(rel_path.replace("\\", "/"))  # Windows 경로 정규화
        return "\n".join(files)

# API Routes
@app.get("/")
async def root():
    frontend_index = os.path.join(os.path.dirname(__file__), '..', 'frontend', 'build', 'index.html')
    if os.path.exists(frontend_index):
        return FileResponse(frontend_index)
    return {"message": "Semantic Agent API"}

@app.get("/api/files")
async def list_files():
    """Playground 디렉토리의 모든 파일 목록 반환"""
    files = glob.glob(os.path.join(PLAYGROUND_DIR, "**/*"), recursive=True)
    file_list = [
        os.path.relpath(f, PLAYGROUND_DIR) 
        for f in files 
        if os.path.isfile(f)
    ]
    return {"files": file_list}

@app.get("/api/files/{file_path:path}")
async def get_file(file_path: str):
    """파일 내용 읽기"""
    result = read_file(file_path)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result

@app.post("/api/files")
async def create_file_endpoint(request: CreateFileRequest):
    """새 파일 생성"""
    if not request.filename.endswith('.yml'):
        request.filename += '.yml'
    
    # semantic_models 디렉토리에 생성
    file_path = os.path.join(PLAYGROUND_DIR, "semantic_models", request.filename)
    
    # 디렉토리 생성
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    if os.path.exists(file_path):
        raise HTTPException(status_code=400, detail="이미 존재하는 파일명입니다.")
    
    try:
        # content가 제공되면 그 내용으로, 없으면 기본 내용으로 생성
        content = request.content if request.content else "# New Semantic Model\n"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return {
            "success": True, 
            "message": f"{request.filename} 파일이 생성되었습니다!",
            "filename": request.filename
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"파일 생성 실패: {str(e)}")

@app.post("/api/files/rename")
async def rename_file_endpoint(request: RenameFileRequest):
    """파일 이름 변경"""
    old_path = os.path.join(PLAYGROUND_DIR, request.old_path)
    if not os.path.exists(old_path):
        raise HTTPException(status_code=404, detail="파일을 찾을 수 없습니다.")
    
    if not request.new_filename.endswith('.yml'):
        request.new_filename += '.yml'
    
    new_path = os.path.join(os.path.dirname(old_path), request.new_filename)
    if os.path.exists(new_path):
        raise HTTPException(status_code=400, detail="이미 존재하는 파일명입니다.")
    
    try:
        os.rename(old_path, new_path)
        return {"success": True, "message": "파일 이름이 변경되었습니다!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"이름 변경 실패: {str(e)}")

@app.post("/api/files/delete")
async def delete_file_endpoint(request: DeleteFileRequest):
    """파일 삭제"""
    file_path = os.path.join(PLAYGROUND_DIR, request.file_path)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="파일을 찾을 수 없습니다.")
    
    try:
        os.remove(file_path)
        return {"success": True, "message": "파일이 삭제되었습니다."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"파일 삭제 실패: {str(e)}")

@app.post("/api/files/save")
async def save_file(request: FileContent):
    """파일 저장"""
    file_path = os.path.join(PLAYGROUND_DIR, request.path)
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(request.content)
        return {"success": True, "message": "파일이 저장되었습니다!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"파일 저장 실패: {str(e)}")

@app.post("/api/files/edit")
async def edit_file_endpoint(request: EditRequest):
    """파일 편집"""
    proposals = [{"file": p.file, "edits": p.edits} for p in request.proposals]
    result = edit_file(proposals)
    return result

@app.post("/api/parse")
async def parse_models():
    """Semantic Model 파싱"""
    result = parse_semantic_models(PLAYGROUND_DIR)
    return result

@app.post("/api/ddl/create")
async def create_from_ddl(request: DDLRequest):
    """DDL에서 Semantic Model 생성"""
    try:
        from backend.semantic.model_manager.utils.ddl_parser import parse_ddl_text
        from backend.semantic.model_manager.utils.ddl_to_semantic_model import generate_semantic_model_from_ddl
        
        # DDL 파싱
        ddl_text = request.ddl_text
        if not ddl_text.strip().startswith('--'):
            ddl_text = f"-- {request.dialect}\n{ddl_text}"
        
        tables = parse_ddl_text(ddl_text, request.dialect)
        
        if not tables:
            raise HTTPException(status_code=400, detail="DDL에서 테이블을 찾을 수 없습니다.")
        
        # 첫 번째 테이블 사용
        table_name, table_info = next(iter(tables.items()))
        
        # sources.yml 경로 설정
        sources_yml_path = os.path.join(PLAYGROUND_DIR, "sources.yml")
        
        # Semantic Model 생성
        yml_content = generate_semantic_model_from_ddl(
            table_info,
            table_name_in_db=table_name,
            database=table_info.database,
            schema=table_info.schema,
            sources_yml_path=sources_yml_path
        )
        
        # 파일명 생성 (filename이 제공되면 사용, 없으면 테이블명 사용)
        if request.filename:
            file_name = request.filename
            if not file_name.endswith('.yml'):
                file_name += '.yml'
        else:
            file_name = f"{table_name.lower()}.yml"
        file_path = os.path.join(PLAYGROUND_DIR, "semantic_models", file_name)
        
        # 디렉토리 생성
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # 파일 저장
        overwritten = os.path.exists(file_path)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(yml_content)
        
        return {
            "success": True,
            "message": f"{file_name} 파일이 {'덮어쓰기' if overwritten else '생성'}되었습니다!",
            "filename": file_name
        }
    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500, 
            detail=f"오류 발생: {str(e)}",
            headers={"X-Traceback": traceback.format_exc()}
        )

@app.get("/api/prompt")
async def get_prompt(prompt_type: str = "test"):
    """System prompt 가져오기"""
    try:
        prompt_content = load_prompt(prompt_type)
        return {"success": True, "prompt": prompt_content}
    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500,
            detail=f"프롬프트 로드 오류: {str(e)}",
            headers={"X-Traceback": traceback.format_exc()}
        )

@app.post("/api/prompt")
async def save_prompt_endpoint(request: Dict[str, str]):
    """System prompt 저장"""
    prompt_type = request.get("prompt_type", "test")
    save_prompt(request.get("prompt", ""), prompt_type)
    return {"success": True, "message": f"System prompt saved! (type: {prompt_type})"}

@app.get("/api/smq/prompt")
async def get_smq_prompt():
    """SMQ 프롬프트 가져오기 (전체)"""
    smq_prompt_file = os.path.join(os.path.dirname(__file__), 'prompts', 'smq_prompt.txt')
    if not os.path.exists(smq_prompt_file):
        return {"error": "SMQ 프롬프트 파일을 찾을 수 없습니다."}
    
    try:
        with open(smq_prompt_file, 'r', encoding='utf-8') as f:
            full_prompt = f.read()
        return {"success": True, "prompt": full_prompt}
    except Exception as e:
        return {"error": f"프롬프트 읽기 오류: {str(e)}"}

@app.post("/api/smq/prompt")
async def save_smq_prompt_endpoint(request: Dict[str, Any]):
    """SMQ 프롬프트 저장 (전체)"""
    try:
        prompt_content = request.get("prompt", "")
        if not prompt_content:
            return {"error": "프롬프트 내용이 없습니다."}
        
        smq_prompt_file = os.path.join(os.path.dirname(__file__), 'prompts', 'smq_prompt.txt')
        os.makedirs(os.path.dirname(smq_prompt_file), exist_ok=True)
        
        with open(smq_prompt_file, 'w', encoding='utf-8') as f:
            f.write(prompt_content)
        
        return {"success": True, "message": "프롬프트가 저장되었습니다."}
    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500,
            detail=f"프롬프트 저장 오류: {str(e)}",
            headers={"X-Traceback": traceback.format_exc()}
        )

def parse_smq_prompt(full_prompt: str) -> Dict[str, str]:
    """SMQ 프롬프트를 툴별로 파싱"""
    import re
    
    parts = {
        "system_role": "",
        "tool_constraints": "",
        "tool_list_header": "",
        "tool1_description": "",
        "tool2_description": "",
        "tool3_description": "",
        "detailed_instructions": "",
        "response_format": ""
    }
    
    # 시스템 역할 추출 (처음부터 Tag Guidance 전까지)
    tag_guidance_match = re.search(r'## Tag Guidance', full_prompt)
    if tag_guidance_match:
        parts["system_role"] = full_prompt[:tag_guidance_match.start()].strip()
    
    # Tag Guidance 부분 추출
    tag_guidance_end = re.search(r'<tool_list>', full_prompt)
    if tag_guidance_match and tag_guidance_end:
        parts["tool_list_header"] = full_prompt[tag_guidance_match.start():tag_guidance_end.start()].strip()
    
    # Tool 1 추출 (## Tool 1. 부터 ## Tool 2. 또는 # SQL 변환 전까지)
    tool1_start = re.search(r'## Tool 1\.', full_prompt)
    tool1_end = re.search(r'(?=## Tool 2\.|# SQL 변환|</tool_list>)', full_prompt[tool1_start.end():] if tool1_start else '')
    if tool1_start:
        tool1_end_pos = tool1_start.end() + tool1_end.start() if tool1_end else len(full_prompt)
        tool1_content = full_prompt[tool1_start.start():tool1_end_pos].strip()
        # "## Tool 1." 제목 부분 제거하고 내용만 추출
        tool1_lines = tool1_content.split('\n', 1)
        if len(tool1_lines) > 1:
            parts["tool1_description"] = tool1_lines[1].strip()
        else:
            parts["tool1_description"] = tool1_content.replace("## Tool 1.", "").strip()
    
    # Tool 2 추출
    tool2_start = re.search(r'## Tool 2\.', full_prompt)
    tool2_end = re.search(r'(?=## Tool 3\.|</tool_list>)', full_prompt[tool2_start.end():] if tool2_start else '')
    if tool2_start:
        tool2_end_pos = tool2_start.end() + tool2_end.start() if tool2_end else len(full_prompt)
        tool2_content = full_prompt[tool2_start.start():tool2_end_pos].strip()
        tool2_lines = tool2_content.split('\n', 1)
        if len(tool2_lines) > 1:
            parts["tool2_description"] = tool2_lines[1].strip()
        else:
            parts["tool2_description"] = tool2_content.replace("## Tool 2.", "").strip()
    
    # Tool 3 추출
    tool3_start = re.search(r'## Tool 3\.', full_prompt)
    tool3_end = re.search(r'</tool_list>', full_prompt[tool3_start.end():] if tool3_start else '')
    if tool3_start:
        tool3_end_pos = tool3_start.end() + tool3_end.start() if tool3_end else len(full_prompt)
        tool3_content = full_prompt[tool3_start.start():tool3_end_pos].strip()
        tool3_lines = tool3_content.split('\n', 1)
        if len(tool3_lines) > 1:
            parts["tool3_description"] = tool3_lines[1].strip()
        else:
            parts["tool3_description"] = tool3_content.replace("## Tool 3.", "").strip()
    
    # detailed_instructions 추출
    detailed_match = re.search(r'<detailed_instructions>(.+?)</detailed_instructions>', full_prompt, re.DOTALL)
    if detailed_match:
        parts["detailed_instructions"] = detailed_match.group(1).strip()
    
    # Response Format 추출 (## Response Format부터 끝까지)
    response_format_match = re.search(r'## Response Format', full_prompt)
    if response_format_match:
        parts["response_format"] = full_prompt[response_format_match.start():].strip()
    
    # 제약사항 부분 추출 (❌ 절대 사용하지 마세요부터 ⚠️ 경고 전까지)
    constraints_match = re.search(r'\*\*❌ 절대 사용하지 마세요[^\n]*\n(.+?)\*\*⚠️ 경고', full_prompt, re.DOTALL)
    if constraints_match:
        parts["tool_constraints"] = constraints_match.group(1).strip()
    
    return parts

def combine_smq_prompt_parts(parts: Dict[str, Any]) -> str:
    """툴별 프롬프트를 하나로 합치기"""
    system_role = parts.get("system_role", "").strip()
    tool_constraints = parts.get("tool_constraints", "").strip()
    tool_list_header = parts.get("tool_list_header", "").strip()
    tool1_description = parts.get("tool1_description", "").strip()
    tool2_description = parts.get("tool2_description", "").strip()
    tool3_description = parts.get("tool3_description", "").strip()
    detailed_instructions = parts.get("detailed_instructions", "").strip()
    response_format = parts.get("response_format", "").strip()
    
    # 프롬프트 템플릿 조합
    prompt_parts = [system_role]
    
    if tool_constraints:
        prompt_parts.append(f"**❌ 절대 사용하지 마세요 (이 tool들은 존재하지 않거나 사용할 수 없습니다):**\n{tool_constraints}")
    
    if tool_list_header:
        prompt_parts.append(tool_list_header)
    
    tool_list_content = "<tool_list>\n# 시멘틱 레이어 탐색\n## Tool 1. 시멘틱 모델 파일 선택 및 내용 읽기 (필수 첫 단계)\n{available_files}\n\n" + tool1_description
    
    if tool2_description:
        tool_list_content += "\n\n# SQL 변환을 위한 SMQ 작성 및 수정\n## Tool 2. SMQ를 새로 작성하여 새롭게 smqState을 설정\n" + tool2_description
    
    if tool3_description:
        tool_list_content += "\n\n## Tool 3. 기존 smqState의 배열을 index로 접근하여 edit\n" + tool3_description
    
    tool_list_content += "\n</tool_list>"
    prompt_parts.append(tool_list_content)
    
    if detailed_instructions:
        prompt_parts.append(f"<detailed_instructions>\n{detailed_instructions}\n</detailed_instructions>")
    
    prompt_parts.append("<user_input>\n{user_query}\n</user_input>")
    prompt_parts.append("<tool_calling_history>\n{tool_history}\n</tool_calling_history>")
    
    if response_format:
        prompt_parts.append(response_format)
    
    return "\n\n".join(prompt_parts)

@app.post("/api/smq/convert")
async def convert_smq_endpoint(request: SMQConvertRequest):
    """SMQ를 SQL 쿼리로 변환"""
    try:
        result = convert_smq_to_sql(
            smq_json=request.smq,
            manifest_path=request.manifest_path,
            dialect=request.dialect
        )
        return result
    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500,
            detail=f"SMQ 변환 오류: {str(e)}",
            headers={"X-Traceback": traceback.format_exc()}
        )

@app.post("/api/smq/execute", response_model=ExecutePostgreSQLResult)
async def execute_sql_endpoint(request: ExecutePostgreSQLRequest):
    """생성된 SQL 쿼리를 PostgreSQL 데이터베이스에서 실행"""
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        # DB 연결 정보 (환경변수에서 가져오거나 기본값 사용)
        db_host = os.getenv("DB_HOST", "dev.daquv.com")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME", "test_3")
        db_user = os.getenv("DB_USER", "daquv")
        db_password = os.getenv("DB_PASSWORD", "daquv123!@()")
        
        # SQL에서 마크다운 코드 블록 제거
        cleaned_sql = request.sql.strip()
        if cleaned_sql.startswith('```sql'):
            cleaned_sql = cleaned_sql[6:].lstrip()
        elif cleaned_sql.startswith('```'):
            cleaned_sql = cleaned_sql[3:].lstrip()
        if cleaned_sql.endswith('```'):
            cleaned_sql = cleaned_sql[:-3].rstrip()
        cleaned_sql = cleaned_sql.strip()
        
        # Oracle SQL을 PostgreSQL로 변환 (필요한 경우)
        try:
            import sqlglot
            # Oracle dialect로 파싱 시도
            try:
                parsed = sqlglot.parse_one(cleaned_sql, dialect='oracle')
                # PostgreSQL로 변환
                cleaned_sql = parsed.sql(dialect='postgres')
            except Exception:
                # Oracle 파싱 실패 시 그대로 사용 (이미 PostgreSQL일 수도 있음)
                pass
        except ImportError:
            # sqlglot이 없으면 그대로 사용
            pass
        
        # PostgreSQL 연결 및 쿼리 실행
        try:
            # 연결 정보 로깅 (디버깅용)
            import logging
            import socket
            logger = logging.getLogger("app")
            logger.info(f"PostgreSQL 연결 시도: {db_user}@{db_host}:{db_port}/{db_name}")
            
            # 네트워크 연결 테스트 (호스트와 포트 접근 가능 여부 확인)
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex((db_host, int(db_port)))
                sock.close()
                if result != 0:
                    return ExecutePostgreSQLResult(
                        success=False,
                        error=f"네트워크 연결 실패:\n호스트 {db_host}:{db_port}에 연결할 수 없습니다.\n포트가 열려있지 않거나 방화벽에 의해 차단되었을 수 있습니다.\n연결 테스트 결과 코드: {result}"
                    )
            except socket.gaierror as e:
                return ExecutePostgreSQLResult(
                    success=False,
                    error=f"DNS 해석 실패:\n호스트 '{db_host}'를 찾을 수 없습니다.\n오류: {str(e)}"
                )
            except Exception as sock_error:
                logger.warning(f"소켓 테스트 중 오류 (무시): {str(sock_error)}")
            
            # 연결 타임아웃 설정 (10초)
            conn = psycopg2.connect(
                host=db_host,
                port=int(db_port),
                database=db_name,
                user=db_user,
                password=db_password,
                connect_timeout=10
            )
            
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            try:
                cursor.execute(cleaned_sql)
                
                # SELECT 쿼리인 경우 결과 반환
                if cleaned_sql.strip().upper().startswith('SELECT'):
                    rows = cursor.fetchall()
                    columns = list(rows[0].keys()) if rows else []
                    rows_data = [[row[col] for col in columns] for row in rows] if rows else []
                    
                    return ExecutePostgreSQLResult(
                        success=True,
                        columns=columns,
                        rows=rows_data,
                        row_count=len(rows_data)
                    )
                else:
                    # INSERT, UPDATE, DELETE 등의 경우
                    conn.commit()
                    row_count = cursor.rowcount
                    
                    return ExecutePostgreSQLResult(
                        success=True,
                        columns=None,
                        rows=None,
                        row_count=row_count
                    )
            except psycopg2.Error as sql_error:
                conn.rollback()
                error_message = str(sql_error)
                # PostgreSQL 오류 메시지에서 더 자세한 정보 추출
                if hasattr(sql_error, 'pgerror') and sql_error.pgerror:
                    error_message = sql_error.pgerror
                if hasattr(sql_error, 'pgcode') and sql_error.pgcode:
                    error_message = f"[{sql_error.pgcode}] {error_message}"
                
                return ExecutePostgreSQLResult(
                    success=False,
                    error=error_message
                )
            finally:
                cursor.close()
                conn.close()
                
        except psycopg2.OperationalError as conn_error:
            # 더 자세한 오류 정보 추출
            import logging
            logger = logging.getLogger("app")
            
            error_details = []
            
            # 예외 객체의 모든 속성 확인
            error_str = str(conn_error)
            error_repr = repr(conn_error)
            
            # 기본 오류 메시지
            if error_str and error_str.strip():
                error_details.append(f"연결 실패: {error_str}")
            elif error_repr:
                error_details.append(f"연결 실패: {error_repr}")
            else:
                error_details.append("연결 실패: 알 수 없는 오류")
            
            # 연결 정보 추가 (보안을 위해 비밀번호는 제외)
            error_details.append(f"호스트: {db_host}:{db_port}")
            error_details.append(f"데이터베이스: {db_name}")
            error_details.append(f"사용자: {db_user}")
            
            # psycopg2의 모든 속성 확인
            error_attrs = {}
            for attr in ['pgerror', 'pgcode', 'diag', 'args', '__cause__', '__context__']:
                if hasattr(conn_error, attr):
                    value = getattr(conn_error, attr)
                    if value is not None:
                        error_attrs[attr] = value
            
            # PostgreSQL 오류 정보
            if 'pgerror' in error_attrs:
                error_details.append(f"PostgreSQL 오류 메시지: {error_attrs['pgerror']}")
            if 'pgcode' in error_attrs:
                error_details.append(f"PostgreSQL 오류 코드: {error_attrs['pgcode']}")
            if 'diag' in error_attrs:
                diag = error_attrs['diag']
                if hasattr(diag, 'severity'):
                    error_details.append(f"심각도: {diag.severity}")
                if hasattr(diag, 'message_primary'):
                    error_details.append(f"주요 메시지: {diag.message_primary}")
                if hasattr(diag, 'message_detail'):
                    error_details.append(f"상세 메시지: {diag.message_detail}")
            
            # 예외 타입 및 전체 정보
            error_details.append(f"예외 타입: {type(conn_error).__name__}")
            
            # args 확인 (튜플일 수 있음)
            if 'args' in error_attrs:
                args_value = error_attrs['args']
                if args_value:
                    if isinstance(args_value, tuple):
                        error_details.append(f"예외 인자: {', '.join(str(a) for a in args_value if a)}")
                    else:
                        error_details.append(f"예외 인자: {args_value}")
            
            # 중첩된 예외 확인 (__cause__ 또는 __context__)
            if '__cause__' in error_attrs:
                cause = error_attrs['__cause__']
                if cause:
                    error_details.append(f"원인 예외: {type(cause).__name__}: {str(cause)}")
            if '__context__' in error_attrs:
                context = error_attrs['__context__']
                if context:
                    error_details.append(f"컨텍스트 예외: {type(context).__name__}: {str(context)}")
            
            # traceback 정보 추가
            import traceback
            tb_str = ''.join(traceback.format_exception(type(conn_error), conn_error, conn_error.__traceback__))
            error_details.append(f"\n상세 스택 트레이스:\n{tb_str}")
            
            # 로깅 (서버 로그에 기록)
            logger.error(f"PostgreSQL 연결 오류 상세: {error_details}")
            logger.error(f"예외 객체: {conn_error}")
            logger.error(f"예외 타입: {type(conn_error)}")
            logger.error(f"예외 속성: {dir(conn_error)}")
            logger.error(f"전체 traceback: {tb_str}")
            
            error_message = "\n".join(error_details)
            return ExecutePostgreSQLResult(
                success=False,
                error=f"데이터베이스 연결 오류:\n{error_message}"
            )
        except psycopg2.Error as pg_error:
            # 기타 PostgreSQL 오류
            error_message = str(pg_error) if str(pg_error) else '알 수 없는 PostgreSQL 오류'
            if hasattr(pg_error, 'pgerror') and pg_error.pgerror:
                error_message = pg_error.pgerror
            if hasattr(pg_error, 'pgcode') and pg_error.pgcode:
                error_message = f"[{pg_error.pgcode}] {error_message}"
            
            return ExecutePostgreSQLResult(
                success=False,
                error=f"PostgreSQL 오류: {error_message}"
            )
        except Exception as db_error:
            import traceback
            error_message = str(db_error) if str(db_error) else '알 수 없는 오류'
            return ExecutePostgreSQLResult(
                success=False,
                error=f"데이터베이스 오류: {error_message}\n{traceback.format_exc()}"
            )
            
    except ImportError as import_err:
        import traceback
        return ExecutePostgreSQLResult(
            success=False,
            error=f"psycopg2 패키지가 필요합니다. 'pip install psycopg2-binary' 명령을 실행해주세요.\n\n오류 상세: {str(import_err)}\n{traceback.format_exc()}"
        )
    except Exception as e:
        import traceback
        error_detail = f"SQL 실행 중 오류: {str(e)}\n{traceback.format_exc()}"
        return ExecutePostgreSQLResult(
            success=False,
            error=error_detail
        )

@app.websocket("/ws/chat")
async def websocket_chat(websocket: WebSocket):
    """WebSocket을 통한 채팅 스트리밍"""
    await websocket.accept()
    
    # 취소 가능한 작업을 추적하기 위한 Task 변수
    agent_task = None
    
    try:
        while True:
            # WebSocket에서 메시지 수신 (메시지 또는 취소 신호)
            try:
                data = await websocket.receive_json()
            except Exception as e:
                # JSON 파싱 실패 또는 연결 종료 시
                # WebSocketDisconnect 예외는 상위에서 처리되므로 여기서는 다른 예외만 처리
                if isinstance(e, WebSocketDisconnect):
                    raise
                # JSON 파싱 실패 시 다음 루프로
                continue
                
            # 취소 신호 확인
            if data.get("type") == "cancel":
                if agent_task and not agent_task.done():
                    agent_task.cancel()
                    try:
                        await agent_task  # 취소 완료 대기
                    except asyncio.CancelledError:
                        pass
                    await websocket.send_json({
                        "type": "cancelled",
                        "content": "작업이 취소되었습니다."
                    })
                continue
            
            message = data.get("message", "")
            prompt_type = data.get("prompt_type", "test")  # 기본값은 "test" (테스트 케이스 생성용)
            agent_type = data.get("agent_type", "semantic")  # "semantic", "smq", or "langgraph"
            llm_config = data.get("llm_config")  # LLM 설정 (vLLM 사용 시)
            
            if not message:
                continue
            
            # agent_type에 따라 적절한 에이전트 선택
            if agent_type == "smq":
                agent = SMQAgent(llm_config=llm_config)
                # load_smq_prompt는 app.py에 있으므로 직접 구현
                smq_prompt_file = os.path.join(os.path.dirname(__file__), 'prompts', 'smq_prompt.txt')
                if os.path.exists(smq_prompt_file):
                    with open(smq_prompt_file, 'r', encoding='utf-8') as f:
                        system_prompt_to_use = f.read()
                else:
                    system_prompt_to_use = "SMQ 프롬프트 파일을 찾을 수 없습니다."
            elif agent_type == "langgraph":
                # llm_config가 있으면 전달, 없으면 기본값 사용
                if llm_config:
                    agent = LangGraphAgent(llm_config=llm_config)
                else:
                    agent = LangGraphAgent()
                system_prompt_to_use = ""  # LangGraph 에이전트는 내부적으로 프롬프트를 관리
            else:
                agent = SemanticAgent(llm_config=llm_config)
                system_prompt_to_use = load_prompt(prompt_type)
            
            full_response = ""
            steps = []
            cancelled = False  # 각 요청마다 새로운 cancelled 변수 생성

            async def stream_text_as_deltas(text: str, chunk_size: int = 50, delay_s: float = 0.005):
                """
                프론트에서 '실시간'으로 보이도록 텍스트를 잘라 delta 이벤트로 전송합니다.
                (실제 LLM 토큰 스트리밍이 아니라, 완성된 텍스트를 chunk로 쪼개서 보내는 방식)
                chunk_size를 줄이고 delay를 줄여서 더 빠르게 보이도록 개선
                """
                if not text:
                    return
                for i in range(0, len(text), chunk_size):
                    if cancelled:
                        break
                    await websocket.send_json({"type": "delta", "content": text[i:i + chunk_size]})
                    # 이벤트 루프에 제어권을 잠깐 넘겨 브라우저 렌더가 자연스럽게 따라오도록 함
                    await asyncio.sleep(delay_s)
            
            # Agent 실행을 Task로 래핑하여 취소 가능하게 만들기
            error_occurred = False  # 에러 발생 여부 추적
            async def run_agent():
                nonlocal cancelled, full_response, steps, error_occurred
                try:
                    event_count = 0
                    async for event in agent.run(message, system_prompt=system_prompt_to_use):
                        event_count += 1
                        if cancelled:
                            break
                            
                        event_type = event.get("type")
                        content = event.get("content")
                        
                        # 디버깅: 모든 이벤트 로깅
                        import logging
                        logger = logging.getLogger("app")
                        logger.info(f"[WebSocket] 이벤트 수신: type={event_type}, content={content[:100] if content else None}")
                        
                        # WebSocket으로 메시지 전송 시 오류 처리
                        try:
                            if event_type == "prompt":
                                step = event.get("step", "")
                                await websocket.send_json({
                                    "type": "prompt",
                                    "content": content,
                                    "step": step
                                })
                            elif event_type == "thought":
                                step = event.get("step", "")
                                details = event.get("details")
                                steps.append({"type": "thought", "content": content, "details": details})
                                await websocket.send_json({
                                    "type": "thought",
                                    "content": content,
                                    "step": step,
                                    "details": details
                                })
                            elif event_type == "tool_call":
                                tool_name = event.get("tool", "Tool")
                                args = event.get("args", {})
                                details = event.get("details")
                                
                                steps.append({
                                    "type": "tool_call",
                                    "content": content,
                                    "tool": tool_name,
                                    "args": args,
                                    "details": details
                                })
                                
                                await websocket.send_json({
                                    "type": "tool_call",
                                    "content": content,
                                    "tool": tool_name,
                                    "args": args,
                                    "details": details
                                })
                            elif event_type == "tool_result":
                                step = event.get("step", "")
                                details = event.get("details")
                                steps.append({"type": "tool_result", "content": content, "details": details})
                                
                                await websocket.send_json({
                                    "type": "tool_result",
                                    "content": content,
                                    "step": step,
                                    "details": details
                                })
                            elif event_type == "error":
                                step = event.get("step", "")
                                import logging
                                logger = logging.getLogger("app")
                                logger.error(f"[WebSocket] 에러 이벤트 수신: step={step}, content={content}")
                                error_occurred = True  # 에러 발생 플래그 설정
                                await websocket.send_json({
                                    "type": "error",
                                    "content": content,
                                    "step": step
                                })
                                full_response += f"\n\nError: {content}"
                                # 에러 발생 시 즉시 종료
                                break
                            elif event_type == "success":
                                # success 이벤트를 먼저 전송하여 프론트엔드가 즉시 받을 수 있도록 함
                                await websocket.send_json({
                                    "type": "success",
                                    "content": content
                                })
                                # 그 다음 delta로 스트리밍하여 실시간처럼 보이도록 함
                                if content and not cancelled:
                                    await stream_text_as_deltas(content)
                                full_response = content
                            elif event_type == "complete":
                                # complete 이벤트 (LangGraph 에이전트의 최종 완료)
                                query_result = event.get("query_result")
                                sql_result = event.get("sql_result")
                                sql_query = event.get("sql_query")
                                smq = event.get("smq")
                                
                                await websocket.send_json({
                                    "type": "complete",
                                    "content": content,
                                    "query_result": query_result,  # executeQuery에서 생성한 예시 데이터
                                    "sql_result": sql_result,  # SQL 변환 결과
                                    "sql_query": sql_query,  # 생성된 SQL 쿼리
                                    "smq": smq  # 생성된 SMQ
                                })
                                if content and not cancelled:
                                    await stream_text_as_deltas(content)
                                full_response = content
                            elif event_type == "message":
                                # message 이벤트를 먼저 전송하여 프론트엔드가 즉시 받을 수 있도록 함
                                await websocket.send_json({
                                    "type": "message",
                                    "content": content
                                })
                                # 그 다음 delta로 스트리밍하여 실시간처럼 보이도록 함
                                if content and not cancelled:
                                    await stream_text_as_deltas(content)
                                full_response = content
                        except (WebSocketDisconnect, Exception) as ws_error:
                            # WebSocket 연결이 끊어졌거나 오류가 발생하면 중단
                            import logging
                            logger = logging.getLogger("app")
                            logger.error(f"[WebSocket] 메시지 전송 오류: {str(ws_error)}")
                            cancelled = True
                            break
                except Exception as e:
                    # run_agent 내부에서 예외 발생 시 에러 전송
                    import logging
                    import traceback
                    logger = logging.getLogger("app")
                    error_msg = f"Agent 실행 중 오류: {str(e)}"
                    logger.error(f"[WebSocket] run_agent 예외: {error_msg}\n{traceback.format_exc()}")
                    try:
                        await websocket.send_json({
                            "type": "error",
                            "content": error_msg,
                            "step": "unknown"
                        })
                    except Exception:
                        pass
                    cancelled = True
                except asyncio.CancelledError:
                    cancelled = True
                    raise
            
            # Agent 실행 Task 생성
            agent_task = asyncio.create_task(run_agent())
            
            try:
                await agent_task
            except asyncio.CancelledError:
                cancelled = True
                await websocket.send_json({
                    "type": "cancelled",
                    "content": "작업이 취소되었습니다."
                })
                continue
            except Exception as e:
                # Agent 실행 중 오류 발생
                import traceback
                error_traceback = traceback.format_exc()
                await websocket.send_json({
                    "type": "error",
                    "content": f"Agent 실행 오류: {str(e)}"
                })
                continue
            
            # 에러가 발생하지 않았고 취소되지 않은 경우에만 complete 이벤트 전송
            if not cancelled and not error_occurred:
                # 최종 메시지 전송
                await websocket.send_json({
                    "type": "complete",
                    "content": full_response or "Task completed.",
                    "steps": steps
                })
            
    except WebSocketDisconnect:
        # WebSocket 연결이 끊어지면 실행 중인 작업 취소
        if agent_task and not agent_task.done():
            agent_task.cancel()
        pass
    except Exception as e:
        # WebSocket이 아직 열려있는 경우에만 오류 메시지 전송
        try:
            await websocket.send_json({
                "type": "error",
                "content": f"Error: {str(e)}"
            })
        except Exception:
            # WebSocket이 이미 닫혀있으면 무시
            pass

@app.websocket("/ws/evaluation")
async def websocket_evaluation(websocket: WebSocket):
    """평가 진행 상황을 WebSocket으로 스트리밍"""
    await websocket.accept()
    
    try:
        # 초기 데이터 수신 (파일 업로드는 별도 엔드포인트에서 처리해야 하므로, 
        # 여기서는 평가 세션 ID나 파일 정보를 받음)
        data = await websocket.receive_json()
        
        # 파일은 별도 엔드포인트에서 업로드하고, 여기서는 평가 세션만 처리
        # 임시로 여기서 파일 정보를 받는 방식 사용
        temp_dir = data.get("temp_dir")
        criteria = data.get("criteria")
        file_paths_info = data.get("file_paths", [])
        
        if not temp_dir or not os.path.exists(temp_dir):
            await websocket.send_json({
                "type": "error",
                "content": "임시 디렉토리가 제공되지 않았습니다."
            })
            return
        
        # 평가 프롬프트 파일 로드
        prompt_file_path = os.path.join(os.path.dirname(__file__), 'prompts', 'evaluation_prompt.txt')
        if os.path.exists(prompt_file_path):
            with open(prompt_file_path, 'r', encoding='utf-8') as f:
                system_prompt = f.read()
        else:
            system_prompt = """당신은 NL2SQL 시스템의 논리적 무결성을 검증하는 Audit Agent입니다.
단계적으로 파일을 읽고 분석하여 평가를 수행하세요.

모든 답변은 반드시 지정된 JSON 형식으로만 출력하십시오.

Response Format:
{
  "reasoning": "현재 단계와 다음 행동 계획",
  "tool_call": {
    "tool": "readFile 또는 submitReport",
    "arguments": { ... }
  }
}
"""
        
        # 사용자 요청 구성
        user_request = f"""제출된 파일들을 평가해주세요.

평가 기준:
{criteria if criteria else "코드 품질, 알고리즘 이해도, 문제 해결 능력, 코드 구조 및 설계 등을 종합적으로 평가해주세요."}

제출된 파일/폴더:
{chr(10).join(file_paths_info)}
"""
        
        # EvaluationAgent 생성 및 실행
        agent = EvaluationAgent(temp_dir)
        
        evaluation_result = ""
        final_report = None
        
        async for event in agent.run_evaluation(system_prompt, user_request, max_turns=10):
            event_type = event.get("type")
            content = event.get("content", "")
            
            # 모든 이벤트를 WebSocket으로 전송
            await websocket.send_json({
                "type": event_type,
                "content": content,
                "tool": event.get("tool"),
                "args": event.get("args")
            })
            
            if event_type == "success":
                try:
                    report_data = json.loads(content)
                    final_report = report_data
                    evaluation_result = json.dumps(report_data, indent=2, ensure_ascii=False)
                except:
                    evaluation_result = content
            elif event_type == "thought":
                if evaluation_result:
                    evaluation_result += "\n\n[분석 과정]\n" + content
                else:
                    evaluation_result = "[분석 과정]\n" + content
            elif event_type == "error":
                await websocket.send_json({
                    "type": "error",
                    "content": content
                })
                return
        
        # 최종 리포트 전송
        if final_report and isinstance(final_report, dict):
            analysis = final_report.get('analysis', {})
            if isinstance(analysis, dict):
                question_difficulty = analysis.get('question_difficulty_analysis', 'N/A')
                relative_date_handling = analysis.get('relative_date_handling', 'N/A')
                semantic_alignment = analysis.get('semantic_alignment', 'N/A')
                nl2dsl_matching = analysis.get('nl2dsl_matching', 'N/A')
                prompt_compliance = analysis.get('prompt_compliance', 'N/A')
                model_alignment = analysis.get('model_alignment', 'N/A')
                detailed_findings = analysis.get('detailed_findings', 'N/A')
                score_breakdown = analysis.get('score_breakdown', 'N/A')
            else:
                question_difficulty = 'N/A'
                relative_date_handling = 'N/A'
                semantic_alignment = 'N/A'
                nl2dsl_matching = 'N/A'
                prompt_compliance = str(analysis) if analysis else 'N/A'
                model_alignment = 'N/A'
                detailed_findings = 'N/A'
                score_breakdown = 'N/A'
            
            score = final_report.get('score', 0)
            
            result_text = f"""# 평가 결과

## 점수: {score}점

## 점수 산정 내역
{score_breakdown}

---

## 1. 작성된 질문의 난이도 평가
{question_difficulty}

---

## 2. 프롬프트의 상대날짜 처리 능력 평가
{relative_date_handling}

---

## 3. 질문과 DSL의 의미적 일치 여부 평가
{semantic_alignment}

---

## 4. 질문과 NL2DSL 매칭 및 성공률 평가
{nl2dsl_matching}

---

## 프롬프트 지침 준수 여부
{prompt_compliance}

---

## 시맨틱 모델 정의 일치 여부
{model_alignment}

---

## 추가 발견 사항 및 개선 제안
{detailed_findings}

---

## 상세 분석 과정
{evaluation_result}
"""
        else:
            result_text = evaluation_result if evaluation_result else "평가 결과를 생성하지 못했습니다."
        
        await websocket.send_json({
            "type": "complete",
            "content": result_text
        })
        
    except WebSocketDisconnect:
        pass
    except Exception as e:
        import traceback
        await websocket.send_json({
            "type": "error",
            "content": f"평가 실행 중 오류: {str(e)}\n{traceback.format_exc()}"
        })

@app.post("/api/evaluation/upload")
async def upload_evaluation_files(
    files: List[UploadFile] = File(...),
    paths_json: str = Form(None)
):
    """평가용 파일 업로드 - 임시 디렉토리 생성 및 파일 저장"""
    if not files or len(files) == 0:
        raise HTTPException(status_code=400, detail="파일이 업로드되지 않았습니다.")
    
    # 경로 정보 파싱
    paths_list = []
    if paths_json:
        try:
            paths_data = json.loads(paths_json)
            paths_list = [item['path'] for item in sorted(paths_data, key=lambda x: x.get('index', 0))]
        except Exception as e:
            pass
    
    # 임시 디렉토리 생성
    temp_dir = tempfile.mkdtemp(prefix="evaluation_")
    file_paths_info = []
    
    try:
        for i, uploaded_file in enumerate(files):
            if i < len(paths_list) and paths_list[i]:
                file_path = paths_list[i]
            else:
                file_path = uploaded_file.filename
            
            full_path = os.path.join(temp_dir, file_path)
            dir_path = os.path.dirname(full_path)
            if dir_path and dir_path != temp_dir:
                os.makedirs(dir_path, exist_ok=True)
            
            with open(full_path, "wb") as buffer:
                shutil.copyfileobj(uploaded_file.file, buffer)
            
            file_paths_info.append(file_path.replace("\\", "/"))
        
        return {
            "success": True,
            "temp_dir": temp_dir,
            "file_paths": file_paths_info
        }
    except Exception as e:
        # 오류 발생 시 임시 디렉토리 정리
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=f"파일 업로드 실패: {str(e)}")

@app.post("/api/evaluation/start", response_model=EvaluationResult)
async def start_evaluation(
    files: List[UploadFile] = File(...),
    paths_json: str = Form(None),
    criteria: Optional[str] = Form(None)
):
    """지원자가 제출한 파일들을 LLM을 통해 평가 (Chain of Analysis 방식)"""
    if not files or len(files) == 0:
        raise HTTPException(status_code=400, detail="파일이 업로드되지 않았습니다.")
    
    # 경로 정보 파싱 (인덱스 기반)
    paths_list = []
    if paths_json:
        try:
            paths_data = json.loads(paths_json)
            paths_list = [item['path'] for item in sorted(paths_data, key=lambda x: x.get('index', 0))]
        except Exception as e:
            pass
    
    # 임시 디렉토리 생성
    temp_dir = tempfile.mkdtemp(prefix="evaluation_")
    
    try:
        # 업로드된 파일들을 임시 디렉토리에 폴더 구조 유지하며 저장
        file_paths_info = []
        
        for i, uploaded_file in enumerate(files):
            # 경로 정보 가져오기 (인덱스 기반)
            if i < len(paths_list) and paths_list[i]:
                file_path = paths_list[i]
            else:
                file_path = uploaded_file.filename
            
            # 폴더 구조 생성
            full_path = os.path.join(temp_dir, file_path)
            dir_path = os.path.dirname(full_path)
            if dir_path and dir_path != temp_dir:
                os.makedirs(dir_path, exist_ok=True)
            
            # 파일 저장
            with open(full_path, "wb") as buffer:
                shutil.copyfileobj(uploaded_file.file, buffer)
            
            file_paths_info.append(file_path.replace("\\", "/"))  # 경로 정규화
        
        # 평가 프롬프트 파일 로드
        prompt_file_path = os.path.join(os.path.dirname(__file__), 'prompts', 'evaluation_prompt.txt')
        
        if os.path.exists(prompt_file_path):
            with open(prompt_file_path, 'r', encoding='utf-8') as f:
                system_prompt = f.read()
        else:
            # 기본 프롬프트
            system_prompt = """당신은 NL2SQL 시스템의 논리적 무결성을 검증하는 Audit Agent입니다.
단계적으로 파일을 읽고 분석하여 평가를 수행하세요.

모든 답변은 반드시 지정된 JSON 형식으로만 출력하십시오.

Response Format:
{
  "reasoning": "현재 단계와 다음 행동 계획",
  "tool_call": {
    "tool": "readFile 또는 submitReport",
    "arguments": { ... }
  }
}
"""
        
        # 사용자 요청 구성
        user_request = f"""제출된 파일들을 평가해주세요.

평가 기준:
{criteria if criteria else "코드 품질, 알고리즘 이해도, 문제 해결 능력, 코드 구조 및 설계 등을 종합적으로 평가해주세요."}

제출된 파일/폴더:
{chr(10).join(file_paths_info)}
"""
        
        # EvaluationAgent 생성 및 실행
        agent = EvaluationAgent(temp_dir)
        
        evaluation_result = ""
        final_report = None
        
        async for event in agent.run_evaluation(system_prompt, user_request, max_turns=10):
            event_type = event.get("type")
            content = event.get("content", "")
            
            if event_type == "success":
                # 최종 리포트 받음
                try:
                    report_data = json.loads(content)
                    final_report = report_data
                    evaluation_result = json.dumps(report_data, indent=2, ensure_ascii=False)
                except:
                    evaluation_result = content
            elif event_type == "tool_result":
                # Tool 결과는 내부적으로 처리
                pass
            elif event_type == "thought":
                # Reasoning 출력
                if evaluation_result:
                    evaluation_result += "\n\n[분석 과정]\n" + content
                else:
                    evaluation_result = "[분석 과정]\n" + content
            elif event_type == "error":
                raise HTTPException(status_code=500, detail=f"평가 중 오류 발생: {content}")
        
        # 최종 리포트가 있으면 사용, 없으면 평가 결과 사용
        if final_report and isinstance(final_report, dict):
            analysis = final_report.get('analysis', {})
            if isinstance(analysis, dict):
                question_difficulty = analysis.get('question_difficulty_analysis', 'N/A')
                relative_date_handling = analysis.get('relative_date_handling', 'N/A')
                semantic_alignment = analysis.get('semantic_alignment', 'N/A')
                nl2dsl_matching = analysis.get('nl2dsl_matching', 'N/A')
                prompt_compliance = analysis.get('prompt_compliance', 'N/A')
                model_alignment = analysis.get('model_alignment', 'N/A')
                detailed_findings = analysis.get('detailed_findings', 'N/A')
                score_breakdown = analysis.get('score_breakdown', 'N/A')
            else:
                question_difficulty = 'N/A'
                relative_date_handling = 'N/A'
                semantic_alignment = 'N/A'
                nl2dsl_matching = 'N/A'
                prompt_compliance = analysis.get('prompt_compliance', 'N/A') if hasattr(analysis, 'get') else str(analysis)
                model_alignment = 'N/A'
                detailed_findings = 'N/A'
                score_breakdown = 'N/A'
            
            score = final_report.get('score', 0)
            
            result_text = f"""# 평가 결과

## 점수: {score}점

## 점수 산정 내역
{score_breakdown}

---

## 1. 작성된 질문의 난이도 평가
{question_difficulty}

---

## 2. 프롬프트의 상대날짜 처리 능력 평가
{relative_date_handling}

---

## 3. 질문과 DSL의 의미적 일치 여부 평가
{semantic_alignment}

---

## 4. 질문과 NL2DSL 매칭 및 성공률 평가
{nl2dsl_matching}

---

## 프롬프트 지침 준수 여부
{prompt_compliance}

---

## 시맨틱 모델 정의 일치 여부
{model_alignment}

---

## 추가 발견 사항 및 개선 제안
{detailed_findings}

---

## 상세 분석 과정
{evaluation_result}
"""
        else:
            result_text = evaluation_result if evaluation_result else "평가 결과를 생성하지 못했습니다."
        
        return EvaluationResult(
            success=True,
            result=result_text
        )
    
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_detail = f"평가 실행 중 오류: {str(e)}\n{traceback.format_exc()}"
        return EvaluationResult(
            success=False,
            error=error_detail
        )
    finally:
        # 임시 디렉토리 정리
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)

@app.post("/api/postprocess/test", response_model=EvaluationResult)
async def postprocess_test(request: PostProcessTestRequest):
    """후처리 테스트 - 데이터프레임 결과와 사용자 질문을 받아 LLM으로 처리"""
    try:
        # 후처리 프롬프트 파일 로드
        prompt_file_path = os.path.join(os.path.dirname(__file__), 'prompts', 'postprocess_prompt.txt')
        
        if os.path.exists(prompt_file_path):
            with open(prompt_file_path, 'r', encoding='utf-8') as f:
                prompt_template = f.read()
        else:
            return EvaluationResult(
                success=False,
                error="후처리 프롬프트 파일을 찾을 수 없습니다."
            )
        
        # 변수 치환
        from datetime import datetime
        today = datetime.now().strftime("%Y%m%d")
        data_base_date = datetime.now().strftime("%Y-%m-%d")
        
        prompt = prompt_template.replace("{today}", today)
        prompt = prompt.replace("{data_base_date}", data_base_date)
        prompt = prompt.replace("{result_df}", request.dataframe_result)
        prompt = prompt.replace("{user_question}", request.user_question)
        
        # LLM 호출
        llm_config = request.llm_config
        
        if llm_config and llm_config.get("model_type") == "vllm":
            # vLLM 사용
            base_url = llm_config.get("url", "http://localhost:8000/v1")
            if not base_url.endswith("/v1"):
                base_url = base_url.rstrip("/") + "/v1"
            model_name = llm_config.get("model_name", "gpt-4o")
            temperature = llm_config.get("temperature", 0.1)
            max_tokens = llm_config.get("max_tokens", 1000)
            api_key = os.getenv("OPENAI_API_KEY", "dummy-key-for-vllm")
            client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        else:
            # OpenAI 사용
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                return EvaluationResult(
                    success=False,
                    error="OPENAI_API_KEY가 설정되지 않았습니다."
                )
            client = AsyncOpenAI(api_key=api_key)
            model_name = "gpt-4o"
            temperature = 0.0
            max_tokens = None
        
        try:
            create_params = {
                "model": model_name,
                "messages": [
                    {"role": "system", "content": "You are a data analysis expert who processes database query results. Return only executable DuckDB SQL query or 'pass' string."},
                    {"role": "user", "content": prompt}
                ]
            }
            if temperature is not None:
                create_params["temperature"] = temperature
            if max_tokens is not None:
                create_params["max_tokens"] = max_tokens
            
            response = await client.chat.completions.create(**create_params)
        except Exception as e:
            return EvaluationResult(
                success=False,
                error=f"LLM 호출 오류: {str(e)}"
            )
        
        content = response.choices[0].message.content
        if not content:
            return EvaluationResult(
                success=False,
                error="LLM 응답이 비어있습니다."
            )
        
        # 마크다운 코드 블록 제거
        cleaned_content = content.strip()
        # ```sql 또는 ``` 로 시작하는 부분 제거
        if cleaned_content.startswith('```sql'):
            cleaned_content = cleaned_content[6:].lstrip()
        elif cleaned_content.startswith('```'):
            cleaned_content = cleaned_content[3:].lstrip()
        # 끝의 ``` 제거
        if cleaned_content.endswith('```'):
            cleaned_content = cleaned_content[:-3].rstrip()
        
        # 결과 반환 (SQL 또는 "pass")
        return EvaluationResult(
            success=True,
            result=cleaned_content
        )
    
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_detail = f"후처리 테스트 실행 중 오류: {str(e)}\n{traceback.format_exc()}"
        return EvaluationResult(
            success=False,
            error=error_detail
        )

@app.post("/api/postprocess/execute", response_model=ExecuteSQLResult)
async def execute_postprocess_sql(request: ExecuteSQLRequest):
    """후처리 SQL 실행 - DuckDB를 사용하여 SQL 실행"""
    try:
        import duckdb
        import pandas as pd
        
        # 데이터프레임 생성
        df = pd.DataFrame(request.table_data)
        
        # DuckDB 연결
        conn = duckdb.connect()
        
        # 데이터프레임을 DuckDB 테이블로 등록
        conn.register(request.table_name, df)
        
        # SQL에서 마크다운 코드 블록 제거
        cleaned_sql = request.sql.strip()
        if cleaned_sql.startswith('```sql'):
            cleaned_sql = cleaned_sql[6:].lstrip()
        elif cleaned_sql.startswith('```'):
            cleaned_sql = cleaned_sql[3:].lstrip()
        if cleaned_sql.endswith('```'):
            cleaned_sql = cleaned_sql[:-3].rstrip()
        cleaned_sql = cleaned_sql.strip()
        
        # SQL 실행
        try:
            result = conn.execute(cleaned_sql).fetchdf()
            
            # 결과를 리스트로 변환
            columns = result.columns.tolist()
            rows = result.values.tolist()
            
            return ExecuteSQLResult(
                success=True,
                columns=columns,
                rows=rows
            )
        except Exception as sql_error:
            return ExecuteSQLResult(
                success=False,
                error=f"SQL 실행 오류: {str(sql_error)}"
            )
        finally:
            conn.close()
    
    except ImportError:
        return ExecuteSQLResult(
            success=False,
            error="duckdb와 pandas 패키지가 필요합니다. 'pip install duckdb pandas' 명령을 실행해주세요."
        )
    except Exception as e:
        import traceback
        error_detail = f"SQL 실행 중 오류: {str(e)}\n{traceback.format_exc()}"
        return ExecuteSQLResult(
            success=False,
            error=error_detail
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

