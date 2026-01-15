
import streamlit as st
import os
import glob
from backend.semantic_agent import SemanticAgent

st.set_page_config(page_title="Semantic Agent Playground", layout="wide")

st.title("Semantic Model Engineer Agent")

# Helper to save playground file
def save_playground_file(file_path, content):
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    except Exception as e:
        st.sidebar.error(f"파일 저장 실패: {str(e)}")
        return False

# Helper to create new playground file
def create_playground_file(file_name):
    if not file_name.endswith('.yml'):
        file_name += '.yml'
    file_path = os.path.join(os.path.dirname(__file__), 'playground', file_name)
    try:
        if os.path.exists(file_path):
            st.sidebar.error("이미 존재하는 파일명입니다.")
            return False
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write("# New Semantic Model\n")
        return True
    except Exception as e:
        st.sidebar.error(f"파일 생성 실패: {str(e)}")
        return False

# Helper to rename playground file
def rename_playground_file(old_path, new_filename):
    if not new_filename.endswith('.yml'):
        new_filename += '.yml'
    
    playground_dir = os.path.dirname(old_path)
    new_path = os.path.join(playground_dir, new_filename)
    
    try:
        if os.path.exists(new_path):
            st.sidebar.error("이미 존재하는 파일명입니다.")
            return False
        os.rename(old_path, new_path)
        return True
    except Exception as e:
        st.sidebar.error(f"이름 변경 실패: {str(e)}")
        return False

# Helper to delete playground file
def delete_playground_file(file_path):
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            return True
        else:
            st.sidebar.error("파일을 찾을 수 없습니다.")
            return False
    except Exception as e:
        st.sidebar.error(f"파일 삭제 실패: {str(e)}")
        return False

# Sidebar: File Explorer
st.sidebar.title("Playground Files")
playground_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'playground'))

# 파싱 기능
st.sidebar.markdown("---")
st.sidebar.subheader("🔧 Semantic Model 파싱")
if st.sidebar.button("📦 Manifest 생성", key="parse_btn", use_container_width=True):
    from backend.tools import parse_semantic_models
    with st.sidebar:
        with st.spinner("Semantic models를 파싱하는 중..."):
            result = parse_semantic_models(playground_dir)
            if result.get("success"):
                st.success("✅ 파싱 완료!")
                models_count = result.get("semantic_models_count", 0)
                metrics_count = result.get("metrics_count", 0)
                st.info(f"📊 {models_count}개 모델, {metrics_count}개 메트릭")
                manifest_path = result.get("manifest_path", "")
                if manifest_path:
                    st.caption(f"📄 {os.path.basename(manifest_path)}")
            else:
                error_msg = result.get('error', 'Unknown error')
                st.error(f"❌ 파싱 실패")
                st.caption(error_msg)
                # 상세 에러 정보가 있으면 표시
                if result.get("traceback"):
                    with st.expander("상세 에러 정보"):
                        st.code(result.get("traceback"), language="python")

# New File UI
with st.sidebar.expander("➕ 새 파일 만들기", expanded=False):
    create_mode = st.radio("생성 방식", ["빈 파일 생성", "DDL에서 자동 생성"], key="create_mode")
    
    if create_mode == "빈 파일 생성":
        new_filename = st.text_input("파일 이름 (예: model.yml)", key="new_file_name")
        if st.button("파일 생성", key="create_file_btn"):
            if new_filename:
                if create_playground_file(new_filename):
                    st.sidebar.success(f"{new_filename} 파일이 생성되었습니다!")
                    st.rerun()
            else:
                st.sidebar.warning("파일 이름을 입력해주세요.")
    else:
        # DDL 입력 모드
        ddl_dialect = st.selectbox(
            "DBMS 타입",
            ["bigquery", "mysql", "postgresql", "oracle", "mssql"],
            key="ddl_dialect"
        )
        ddl_text = st.text_area(
            "DDL 문 입력",
            height=200,
            placeholder="-- bigquery\nCREATE TABLE `project.dataset.table_name` (\n  `id` INTEGER NOT NULL,\n  `name` STRING,\n  `amount` DECIMAL(10, 2),\n  PRIMARY KEY (`id`)\n);",
            key="ddl_input"
        )
        
        if st.button("DDL에서 생성", key="create_from_ddl_btn"):
            if ddl_text.strip():
                try:
                    from backend.semantic.model_manager.utils.ddl_parser import parse_ddl_text
                    from backend.semantic.model_manager.utils.ddl_to_semantic_model import generate_semantic_model_from_ddl
                    
                    # DDL 파싱
                    with st.spinner("DDL을 파싱하는 중..."):
                        # dialect 주석이 없으면 선택한 dialect 추가
                        if not ddl_text.strip().startswith('--'):
                            ddl_text = f"-- {ddl_dialect}\n{ddl_text}"
                        
                        tables = parse_ddl_text(ddl_text, ddl_dialect)
                        
                        if not tables:
                            st.sidebar.error("DDL에서 테이블을 찾을 수 없습니다.")
                        else:
                            # 첫 번째 테이블 사용
                            table_name, table_info = next(iter(tables.items()))
                            
                            # Semantic Model 생성
                            yml_content = generate_semantic_model_from_ddl(
                                table_info,
                                table_name_in_db=table_name,
                                database=table_info.database,
                                schema=table_info.schema
                            )
                            
                            # 파일명 생성 (테이블명.yml)
                            file_name = f"{table_name.lower()}.yml"
                            file_path = os.path.join(playground_dir, "semantic_models", file_name)
                            
                            # 디렉토리 생성 (없는 경우)
                            os.makedirs(os.path.dirname(file_path), exist_ok=True)
                            
                            # 파일 저장 (존재하면 덮어쓰기)
                            overwritten = os.path.exists(file_path)
                            with open(file_path, 'w', encoding='utf-8') as f:
                                f.write(yml_content)
                            
                            if overwritten:
                                st.sidebar.warning(f"{file_name} 파일이 덮어쓰기되었습니다!")
                            else:
                                st.sidebar.success(f"{file_name} 파일이 생성되었습니다!")
                            st.rerun()
                                
                except Exception as e:
                    st.sidebar.error(f"오류 발생: {str(e)}")
                    import traceback
                    with st.expander("상세 에러 정보"):
                        st.code(traceback.format_exc(), language='python')
            else:
                st.sidebar.warning("DDL 문을 입력해주세요.")

files = glob.glob(os.path.join(playground_dir, "**/*"), recursive=True)

selected_file_rel = st.sidebar.selectbox("View File", [os.path.relpath(f, playground_dir) for f in files if os.path.isfile(f)])

if selected_file_rel:
    file_path = os.path.join(playground_dir, selected_file_rel)
    
    # 편집 모드 및 삭제 버튼
    col_edit1, col_edit2 = st.sidebar.columns([1, 1])
    with col_edit1:
        edit_mode = st.checkbox("편집 모드", key="edit_mode")
    with col_edit2:
        if st.button("🚨 영구 삭제", key="delete_file_btn", use_container_width=True):
            if delete_playground_file(file_path):
                st.sidebar.success("삭제됨")
                st.rerun()
    
    with open(file_path, 'r', encoding='utf-8') as f:
        file_content = f.read()
    
    if edit_mode:
        # 파일 이름 수정 UI
        st.sidebar.markdown("---")
        st.sidebar.subheader("파일 이름 수정")
        new_file_name_input = st.sidebar.text_input("새 파일 이름:", value=os.path.basename(file_path), key="rename_input")
        if st.sidebar.button("📝 이름 변경", key="rename_btn"):
            if new_file_name_input and new_file_name_input != os.path.basename(file_path):
                if rename_playground_file(file_path, new_file_name_input):
                    st.sidebar.success("파일 이름이 변경되었습니다!")
                    st.rerun()
        
        st.sidebar.markdown("---")
        # 편집 가능한 텍스트 영역
        edited_content = st.sidebar.text_area(
            "파일 내용 편집:",
            value=file_content,
            height=400,
            key="file_editor"
        )
        
        # 저장 버튼
        col1, col2 = st.sidebar.columns(2)
        with col1:
            if st.button("💾 저장", key="save_file"):
                if save_playground_file(file_path, edited_content):
                    st.sidebar.success("파일이 저장되었습니다!")
                    st.rerun()
        with col2:
            if st.button("🔄 되돌리기", key="reset_file"):
                st.rerun()
    else:
        # 읽기 전용 모드
        st.sidebar.code(file_content, language='yaml')

# Helper to load/save prompt
PROMPT_FILE = os.path.join(os.path.dirname(__file__), 'prompts', 'system_prompt.txt')

def load_prompt():
    """프롬프트를 불러옵니다."""
    if os.path.exists(PROMPT_FILE):
        with open(PROMPT_FILE, 'r', encoding='utf-8') as f:
            return f.read()
    return ""

def save_prompt(content):
    """프롬프트를 저장합니다."""
    with open(PROMPT_FILE, 'w', encoding='utf-8') as f:
        f.write(content)

# Tabs
tab1, tab2 = st.tabs(["Chat Interface", "Prompt Settings"])

with tab2:
    st.header("System Prompt Editor")
    
    # 프롬프트 편집기
    current_prompt = load_prompt()
    new_prompt = st.text_area("Edit the system prompt here:", value=current_prompt, height=400)
    if st.button("Save Prompt"):
        save_prompt(new_prompt)
        st.success("System prompt saved!")

with tab1:
    # Chat Interface
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            # Render intermediate steps if present
            if "steps" in message and message["steps"]:
                for step in message["steps"]:
                    if step["type"] == "thought":
                        with st.expander("Reasoning", expanded=False):
                            st.write(step["content"])
                    elif step["type"] == "tool_call":
                        tool_name = step.get("tool", "Tool")
                        with st.status(f"Executing Tool: {tool_name}", state="complete"):
                            if "args" in step:
                                st.json(step["args"])
                            else:
                                st.write(step["content"])
                    elif step["type"] == "tool_result":
                        # Try to parse content as JSON to see if there is a diff
                        import json
                        try:
                            # It should be a JSON string from semantic_agent.py
                            if isinstance(step["content"], str):
                                result_data = json.loads(step["content"])
                            else:
                                result_data = step["content"]
                            
                            if isinstance(result_data, dict) and "diff" in result_data and result_data["diff"]:
                                st.code(result_data["diff"], language='diff')
                        except Exception as e:
                            # If parsing fails or diff not present, just ignore
                            pass
                            
                        with st.expander("Tool Result", expanded=False):
                             st.code(step["content"], language='json')
            
            st.markdown(message["content"])

    if prompt := st.chat_input("What would you like to change in the model?"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            agent = SemanticAgent()
            
            # Load the latest prompt from file
            system_prompt_to_use = load_prompt()
            
            full_response = ""
            steps = []
            
            # Run agent and stream specific updates
            for event in agent.run(prompt, system_prompt=system_prompt_to_use):
                event_type = event.get("type")
                content = event.get("content")
                
                if event_type == "thought":
                    steps.append({"type": "thought", "content": content})
                    with st.expander("Reasoning", expanded=True):
                        st.write(content)
                elif event_type == "tool_call":
                    tool_name = event.get("tool", "Tool")
                    args = event.get("args", {})
                    
                    steps.append({
                        "type": "tool_call", 
                        "content": content, 
                        "tool": tool_name,
                        "args": args
                    }) 
                    
                    with st.status(f"Executing Tool: {tool_name}...", expanded=True) as status:
                        st.json(args)
                        
                elif event_type == "tool_result":
                    steps.append({"type": "tool_result", "content": content})
                    
                    # Render Diff if available
                    import json
                    try:
                        result_data = json.loads(content)
                        if "diff" in result_data and result_data["diff"]:
                            st.code(result_data["diff"], language='diff')
                    except:
                        pass

                    with st.expander("Tool Result"):
                        st.code(content, language='json')
                elif event_type == "error":
                    st.error(content)
                    full_response += f"\n\nError: {content}"
                elif event_type == "success":
                    st.success(content)
                    full_response = content # Capture success message as final response
                elif event_type == "message":
                    st.markdown(content)
                    full_response = content
            
            # Save both the final content and the intermediate steps
            st.session_state.messages.append({
                "role": "assistant", 
                "content": full_response or "Task completed.",
                "steps": steps
            })
            
            # Rerun to update sidebar file content immediately if changed
            st.rerun()
