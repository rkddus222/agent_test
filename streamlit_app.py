
import streamlit as st
import os
import glob
from semantic_agent import SemanticAgent

st.set_page_config(page_title="Semantic Agent Playground", layout="wide")

st.title("Semantic Model Engineer Agent")

# Sidebar: File Explorer
st.sidebar.title("Playground Files")
playground_dir = os.path.join(os.path.dirname(__file__), 'playground')
files = glob.glob(os.path.join(playground_dir, "**/*"), recursive=True)

selected_file = st.sidebar.selectbox("View File", [os.path.relpath(f, playground_dir) for f in files if os.path.isfile(f)])

if selected_file:
    file_path = os.path.join(playground_dir, selected_file)
    with open(file_path, 'r', encoding='utf-8') as f:
        st.sidebar.code(f.read(), language='yaml')

# Helper to load/save prompt
PROMPT_FILE = os.path.join(os.path.dirname(__file__), 'system_prompt.txt')

def load_prompt():
    if os.path.exists(PROMPT_FILE):
        with open(PROMPT_FILE, 'r', encoding='utf-8') as f:
            return f.read()
    return ""

def save_prompt(content):
    with open(PROMPT_FILE, 'w', encoding='utf-8') as f:
        f.write(content)

# Tabs
tab1, tab2 = st.tabs(["Chat Interface", "Prompt Settings"])

with tab2:
    st.header("System Prompt Editor")
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
