
import os
import json
import yaml
from openai import OpenAI
from dotenv import load_dotenv
from tools import read_file, edit_file

load_dotenv()

class SemanticAgent:
    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.client = OpenAI(api_key=self.api_key) if self.api_key else None
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

    def run(self, user_request, system_prompt, max_turns=5):
        if not self.client:
            yield {"type": "error", "content": "OPENAI_API_KEY not set."}
            return

        self.tool_history = [] # Reset history for new request
        
        for turn in range(max_turns):
            directory_structure = self.get_directory_structure()
            tool_history_str = json.dumps(self.tool_history, indent=2, ensure_ascii=False)
            
            prompt = system_prompt.replace("{directory_structure}", directory_structure)\
                                  .replace("{user_request}", user_request)\
                                  .replace("{tool_history}", tool_history_str)
            
            try:
                response = self.client.chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {"role": "system", "content": "You are a helpful Semantic Model Engineer."},
                        {"role": "user", "content": prompt}
                    ],
                    response_format={ "type": "json_object" }
                )
                
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
                    else:
                        result = {"error": f"Unknown tool: {tool_name}"}
                    
                    # Log to history
                    self.tool_history.append({
                        "request": tool_call_data,
                        "response": result
                    })
                    
                    yield {"type": "tool_result", "content": json.dumps(result, indent=2, ensure_ascii=False)}
                    
                    if tool_name == "editFile" and result.get("success") and result.get("error_count", 0) == 0:
                         yield {"type": "success", "content": "Changes applied successfully without errors."}
                         return

                else:
                    # No tool call? Maybe finished or asking clarification?
                    yield {"type": "message", "content": content}
                    return

            except Exception as e:
                yield {"type": "error", "content": str(e)}
                break
