"""SMQ 생성 에이전트 - LLM 기반 tool calling"""
import os
import json
import asyncio
from openai import AsyncOpenAI
from dotenv import load_dotenv

load_dotenv()

class SMQAgent:
    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.client = AsyncOpenAI(api_key=self.api_key) if self.api_key else None
        self.tool_history = []
        self.playground_dir = os.path.join(os.path.dirname(__file__), 'playground')
        self.semantic_models_dir = os.path.join(self.playground_dir, 'semantic_models')

    def get_available_semantic_model_files(self):
        """사용 가능한 시멘틱 모델 파일 목록 반환"""
        files = []
        if os.path.exists(self.semantic_models_dir):
            for filename in os.listdir(self.semantic_models_dir):
                if filename.endswith('.yml') or filename.endswith('.yaml'):
                    model_name = filename.replace('.yml', '').replace('.yaml', '')
                    files.append({
                        "filename": filename,
                        "model_name": model_name
                    })
        return files

    async def run(self, user_query, system_prompt, max_turns=10):
        """LLM 기반 tool calling 실행"""
        if not self.client:
            yield {"type": "error", "content": "OPENAI_API_KEY not set."}
            return

        self.tool_history = []
        available_files = self.get_available_semantic_model_files()
        # 파일 목록을 더 명확하게 표시
        file_list_str = "사용 가능한 시멘틱 모델 파일 목록:\n"
        for f in available_files:
            file_list_str += f"  - {f['filename']} (모델명: {f['model_name']})\n"
        file_list_str += "\n위 목록에서 사용자 질문과 관련 있는 파일을 선택하세요."

        for turn in range(max_turns):
            tool_history_str = json.dumps(self.tool_history, indent=2, ensure_ascii=False)
            
            prompt = system_prompt.replace("{user_query}", user_query)\
                                  .replace("{available_files}", file_list_str)\
                                  .replace("{tool_history}", tool_history_str)

            try:
                # 디버깅: 프롬프트 확인
                print(f"[DEBUG SMQAgent] ========== LLM 호출 ==========")
                print(f"[DEBUG SMQAgent] 프롬프트 길이: {len(prompt)}")
                print(f"[DEBUG SMQAgent] 프롬프트에 getRandomYmlFile 포함: {'getRandomYmlFile' in prompt}")
                print(f"[DEBUG SMQAgent] 프롬프트에 SemanticModelSelector 포함: {'SemanticModelSelector' in prompt}")
                print(f"[DEBUG SMQAgent] 프롬프트 처음 800자:\n{prompt[:800]}")
                
                response = await self.client.chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {"role": "system", "content": "You are a helpful Semantic Model Query Agent. You MUST use only the tools listed in the prompt. Do NOT use getRandomYmlFile or generateSmqAndConvert."},
                        {"role": "user", "content": prompt}
                    ],
                    response_format={"type": "json_object"}
                )
                
                print(f"[DEBUG SMQAgent] LLM 응답 받음")
                print(f"[DEBUG SMQAgent] ==============================")
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

            reasoning = data.get("reasoning") or data.get("<reasoning>")
            if reasoning:
                yield {"type": "thought", "content": reasoning}

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

                result = await self._execute_tool(tool_name, args)
                
                self.tool_history.append({
                    "request": tool_call_data,
                    "response": result
                })

                # 에러가 발생한 경우 명확한 메시지 표시
                if isinstance(result, dict) and result.get("error"):
                    error_msg = result.get("error", "")
                    yield {"type": "error", "content": f"Tool 실행 오류: {error_msg}"}
                    yield {"type": "tool_result", "content": json.dumps(result, indent=2, ensure_ascii=False)}
                else:
                    yield {"type": "tool_result", "content": json.dumps(result, indent=2, ensure_ascii=False)}

                # 완료 조건 확인
                if self._is_complete(result, tool_name):
                    yield {"type": "success", "content": "SMQ 생성 및 SQL 변환이 완료되었습니다."}
                    return
            else:
                # Tool 호출이 없으면 완료로 간주
                yield {"type": "message", "content": content}
                return

    async def _execute_tool(self, tool_name, args):
        """Tool 실행"""
        # 사용 불가능한 tool에 대한 명확한 에러 메시지
        if tool_name == "getRandomYmlFile":
            return {"error": "getRandomYmlFile tool은 사용할 수 없습니다. SemanticModelSelector.selectSemanticModelFiles tool을 사용하여 사용자 질문과 관련 있는 파일을 선택하세요."}
        elif tool_name == "generateSmqAndConvert":
            return {"error": "generateSmqAndConvert tool은 사용할 수 없습니다. SemanticModelQuery.convertSmqToSql tool을 사용하세요."}
        elif tool_name == "SemanticModelSelector.selectSemanticModelFiles":
            return await self._select_semantic_model_files(args.get("userQuery", ""))
        elif tool_name == "SemanticLayer.readSemanticModelFile":
            # 이 tool은 더 이상 사용하지 않지만, 하위 호환성을 위해 유지
            return await self._read_semantic_model_file(args.get("filename", ""))
        elif tool_name == "SemanticLayer.getModelDataElements":
            # 이 tool은 더 이상 사용하지 않지만, 하위 호환성을 위해 유지
            return await self._get_model_data_elements(
                args.get("searchQuery", ""),
                args.get("semanticModel", [])
            )
        elif tool_name == "SemanticModelQuery.convertSmqToSql":
            return await self._convert_smq_to_sql(args.get("smq", []))
        elif tool_name == "SemanticModelQuery.editSmq":
            return await self._edit_smq(args.get("smqEdits", []))
        else:
            return {"error": f"Unknown tool: {tool_name}. 사용 가능한 tool은 SemanticModelSelector.selectSemanticModelFiles, SemanticModelQuery.convertSmqToSql, SemanticModelQuery.editSmq 입니다."}

    async def _select_semantic_model_files(self, user_query):
        """시멘틱 모델 파일 선택 및 내용 읽기 - 사용자 질문 기반으로 관련 파일 선택 후 내용 반환"""
        available_files = self.get_available_semantic_model_files()
        
        # 사용자 질문을 기반으로 관련 파일 선택 (간단한 키워드 매칭)
        user_query_lower = user_query.lower() if user_query else ""
        selected_files = []
        
        # 키워드 매칭으로 관련 파일 선택
        keyword_mapping = {
            "고객": ["고객기본", "고객"],
            "직원": ["직원정보", "직원"],
            "부점": ["부점코드", "부점"],
            "계좌": ["수신계좌", "여신계좌", "계좌"],
            "상품": ["상품기본", "상품"],
            "거래": ["거래", "transaction"],
            "customer": ["고객기본", "고객"],
            "branch": ["부점코드", "부점"],
            "account": ["수신계좌", "여신계좌", "계좌"],
            "product": ["상품기본", "상품"]
        }
        
        # 키워드 매칭으로 파일 선택
        for file_info in available_files:
            filename_lower = file_info["filename"].lower()
            model_name_lower = file_info["model_name"].lower()
            
            # 직접 파일명이나 모델명에 키워드가 포함되어 있는지 확인
            matched = False
            for keyword, related_terms in keyword_mapping.items():
                if keyword in user_query_lower:
                    for term in related_terms:
                        if term in filename_lower or term in model_name_lower:
                            matched = True
                            break
                    if matched:
                        break
            
            # 파일명이나 모델명이 질문에 직접 포함되어 있는지 확인
            if not matched:
                for part in filename_lower.replace('.yml', '').replace('.yaml', '').split('_'):
                    if part and part in user_query_lower:
                        matched = True
                        break
                if not matched and model_name_lower in user_query_lower:
                    matched = True
            
            if matched and file_info not in selected_files:
                selected_files.append(file_info)
        
        # 매칭된 파일이 없으면 모든 파일 반환 (LLM이 선택하도록)
        if not selected_files:
            selected_files = available_files
        
        # 선택된 파일들의 내용 읽기
        files_content = []
        for file_info in selected_files:
            filename = file_info["filename"]
            file_path = os.path.join(self.semantic_models_dir, filename)
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    files_content.append({
                        "filename": filename,
                        "model_name": file_info["model_name"],
                        "path": file_path,
                        "content": content
                    })
                except Exception as e:
                    files_content.append({
                        "filename": filename,
                        "model_name": file_info["model_name"],
                        "error": f"파일 읽기 오류: {str(e)}"
                    })
        
        return {
            "selected_files": [{"filename": f["filename"], "model_name": f["model_name"]} for f in files_content],
            "files_content": files_content,
            "all_available_files": available_files
        }

    async def _read_semantic_model_file(self, filename):
        """시멘틱 모델 파일 내용 읽기 (단일 파일 또는 여러 파일)"""
        try:
            # filename이 리스트인 경우 여러 파일 읽기
            if isinstance(filename, list):
                files = []
                errors = []
                for fn in filename:
                    file_path = os.path.join(self.semantic_models_dir, fn)
                    if not os.path.exists(file_path):
                        errors.append(f"파일을 찾을 수 없습니다: {fn}")
                        continue
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                        files.append({
                            "filename": fn,
                            "path": file_path,
                            "content": content
                        })
                    except Exception as e:
                        errors.append(f"파일 읽기 오류 ({fn}): {str(e)}")
                
                result = {"files": files}
                if errors:
                    result["errors"] = errors
                return result
            else:
                # 단일 파일 읽기
                file_path = os.path.join(self.semantic_models_dir, filename)
                if not os.path.exists(file_path):
                    return {"error": f"파일을 찾을 수 없습니다: {filename}"}
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                return {
                    "filename": filename,
                    "path": file_path,
                    "content": content
                }
        except Exception as e:
            return {"error": str(e)}

    async def _get_model_data_elements(self, search_query, semantic_models):
        """모델 요소 조회"""
        try:
            from backend.semantic.services.smq2sql_service import prepare_smq_to_sql
            
            manifest_path = os.path.join(self.playground_dir, "semantic_manifest.json")
            if not os.path.exists(manifest_path):
                return {"error": "semantic_manifest.json을 찾을 수 없습니다."}
            
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = json.load(f)
            
            semantic_models_list = manifest.get("semantic_models", [])
            metrics = manifest.get("metrics", [])
            
            results = []
            for model_name in semantic_models:
                model = next((m for m in semantic_models_list if m.get("name") == model_name), None)
                if not model:
                    continue
                
                model_metrics = []
                search_query_lower = search_query.lower()
                
                for metric in metrics:
                    metric_name = metric.get("name", "").lower()
                    metric_desc = str(metric.get("description", "")).lower()
                    
                    if not search_query or search_query_lower in metric_name or search_query_lower in metric_desc:
                        model_metrics.append({
                            "name": metric.get("name"),
                            "description": metric.get("description", "")
                        })
                
                dimensions = []
                for dim in model.get("dimensions", []):
                    dim_name = dim.get("name", "").lower()
                    dim_desc = str(dim.get("description", "")).lower()
                    
                    if not search_query or search_query_lower in dim_name or search_query_lower in dim_desc:
                        dimensions.append({
                            "name": dim.get("name"),
                            "description": dim.get("description", "")
                        })
                
                results.append({
                    "model": model_name,
                    "metrics": model_metrics,
                    "dimensions": dimensions
                })
            
            return {"results": results}
        except Exception as e:
            return {"error": str(e)}

    async def _convert_smq_to_sql(self, smq_list):
        """SMQ를 SQL로 변환"""
        try:
            print(f"[DEBUG SMQAgent] _convert_smq_to_sql 시작 - smq_list 개수: {len(smq_list) if isinstance(smq_list, list) else 1}")
            from backend.semantic.services.smq2sql_service import prepare_smq_to_sql
            
            manifest_path = os.path.join(self.playground_dir, "semantic_manifest.json")
            if not os.path.exists(manifest_path):
                print(f"[DEBUG SMQAgent] manifest 파일을 찾을 수 없음: {manifest_path}")
                return {"error": "semantic_manifest.json을 찾을 수 없습니다."}
            
            print(f"[DEBUG SMQAgent] manifest 파일 읽기 시작: {manifest_path}")
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest_content = f.read()
            print(f"[DEBUG SMQAgent] manifest 파일 읽기 완료 - 길이: {len(manifest_content)}")
            
            smq_state = []
            for index, smq in enumerate(smq_list):
                print(f"[DEBUG SMQAgent] SMQ 변환 시작 (index: {index}): {json.dumps(smq, ensure_ascii=False)[:200]}")
                try:
                    # 동기 함수를 async context에서 실행하기 위해 asyncio.to_thread 사용
                    import asyncio
                    print(f"[DEBUG SMQAgent] asyncio.to_thread로 SMQ 변환 시작 (index: {index})...")
                    
                    # 타임아웃 설정 (60초)
                    result = await asyncio.wait_for(
                        asyncio.to_thread(
                            prepare_smq_to_sql,
                            smq=smq,
                            manifest_content=manifest_content,
                            dialect="bigquery",
                            cte=True
                        ),
                        timeout=60.0
                    )
                    print(f"[DEBUG SMQAgent] SMQ 변환 완료 (index: {index}), success: {result.get('success')}")
                    
                    if result.get('success'):
                        queries = result.get('results', {}).get('queries', [])
                        if queries:
                            sql = queries[0].get('query', '')
                            smq_state.append({
                                "smq": smq,
                                "index": index,
                                "smqToSqlResult": sql
                            })
                            print(f"[DEBUG SMQAgent] SQL 생성 성공 (index: {index}), SQL 길이: {len(sql)}")
                        else:
                            print(f"[DEBUG SMQAgent] 쿼리 생성 실패 - queries가 비어있음 (index: {index})")
                            smq_state.append({
                                "smq": smq,
                                "index": index,
                                "smqToSqlResult": "Error: 쿼리 생성 실패"
                            })
                    else:
                        error_msg = result.get('error', 'Unknown error')
                        print(f"[DEBUG SMQAgent] SMQ 변환 실패 (index: {index}): {error_msg}")
                        smq_state.append({
                            "smq": smq,
                            "index": index,
                            "smqToSqlResult": f"Error: {error_msg}"
                        })
                except asyncio.TimeoutError:
                    error_msg = "SMQ 변환 시간 초과 (60초 이상 소요)"
                    print(f"[DEBUG SMQAgent] SMQ 변환 타임아웃 (index: {index}): {error_msg}")
                    smq_state.append({
                        "smq": smq,
                        "index": index,
                        "smqToSqlResult": f"Error: {error_msg}"
                    })
                except Exception as e:
                    import traceback
                    error_trace = traceback.format_exc()
                    print(f"[DEBUG SMQAgent] SMQ 변환 중 예외 발생 (index: {index}): {str(e)}")
                    print(f"[DEBUG SMQAgent] Traceback: {error_trace}")
                    smq_state.append({
                        "smq": smq,
                        "index": index,
                        "smqToSqlResult": f"Error: {str(e)}"
                    })
            
            print(f"[DEBUG SMQAgent] _convert_smq_to_sql 완료 - smqState 개수: {len(smq_state)}")
            return {"smqState": smq_state}
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            print(f"[DEBUG SMQAgent] _convert_smq_to_sql 예외 발생: {str(e)}")
            print(f"[DEBUG SMQAgent] Traceback: {error_trace}")
            return {"error": str(e)}

    async def _edit_smq(self, smq_edits):
        """SMQ 편집 - tool_history에서 이전 smqState를 가져와 편집 후 재변환"""
        try:
            # tool_history에서 가장 최근의 convertSmqToSql 결과를 찾아 smqState 가져오기
            current_smq_state = None
            for history_entry in reversed(self.tool_history):
                response = history_entry.get("response", {})
                if "smqState" in response:
                    current_smq_state = response["smqState"]
                    break
            
            if not current_smq_state:
                return {"error": "편집할 SMQ를 찾을 수 없습니다. 먼저 convertSmqToSql을 실행해주세요."}
            
            # smq_edits를 적용하여 SMQ 수정
            edited_smq_list = []
            
            # 현재 smqState를 딕셔너리로 변환 (index를 키로 사용)
            smq_dict = {item["index"]: item["smq"].copy() for item in current_smq_state}
            
            # 각 편집 항목 처리
            for edit in smq_edits:
                index = edit.get("index")
                if index not in smq_dict:
                    return {"error": f"인덱스 {index}에 해당하는 SMQ를 찾을 수 없습니다."}
                
                smq = smq_dict[index]
                
                # set: 전체 교체
                if "set" in edit and edit["set"]:
                    for key, value in edit["set"].items():
                        smq[key] = value
                
                # add: 배열에 추가
                if "add" in edit and edit["add"]:
                    for key, values in edit["add"].items():
                        if key in smq:
                            if isinstance(smq[key], list):
                                # 중복 제거를 위해 set 사용 후 다시 list로
                                existing = set(smq[key])
                                new_values = set(values) if isinstance(values, list) else {values}
                                smq[key] = list(existing | new_values)
                            else:
                                smq[key] = values
                        else:
                            smq[key] = values if isinstance(values, list) else [values]
                
                # remove: 배열에서 제거
                if "remove" in edit and edit["remove"]:
                    for key, values in edit["remove"].items():
                        if key in smq:
                            if isinstance(smq[key], list):
                                remove_set = set(values) if isinstance(values, list) else {values}
                                smq[key] = [v for v in smq[key] if v not in remove_set]
                            elif key == "limit" and values is None:
                                smq[key] = None
                            else:
                                # 해당 키를 제거하거나 None으로 설정
                                if values is None:
                                    smq[key] = None
                                else:
                                    smq.pop(key, None)
                
                smq_dict[index] = smq
            
            # 편집된 SMQ 리스트 생성 (index 순서대로)
            edited_smq_list = [smq_dict[idx] for idx in sorted(smq_dict.keys())]
            
            # 편집된 SMQ를 다시 SQL로 변환
            return await self._convert_smq_to_sql(edited_smq_list)
            
        except Exception as e:
            return {"error": f"SMQ 편집 중 오류 발생: {str(e)}"}

    def _is_complete(self, result, tool_name):
        """완료 조건 확인"""
        if tool_name == "SemanticModelQuery.convertSmqToSql":
            if result.get("smqState"):
                # 모든 SMQ가 성공적으로 변환되었는지 확인
                for state in result.get("smqState", []):
                    if state.get("smqToSqlResult", "").startswith("Error"):
                        return False
                return True
        return False
