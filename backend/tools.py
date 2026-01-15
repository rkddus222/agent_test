
import os
import yaml
import difflib
import asyncio
import random
import json
import glob
from pathlib import Path
# Import hook을 먼저 로드하여 service.semantic -> semantic 매핑
try:
    import import_hook
except ImportError:
    pass  # import_hook이 없어도 동작하도록

def read_file(path):
    """
    Reads the content of a file.
    Args:
        path (str): Relative path to the file from the playground directory.
    Returns:
        dict: A dictionary containing the content of the file.
        JSON files are returned in pretty-printed format.
    """
    # Restrict access to playground directory for safety
    base_dir = os.path.join(os.path.dirname(__file__), 'playground')
    target_path = os.path.join(base_dir, path)
    
    if not os.path.exists(target_path):
        return {"error": f"File not found: {path}"}
    
    try:
        # JSON 파일인지 확인
        if path.lower().endswith('.json'):
            with open(target_path, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    # Pretty print JSON
                    content = json.dumps(data, indent=2, ensure_ascii=False)
                    return {"content": content}
                except json.JSONDecodeError:
                    # JSON 파싱 실패 시 원본 내용 반환
                    with open(target_path, 'r', encoding='utf-8') as f2:
                        content = f2.read()
                    return {"content": content}
        else:
            # YAML 또는 기타 텍스트 파일
            with open(target_path, 'r', encoding='utf-8') as f:
                content = f.read()
            return {"content": content}
    except Exception as e:
        return {"error": str(e)}

def edit_file(proposals):
    """
    Edits files based on proposals and checks for YAML syntax errors and semantic model linting.
    Args:
        proposals (list): List of dictionaries containing file path and edits.
    Returns:
        dict: Success status, issues list, and error/warning counts.
    """
    base_dir = os.path.join(os.path.dirname(__file__), 'playground')
    issues = []
    error_count = 0
    warning_count = 0
    diff = ""
    edited_files = []  # 수정된 파일 목록 추적
    
    for proposal in proposals:
        path = proposal['file']
        edits = proposal['edits']
        target_path = os.path.join(base_dir, path)
        
        if not os.path.exists(target_path):
            return {"success": False, "error": f"File not found: {path}"}
        
        try:
            with open(target_path, 'r', encoding='utf-8') as f:
                original_content = f.read()
            
            # Normalize line endings to \n for consistent matching
            full_text = original_content.replace('\r\n', '\n')
            
            for edit in edits:
                old_text = edit.get('oldText', '')
                new_text = edit.get('newText', '')
                
                # Normalize edit texts as well
                old_text = old_text.replace('\r\n', '\n')
                new_text = new_text.replace('\r\n', '\n')

                # Check for occurrences
                count = full_text.count(old_text)
                if count == 0:
                     return {"success": False, "error": f"Target content not found. Check indentation and exact characters."}
                elif count > 1:
                     return {"success": False, "error": f"Target content found {count} times. Please provide more context to make it unique."}
                
                full_text = full_text.replace(old_text, new_text)
            
            new_full_text = full_text

            # Save the new text directly
            with open(target_path, 'w', encoding='utf-8', newline='\n') as f:
                f.write(new_full_text)
            
            edited_files.append(path)
                
            # Lint Check (YAML Syntax)
            try:
                yaml.safe_load(new_full_text)
            except yaml.YAMLError as exc:
                error_count += 1
                issues.append({
                    "severity": "ERROR",
                    "file": path,
                    "line": getattr(exc, 'problem_mark', None).line + 1 if hasattr(exc, 'problem_mark') and exc.problem_mark else 0,
                    "code": "YAML_SYNTAX_ERROR",
                    "message": str(exc)
                })

        except Exception as e:
             return {"success": False, "error": str(e)}

        # Generate Diff
        original_lines = original_content.replace('\r\n', '\n').splitlines(keepends=True)
        new_lines = new_full_text.splitlines(keepends=True)
        
        diff = "".join(difflib.unified_diff(
            original_lines, 
            new_lines,
            fromfile=f"a/{path}", 
            tofile=f"b/{path}"
        ))

    # Semantic Model Linting (YAML 문법 오류가 없을 때만 수행)
    if error_count == 0 and edited_files:
        try:
            from backend.semantic.model_manager.linter.semantic_linter import lint_semantic_models as lint_func
            
            # playground 디렉토리 전체를 린트
            lint_result = lint_func(base_dir)
            
            # 수정된 파일과 관련된 이슈만 필터링
            for issue in lint_result.issues:
                # issue.file은 절대 경로 또는 상대 경로일 수 있음
                # edited_files의 path와 매칭
                issue_file = issue.file
                
                # 절대 경로인 경우 상대 경로로 변환
                if os.path.isabs(issue_file):
                    try:
                        issue_file = os.path.relpath(issue_file, base_dir)
                    except ValueError:
                        pass  # 변환 실패 시 원본 사용
                
                # 수정된 파일 중 하나와 일치하는지 확인
                for edited_file in edited_files:
                    # 정확히 일치하거나, edited_file이 issue_file의 일부인 경우
                    if issue_file == edited_file or issue_file.endswith(edited_file):
                        issues.append({
                            "severity": issue.severity,
                            "file": edited_file,  # 상대 경로로 통일
                            "line": issue.line,
                            "code": issue.code,
                            "message": issue.message
                        })
                        if issue.severity == "ERROR":
                            error_count += 1
                        elif issue.severity == "WARN":
                            warning_count += 1
                        break
        except Exception as e:
            # 린트 실패는 경고로만 처리 (YAML 문법 오류가 없으면 성공으로 간주)
            import traceback
            issues.append({
                "severity": "WARN",
                "file": edited_files[0] if edited_files else "",
                "line": 0,
                "code": "LINT_ERROR",
                "message": f"Semantic linting failed: {str(e)}"
            })
            warning_count += 1

    return {
        "success": error_count == 0,
        "issues": issues,
        "error_count": error_count,
        "warning_count": warning_count,
        "diff": diff
    }

def lint_semantic_models(path: str):
    """
    Semantic model을 린트합니다.
    Args:
        path (str): playground 디렉토리 내의 경로 또는 절대 경로
    Returns:
        dict: 린트 결과
    """
    try:
        from backend.semantic.model_manager.linter.semantic_linter import lint_semantic_models as lint_func
        
        # 경로 처리
        if os.path.isabs(path):
            base_dir = path
        else:
            base_dir = os.path.join(os.path.dirname(__file__), 'playground', path)
        
        if not os.path.exists(base_dir):
            return {"success": False, "error": f"Path not found: {path}"}
        
        # 동기 함수로 실행 (이미 동기 함수이므로)
        result = lint_func(base_dir)
        
        # 결과를 딕셔너리로 변환
        return {
            "success": True,
            "issues": [
                {
                    "severity": issue.severity,
                    "file": issue.file,
                    "line": issue.line,
                    "code": issue.code,
                    "message": issue.message
                }
                for issue in result.issues
            ],
            "error_count": result.error_count,
            "warning_count": result.warning_count
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

def parse_semantic_models(path: str):
    """
    Semantic model을 파싱하여 manifest를 생성합니다.
    Args:
        path (str): playground 디렉토리 내의 경로 또는 절대 경로
    Returns:
        dict: 파싱 결과
    """
    try:
        # Import hook을 먼저 로드
        try:
            import import_hook
        except ImportError:
            pass
        
        from backend.semantic.model_manager.parser.semantic_parser import assemble_manifest, write_manifest
        
        # 경로 처리 - 절대 경로로 정규화
        if os.path.isabs(path):
            base_dir = path
        else:
            # 상대 경로인 경우
            # playground로 시작하거나 .\playground 같은 경우 처리
            normalized_path = path.replace('./', '').replace('.\\', '').replace('playground/', '').replace('playground\\', '')
            if normalized_path:
                base_dir = os.path.join(os.path.dirname(__file__), 'playground', normalized_path)
            else:
                # 빈 문자열이면 playground 디렉토리 자체
                base_dir = os.path.join(os.path.dirname(__file__), 'playground')
        
        # 경로 정규화 (절대 경로로 변환 및 정리)
        base_dir = os.path.abspath(os.path.normpath(base_dir))
        
        if not os.path.exists(base_dir):
            return {"success": False, "error": f"Path not found: {base_dir} (입력 경로: {path})"}
        
        # 필수 파일/디렉토리 확인 및 생성
        sources_yml = os.path.join(base_dir, 'sources.yml')
        sem_dir = os.path.join(base_dir, 'semantic_models')
        
        missing_items = []
        created_items = []
        
        # semantic_models 디렉토리 생성
        if not os.path.exists(sem_dir) or not os.path.isdir(sem_dir):
            os.makedirs(sem_dir, exist_ok=True)
            created_items.append("semantic_models/ 디렉토리")
        
        # sources.yml 파일 생성 또는 업데이트 (없는 경우 또는 테이블 정보가 없는 경우)
        sources_need_update = False
        if not os.path.exists(sources_yml):
            sources_need_update = True
            existing_sources = {}
        else:
            # 기존 sources.yml 읽기
            import yaml
            try:
                with open(sources_yml, 'r', encoding='utf-8') as f:
                    existing_sources_data = yaml.safe_load(f) or {}
                    existing_sources = {src.get('name'): src for src in existing_sources_data.get('sources', [])}
            except Exception:
                existing_sources = {}
                sources_need_update = True
        
        # 기존 semantic model 파일들을 스캔하여 사용된 소스와 테이블 추출
        source_tables = {}  # {source_name: set(table_names)}
        if os.path.exists(sem_dir):
            for yml_file in os.listdir(sem_dir):
                if yml_file.endswith(('.yml', '.yaml')):
                    yml_path = os.path.join(sem_dir, yml_file)
                    try:
                        import yaml
                        with open(yml_path, 'r', encoding='utf-8') as f:
                            data = yaml.safe_load(f)
                            if data:
                                # semantic_models에서 table 필드 추출
                                if 'semantic_models' in data:
                                    for sm in data.get('semantic_models', []):
                                        table_ref = sm.get('table', '')
                                        if isinstance(table_ref, str) and '(' in table_ref:
                                            # source_name('table_name') 형식에서 추출
                                            import re
                                            match = re.search(r"(\w+)\s*\(\s*['\"]?([^'\"]+)['\"]?\s*\)", table_ref)
                                            if match:
                                                source_name = match.group(1)
                                                table_name = match.group(2)
                                                if source_name not in source_tables:
                                                    source_tables[source_name] = set()
                                                source_tables[source_name].add(table_name)
                    except Exception:
                        pass
        
        # sources.yml 업데이트 필요 여부 확인
        if source_tables:
            for source_name, tables in source_tables.items():
                if source_name not in existing_sources:
                    sources_need_update = True
                    break
                # 기존 소스에 테이블이 없거나 부족한 경우
                existing_tables = {tbl.get('name') for tbl in existing_sources[source_name].get('tables', [])}
                if not tables.issubset(existing_tables):
                    sources_need_update = True
                    break
        
        if sources_need_update:
            # sources.yml 생성 또는 업데이트
            sources_list = []
            
            # 기존 소스 유지하면서 테이블 정보 업데이트
            for source_name, tables in source_tables.items():
                if source_name in existing_sources:
                    # 기존 소스 업데이트
                    existing_source = existing_sources[source_name].copy()
                    existing_table_names = {tbl.get('name') for tbl in existing_source.get('tables', [])}
                    # 누락된 테이블 추가
                    for table_name in tables:
                        if table_name not in existing_table_names:
                            if 'tables' not in existing_source:
                                existing_source['tables'] = []
                            existing_source['tables'].append({"name": table_name})
                    sources_list.append(existing_source)
                else:
                    # 새 소스 생성
                    sources_list.append({
                        "name": source_name,
                        "database": "default_db",
                        "schema": "default_schema",
                        "tables": [{"name": table_name} for table_name in sorted(tables)]
                    })
            
            # 기존 소스 중 사용되지 않는 것도 유지 (데이터베이스 정보 보존)
            for source_name, source_data in existing_sources.items():
                if source_name not in source_tables:
                    sources_list.append(source_data)
            
            # 소스가 없으면 기본 소스 생성
            if not sources_list:
                sources_list.append({
                    "name": "default",
                    "database": "default_db",
                    "schema": "default_schema",
                    "tables": []
                })
            
            default_sources = {"sources": sources_list}
            import yaml
            with open(sources_yml, 'w', encoding='utf-8') as f:
                yaml.dump(default_sources, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
            if not os.path.exists(sources_yml) or created_items:
                created_items.append("sources.yml (기본 템플릿)")
            else:
                created_items.append("sources.yml (업데이트됨)")
        
        # 기존 YAML 파일들을 semantic_models 디렉토리로 이동 (있는 경우)
        existing_yml_files = [f for f in os.listdir(base_dir) 
                             if f.endswith(('.yml', '.yaml')) and f != 'sources.yml' and f != 'date.yml']
        
        moved_files = []
        for yml_file in existing_yml_files:
            src_path = os.path.join(base_dir, yml_file)
            dst_path = os.path.join(sem_dir, yml_file)
            if not os.path.exists(dst_path):
                try:
                    import shutil
                    shutil.move(src_path, dst_path)
                    moved_files.append(yml_file)
                except Exception as e:
                    pass  # 이동 실패 시 무시
        
        # date.yml 파일 생성 (없는 경우)
        date_yml = os.path.join(base_dir, 'date.yml')
        if not os.path.exists(date_yml):
            import yaml
            default_date_yml = {
                "models": [
                    {
                        "name": "time_spine_daily",
                        "columns": [
                            {
                                "name": "date_day",
                                "granularity": "day"
                            }
                        ]
                    }
                ]
            }
            with open(date_yml, 'w', encoding='utf-8') as f:
                yaml.dump(default_date_yml, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
            created_items.append("date.yml (기본 템플릿)")
        
        # time_spine_daily.sql 파일 생성 (없는 경우)
        time_spine_sql = os.path.join(base_dir, 'time_spine_daily.sql')
        if not os.path.exists(time_spine_sql):
            # 기본 time_spine SQL 생성
            default_time_spine_sql = """-- Time spine table for date dimension
-- This table is used for time-based aggregations
SELECT date_day
FROM UNNEST(GENERATE_DATE_ARRAY('2000-01-01', '2100-12-31')) AS date_day
"""
            with open(time_spine_sql, 'w', encoding='utf-8') as f:
                f.write(default_time_spine_sql)
            created_items.append("time_spine_daily.sql (기본 템플릿)")
        
        if created_items or moved_files:
            info_msg = []
            if created_items:
                info_msg.append(f"생성됨: {', '.join(created_items)}")
            if moved_files:
                info_msg.append(f"이동됨: {', '.join(moved_files)}")
            # 정보는 반환값에 포함시키지 않고 계속 진행
        
        # manifest 생성
        manifest = assemble_manifest(base_dir)
        
        # manifest 파일 저장
        manifest_path = os.path.join(base_dir, "semantic_manifest.json")
        write_manifest(manifest, manifest_path)
        
        # 결과 요약
        semantic_models_count = len(manifest.get("semantic_models", []))
        metrics_count = len(manifest.get("metrics", []))
        
        return {
            "success": True,
            "message": f"Manifest 생성 완료: {semantic_models_count}개 모델, {metrics_count}개 메트릭",
            "manifest_path": manifest_path,
            "semantic_models_count": semantic_models_count,
            "metrics_count": metrics_count
        }
    except Exception as e:
        import traceback
        error_detail = str(e)
        # ParseError인 경우 더 명확한 메시지
        if "ParseError" in str(type(e)):
            error_detail = f"파싱 오류: {str(e)}"
        return {
            "success": False, 
            "error": error_detail,
            "traceback": traceback.format_exc() if "traceback" in dir() else None
        }

def get_random_yml_file():
    """
    playground 디렉토리에서 랜덤으로 yml 파일 1개를 선택하여 반환합니다.
    sources.yml 파일은 제외합니다.
    Returns:
        dict: 선택된 파일의 경로와 내용
    """
    try:
        base_dir = os.path.join(os.path.dirname(__file__), 'playground')
        yml_files = glob.glob(os.path.join(base_dir, "**/*.yml"), recursive=True)
        yml_files = [f for f in yml_files if os.path.isfile(f)]
        
        # sources.yml 파일 제외
        yml_files = [f for f in yml_files if os.path.basename(f) != "sources.yml"]
        
        if not yml_files:
            return {"error": "No YML files found in playground directory (excluding sources.yml)"}
        
        # 랜덤으로 1개 선택
        selected_file = random.choice(yml_files)
        rel_path = os.path.relpath(selected_file, base_dir)
        
        with open(selected_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        return {
            "path": rel_path,
            "content": content
        }
    except Exception as e:
        return {"error": str(e)}

def convert_smq_to_sql(smq_json: str, manifest_path: str = None, dialect: str = "bigquery"):
    """
    SMQ를 SQL 쿼리로 변환합니다.
    Args:
        smq_json (str): JSON 형식의 SMQ 문자열
        manifest_path (str): semantic_manifest.json 파일 경로 (playground 기준 상대 경로)
        dialect (str): SQL 방언 (기본값: "bigquery")
    Returns:
        dict: 변환 결과 (success, sql, error 등)
    """
    try:
        # sqlglot 모듈 확인
        try:
            import sqlglot
        except ImportError:
            return {
                "success": False,
                "error": "sqlglot 모듈이 설치되지 않았습니다. 'pip install sqlglot' 명령으로 설치해주세요."
            }
        
        # Import hook을 먼저 로드하고 등록 확인
        import sys
        try:
            import import_hook
            # import hook이 sys.meta_path에 등록되어 있는지 확인
            from import_hook import SemanticImportHook
            if not any(isinstance(hook, SemanticImportHook) for hook in sys.meta_path):
                sys.meta_path.insert(0, SemanticImportHook())
        except ImportError:
            pass  # import_hook이 없어도 동작하도록
        
        from backend.semantic.services.smq2sql_service import prepare_smq_to_sql
        
        # SMQ 파싱
        try:
            smq = json.loads(smq_json)
        except json.JSONDecodeError as e:
            return {"success": False, "error": f"Invalid SMQ JSON: {str(e)}"}
        
        # Manifest 로드
        base_dir = os.path.join(os.path.dirname(__file__), 'playground')
        if manifest_path:
            manifest_file = os.path.join(base_dir, manifest_path)
        else:
            # 기본 manifest 경로 시도
            manifest_file = os.path.join(base_dir, "semantic_manifest.json")
        
        if not os.path.exists(manifest_file):
            return {"success": False, "error": f"Manifest file not found: {manifest_file}"}
        
        with open(manifest_file, 'r', encoding='utf-8') as f:
            manifest_content = json.load(f)
        
        # SMQ를 SQL로 변환
        result = prepare_smq_to_sql(smq, manifest_content, dialect, cte=True)
        
        if result.get("success"):
            # queries 배열에서 첫 번째 쿼리 추출
            results = result.get("results")
            if results:
                queries = results.get("queries", [])
                if queries and len(queries) > 0:
                    return {
                        "success": True,
                        "sql": queries[0].get("query", ""),
                        "metadata": queries[0].get("metadata") or [],  # None인 경우 빈 리스트
                        "all_queries": queries
                    }
                else:
                    return {"success": False, "error": "No queries generated from SMQ"}
            else:
                # results가 없는 경우 (단일 쿼리 반환 형식)
                return {
                    "success": True,
                    "sql": result.get("sql", ""),
                    "metadata": result.get("metadata") or [],  # None인 경우 빈 리스트
                    "all_queries": [{"query": result.get("sql", ""), "metadata": result.get("metadata") or []}]
                }
        else:
            return {"success": False, "error": result.get("error", "Unknown error")}
    
    except Exception as e:
        import traceback
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }
