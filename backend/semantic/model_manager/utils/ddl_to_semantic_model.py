import yaml
import os
from typing import Dict, Any, Optional, Tuple
from backend.semantic.model_manager.utils.ddl_types import TableInfo

def _parse_comment(comment: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    주석을 ':' 기준으로 분리하여 label과 description을 반환합니다.
    
    Args:
        comment: 주석 문자열 (예: "기준일자 : 기준일자")
        
    Returns:
        (label, description) 튜플
        - ':'가 있으면: (앞부분, 뒷부분)
        - ':'가 없으면: (전체, 전체)
        - comment가 None이면: (None, None)
    """
    if not comment:
        return None, None
    
    comment = comment.strip()
    if ':' in comment:
        parts = comment.split(':', 1)
        label = parts[0].strip()
        description = parts[1].strip() if len(parts) > 1 else label
        return label, description
    else:
        return comment, comment

def _find_source_name_for_table(table_name: str, sources_yml_path: Optional[str] = None) -> Optional[str]:
    """
    sources.yml에서 테이블명으로 source name을 찾습니다.
    
    Args:
        table_name: 찾을 테이블명
        sources_yml_path: sources.yml 파일 경로 (None이면 기본 경로 사용)
        
    Returns:
        source name 또는 None (찾지 못한 경우)
    """
    if not sources_yml_path:
        # 기본 경로: playground/sources.yml
        current_dir = os.path.dirname(os.path.abspath(__file__))
        playground_dir = os.path.join(current_dir, '..', '..', '..', 'playground')
        sources_yml_path = os.path.join(playground_dir, 'sources.yml')
    
    if not os.path.exists(sources_yml_path):
        return None
    
    try:
        with open(sources_yml_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        if not data or 'sources' not in data:
            return None
        
        # 모든 source를 순회하며 테이블명 찾기
        for src in data.get('sources', []):
            source_name = src.get('name')
            for tbl in src.get('tables', []):
                if isinstance(tbl, dict):
                    tname = tbl.get('name')
                else:
                    tname = tbl  # 리스트에 직접 문자열로 있는 경우
                
                if tname == table_name:
                    return source_name
    except Exception:
        return None
    
    return None

def generate_semantic_model_from_ddl(
    table_info: TableInfo,
    table_name_in_db: str,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    sources_yml_path: Optional[str] = None
) -> str:
    """
    TableInfo 객체를 Semantic Model YAML 문자열로 변환합니다.
    
    Args:
        table_info: 파싱된 테이블 정보
        table_name_in_db: DB상의 테이블 이름
        database: 데이터베이스 이름
        schema: 스키마 이름
        
    Returns:
        YAML 형식의 문자열
    """
    
    # 1. 기본 모델 구조 생성
    model_name = table_info.name
    
    # sources.yml에서 source name 찾기
    source_name = _find_source_name_for_table(table_name_in_db, sources_yml_path)
    
    # 테이블 참조 문자열 생성
    # sources.yml에서 찾은 경우: source_name('table_name') 형식
    # 찾지 못한 경우: 기본값 'test_daquv' 사용
    if source_name:
        table_ref = f"{source_name}('{table_name_in_db}')"
    else:
        # sources.yml에서 찾지 못한 경우 기본값 사용
        table_ref = f"test_daquv('{table_name_in_db}')"
        
    semantic_model = {
        "name": model_name,
        "table": table_ref,
        "description": table_info.comment or f"Semantic model for {table_name_in_db}",
        "entities": [],
        "dimensions": [],
        "measures": []
    }
    
    # 2. PK 처리 (entities)
    # PK가 없으면 첫 번째 컬럼을 entity로 사용하거나 생략? 
    # 보통 PK는 entity로 매핑
    if table_info.primary_keys:
        for pk in table_info.primary_keys:
            semantic_model["entities"].append({
                "name": pk.lower(),
                "type": "primary",
                "expr": pk
            })
    
    # 3. 컬럼 처리 (dimensions 및 measures)
    pk_columns_set = set(table_info.primary_keys) if table_info.primary_keys else set()
    
    for col in table_info.columns:
        col_type = _map_db_type_to_semantic_type(col.type)
        
        # 주석 파싱 (':' 기준으로 label과 description 분리)
        label, description = _parse_comment(col.comment)
        if not label:
            label = col.name
        if not description:
            description = label  # description이 없으면 label과 동일하게 설정
        
        # number 타입이 아닌 컬럼만 dimension으로 추가
        if col_type != "number":
            dim = {
                "name": col.name.lower(),
                "type": col_type,
                "expr": col.name,
                "label": label,
                "description": description
            }
            
            # 날짜 타입인 경우 granularity 추가
            if dim["type"] == "date" or dim["type"] == "time": # time은 granularity가 다를 수 있음
                 if dim["type"] == "date":
                    dim["type_params"] = {"time_granularity": "day"}

            semantic_model["dimensions"].append(dim)
        
        # number 타입 컬럼은 measure로만 추가 (dimension에는 추가하지 않음)
        if col_type == "number":
            measure = {
                "name": col.name.lower(),
                "type": col_type,
                "expr": col.name,
                "label": label,
                "description": description
            }
            semantic_model["measures"].append(measure)
    
    # 4. PK를 count하는 measure 추가 (첫 번째 PK 사용)
    pk_count_measure_name = None
    if table_info.primary_keys:
        first_pk = table_info.primary_keys[0]
        pk_count_measure_name = f"{first_pk.lower()}_count"
        
        # PK 컬럼 정보 찾기
        pk_col = next((col for col in table_info.columns if col.name == first_pk), None)
        pk_comment = pk_col.comment if pk_col else None
        
        # PK 주석 파싱 (':' 기준으로 label과 description 분리)
        pk_label, pk_description = _parse_comment(pk_comment)
        if not pk_label:
            pk_label = f"{first_pk} 수"
        if not pk_description:
            pk_description = pk_label  # description이 없으면 label과 동일하게 설정
        
        semantic_model["measures"].append({
            "name": pk_count_measure_name,
            "type": "number",
            "expr": first_pk,
            "label": pk_label,
            "description": pk_description
        })

    # 전체 구조 조립
    yml_data = {
        "semantic_models": [semantic_model]
    }
    
    # 5. Metrics 생성 (PK를 COUNT_DISTINCT하는 기본 메트릭)
    if pk_count_measure_name and table_info.primary_keys:
        first_pk = table_info.primary_keys[0]
        metric_name = f"total_{model_name.lower()}_count"
        
        yml_data["metrics"] = [{
            "name": metric_name,
            "metric_type": "simple",
            "type": "integer",
            "expr": f"COUNT(DISTINCT {model_name}__{pk_count_measure_name})",
            "label": f"총 {model_name} 수",
            "description": f"총 {model_name} 수"
        }]
    
    # YAML 변환 (한글 처리 포함)
    return yaml.dump(yml_data, allow_unicode=True, default_flow_style=False, sort_keys=False)

def _map_db_type_to_semantic_type(db_type: str) -> str:
    """DB 데이터 타입을 Semantic Model 타입으로 매핑"""
    db_type_lower = db_type.lower()
    
    if any(t in db_type_lower for t in ['int', 'number', 'numeric', 'decimal', 'float', 'double']):
        return 'number' # 또는 sum, average 등 집계 가능성 고려
        # 하지만 dimension type으로는 number, string, boolean, date, time 등이 있음.
        # semantic layer 에서는 categorical(varchar), quantitative(number), time(date/timestamp) 구분 중요
    elif any(t in db_type_lower for t in ['char', 'text', 'string', 'varchar']):
        return 'varchar'  # categorical 대신 varchar 사용
    elif any(t in db_type_lower for t in ['date', 'time', 'timestamp']):
        return 'date' # timestamp도 date로 통칭하거나 datetime 구분
    elif 'bool' in db_type_lower:
        return 'boolean'
    
    return 'varchar' # 기본값 (categorical 대신 varchar)
