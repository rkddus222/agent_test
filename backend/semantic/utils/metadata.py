from typing import List, Dict, Any, Optional

import pandas as pd
import vendor_setup
from sqlglot import expressions as exp

from backend.semantic.types.semantic_model_type import SemanticModel
from backend.semantic.types.metric_type import Metric


def has_division(expr) -> bool:
    """sqlglot Expression에 나눗셈이 있는지 확인"""
    if expr is None:
        return False
    if isinstance(expr, exp.Div):
        return True
    if hasattr(expr, 'find'):
        try:
            return any(expr.find(exp.Div))
        except:
            return False
    return False


def infer_deriv_type(
    dependencies: list,
    metrics_df: pd.DataFrame,
    expr: Any
) -> str:
    """
    파생 metric의 타입을 추론합니다.
    
    Args:
        dependencies: 의존하는 metric 이름들
        metrics_df: metrics DataFrame
        expr: sqlglot Expression (연산식)
    
    Returns:
        추론된 데이터 타입
    """
    if not dependencies or len(dependencies) == 0:
        return "numeric"
    
    # 의존 metric들의 타입 수집
    dep_types = []
    for dep_name in dependencies:
        dep_rows = metrics_df[metrics_df["name"] == dep_name]
        if not dep_rows.empty:
            dep_metric = dep_rows.iloc[0]
            # type 필드가 있으면 사용, 없으면 numeric
            dep_type = dep_metric.get("type", "numeric")
            if dep_type is not None:
                dep_types.append(dep_type)
            else:
                dep_types.append("numeric")
    
    if not dep_types:
        return "numeric"
    
    # 타입 추론 로직
    # 1. 모든 의존 metric이 같은 타입이면 그 타입 사용
    if len(set(dep_types)) == 1:
        base_type = dep_types[0]
        
        # 나눗셈 연산이 있으면 decimal/float로 변환
        if has_division(expr):
            if base_type in ("integer", "bigint", "int", "number"):
                return "decimal"
        
        return base_type
    
    # 2. 여러 타입이 섞여 있으면 우선순위에 따라 결정
    # decimal > float > bigint > integer
    type_priority = {
        "decimal": 5,
        "numeric": 5,
        "float": 4,
        "double": 4,
        "bigint": 3,
        "long": 3,
        "integer": 2,
        "int": 2,
        "string": 1,
        "varchar": 1,
    }
    
    prioritized = max(dep_types, key=lambda t: type_priority.get(t, 0))
    
    # 나눗셈이 있고 정수 타입이면 decimal로 변환
    if has_division(expr) and prioritized in ("integer", "bigint", "int", "long", "number"):
        return "decimal"
    
    return prioritized


def get_select_metadata(
    select_clause: pd.DataFrame,
    metrics_df: pd.DataFrame,
    dimensions_df: pd.DataFrame
) -> List[Dict[str, str]]:
    """
    select_clause에서 최종 SELECT에 포함될 컬럼들의 메타데이터를 수집합니다.
    agg CTE의 컬럼들과 deriv metric들을 포함합니다.
    
    Args:
        select_clause: QueryAssembler의 select_clause DataFrame
        metrics_df: metrics DataFrame
        dimensions_df: dimensions DataFrame
        
    Returns:
        List of metadata dictionaries with 'column', 'type', 'label' keys
    """
    metadata = []
    
    # agg level 컬럼들 (dimensions + metrics)
    agg_rows = select_clause[select_clause["level"] == "agg"]
    
    for row in agg_rows.itertuples(index=False):
        model = row.model
        name = row.name
        
        # metrics_df에서 먼저 확인
        if model == "metric" or not model or model == "":
            # Metric 처리
            metric_rows = metrics_df[metrics_df["name"] == name]
            if not metric_rows.empty:
                metric = metric_rows.iloc[0]
                # metric.type은 데이터 타입 (integer, float, decimal, number, varchar, date, datetime, array, map)
                # metric.metric_type은 메트릭 분류 (simple, ratio, derived, conversion, cumulative)
                metadata.append({
                    "column": name,
                    "type": metric.type or "numeric",  # 기본값으로 numeric 사용
                    "label": metric.label or name
                })
                continue  # Metric을 찾았으면 다음 row로
        
        # Dimension 처리
        # if model and model != "":
        #     # model이 있는 경우: semantic_model과 name으로 정확히 매칭
        #     dim_rows = dimensions_df[
        #         (dimensions_df["name"] == name) & 
        #         (dimensions_df["semantic_model"] == model)
        #     ]
        # else:
        #     # model이 비어있는 경우: name만으로 검색 (첫 번째 매칭 결과 사용)
        dim_rows = dimensions_df[dimensions_df["name"] == name]
        
        if not dim_rows.empty:
            dimension = dim_rows.iloc[0]
            metadata.append({
                "column": name,
                "type": dimension.type,
                "label": dimension.label or name
            })
    
    # deriv level 컬럼들 (파생 metric 식들) - 개선된 타입 추론
    deriv_rows = select_clause[select_clause["level"] == "deriv"]
    for row in deriv_rows.itertuples(index=False):
        name = row.name
        dependencies = getattr(row, "dependencies", []) or []
        expr = getattr(row, "expr", None)
        
        # semantic_manifest.json에 정의된 derived metric인지 먼저 확인
        metric_rows = metrics_df[metrics_df["name"] == name]
        if not metric_rows.empty:
            # 정의된 metric이 있으면 그 label과 type을 그대로 사용
            metric = metric_rows.iloc[0]
            metadata.append({
                "column": name,
                "type": metric.type or "numeric",
                "label": metric.label or name
            })
        else:
            # 정의되지 않은 동적 파생 metric인 경우에만 추론
            inferred_type = infer_deriv_type(dependencies, metrics_df, expr)
            
            metadata.append({
                "column": name,
                "type": inferred_type,
                "label": name
            })
    
    return metadata


def collect_select_clause_from_sql(
    sql: exp.Select,
    parsed_dsl: Dict[str, Any],
    semantic_manifest: Dict[str, Any]
) -> pd.DataFrame:
    """
    SQL의 SELECT 절과 parsed_dsl에서 select_clause DataFrame을 생성합니다.
    agg 레이어와 deriv 레이어의 컬럼 정보를 모두 포함합니다.
    
    Args:
        sql: sqlglot Select 객체 (최종 SELECT 쿼리)
        parsed_dsl: SMQParser가 파싱한 결과
        semantic_manifest: semantic manifest 딕셔너리
        
    Returns:
        select_clause DataFrame with columns: model, name, level, dependencies, expr
    """
    rows = []
    
    # SQL의 SELECT 절에서 컬럼 추출
    for expr in sql.expressions:
        if isinstance(expr, exp.Alias):
            # alias는 exp.Identifier 또는 exp.Column일 수 있음
            if expr.alias:
                if isinstance(expr.alias, exp.Identifier):
                    alias_name = expr.alias.name
                elif isinstance(expr.alias, exp.Column):
                    alias_name = expr.alias.name
                else:
                    alias_name = str(expr.alias)
            else:
                alias_name = None
            col = expr.this
            
            if isinstance(col, exp.Column):
                # agg 레이어의 컬럼
                table_name = col.table.name if isinstance(col.table, exp.Identifier) else (col.table if col.table else None)
                rows.append({
                    "model": table_name,  # 테이블명 (없으면 None)
                    "name": col.name,  # 컬럼명
                    "level": "agg",
                    "dependencies": [],
                    "expr": None,
                })
            else:
                # deriv 레이어의 계산식 (exp.Div, exp.Add 등)
                if alias_name:
                    # 계산식에서 의존하는 컬럼들 추출
                    dependencies = []
                    for col_node in expr.this.find_all(exp.Column):
                        if col_node.name:
                            dependencies.append(col_node.name)
                    
                    rows.append({
                        "model": None,
                        "name": alias_name,
                        "level": "deriv",
                        "dependencies": dependencies,
                        "expr": expr.this,
                    })
        elif isinstance(expr, exp.Column):
            # Alias가 없는 컬럼 (일반적으로는 발생하지 않지만 안전을 위해)
            table_name = expr.table.name if isinstance(expr.table, exp.Identifier) else (expr.table if expr.table else None)
            rows.append({
                "model": table_name,
                "name": expr.name,
                "level": "agg",
                "dependencies": [],
                "expr": None,
            })
    
    # parsed_dsl의 deriv 레이어 정보도 확인하여 누락된 항목 추가
    if "deriv" in parsed_dsl and "metrics" in parsed_dsl["deriv"]:
        for deriv_metric in parsed_dsl["deriv"]["metrics"]:
            if isinstance(deriv_metric, exp.Alias):
                # alias는 exp.Identifier 또는 exp.Column일 수 있음
                if deriv_metric.alias:
                    if isinstance(deriv_metric.alias, exp.Identifier):
                        alias_name = deriv_metric.alias.name
                    elif isinstance(deriv_metric.alias, exp.Column):
                        alias_name = deriv_metric.alias.name
                    else:
                        alias_name = str(deriv_metric.alias)
                else:
                    alias_name = None
                if alias_name:
                    # 이미 추가된 항목인지 확인
                    existing = [r for r in rows if r.get("name") == alias_name and r.get("level") == "deriv"]
                    if not existing:
                        # 계산식에서 의존하는 컬럼들 추출
                        dependencies = []
                        for col_node in deriv_metric.this.find_all(exp.Column):
                            if col_node.name:
                                dependencies.append(col_node.name)
                        
                        rows.append({
                            "model": None,
                            "name": alias_name,
                            "level": "deriv",
                            "dependencies": dependencies,
                            "expr": deriv_metric.this,
                        })
    
    # DataFrame 생성
    if rows:
        select_clause = pd.DataFrame(rows)
    else:
        select_clause = pd.DataFrame(columns=["model", "name", "level", "dependencies", "expr"])
    
    return select_clause


def collect_metadata_from_sql(
    sql: exp.Select,
    parsed_dsl: Dict[str, Any],
    semantic_manifest: Dict[str, Any]
) -> List[Dict[str, str]]:
    """
    SQL과 parsed_dsl에서 메타데이터를 수집합니다.
    
    Args:
        sql: sqlglot Select 객체 (최종 SELECT 쿼리)
        parsed_dsl: SMQParser가 파싱한 결과
        semantic_manifest: semantic manifest 딕셔너리
        
    Returns:
        List of metadata dictionaries with 'column', 'type', 'label' keys
    """
    # select_clause DataFrame 생성
    select_clause = collect_select_clause_from_sql(sql, parsed_dsl, semantic_manifest)
    
    # metrics와 dimensions DataFrame 생성
    metrics_df = pd.DataFrame(semantic_manifest["metrics"])
    sm = semantic_manifest["semantic_models"]
    
    dimensions_df: pd.DataFrame = pd.DataFrame(
        [
            {
                **d,
                "semantic_model": m["name"],
            }
            for m in sm
            for d in m["dimensions"]
        ]
    )
    
    # 메타데이터 수집
    if not select_clause.empty:
        metadata = get_select_metadata(select_clause, metrics_df, dimensions_df)
    else:
        metadata = []
    
    return metadata
