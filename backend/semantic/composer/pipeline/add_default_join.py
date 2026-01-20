from __future__ import annotations
import vendor_setup
from typing import Dict, List, Optional, Set, Tuple
from backend.semantic.utils import append_node
import sqlglot
from sqlglot import expressions as exp


def add_default_join(parsed_smq, original_smq, semantic_manifest, dialect):

    if original_smq.get("joins"):
        return parsed_smq

    models = list(parsed_smq.keys())
    base_models = [model for model in models if model not in {"agg", "deriv"}]

    if len(base_models) == 1:
        return parsed_smq

    join_sql = generate_join_sql(semantic_manifest, base_models)
    join_node = sqlglot.parse_one(join_sql, dialect=dialect)
    join_columns = join_node.find_all(exp.Column)
    # 만약에 join column이 proj layer에 없으면 추가해 줍니다.
    for col in join_columns:
        table_name = col.table
        column_name = col.this.this
        proj_layer_metrics_in_str = [
            node.name for node in parsed_smq[table_name].get("metrics", []) if node.name
        ]
        if column_name not in proj_layer_metrics_in_str:
            parsed_smq = append_node(
                parsed_smq,
                table_name,
                "metrics",
                exp.Column(this=exp.Identifier(this=column_name)),
            )

    parsed_smq = append_node(parsed_smq, "agg", "joins", join_node)

    return parsed_smq


class JoinError(Exception):
    def __init__(self, message: str, model_sets: list[tuple]):
        super().__init__(message)
        self.model_sets = model_sets

    def __str__(self):
        return f"JoinError: {self.args[0]} | model_sets={self.model_sets}"


def _expr_or_name(ent) -> str:
    if isinstance(ent, dict):
        return ent.get("expr") or ent.get("name")
    return getattr(ent, "expr", None) or ent.name


def _primary_lookup(sm) -> Dict[str, str]:
    # 딕셔너리와 객체 둘 다 지원
    entities = (
        sm.get("entities", []) if isinstance(sm, dict) else getattr(sm, "entities", [])
    )
    result = {}
    for ent in entities:
        ent_type = (
            ent.get("type") if isinstance(ent, dict) else getattr(ent, "type", None)
        )
        if ent_type == "primary":
            ent_name = ent.get("name") if isinstance(ent, dict) else ent.name
            result[ent_name] = _expr_or_name(ent)
    return result


def find_join_path(sm1, sm2) -> Optional[List[Tuple[str, str, str, str]]]:
    """
    복합 키를 지원하는 JOIN 경로 찾기
    Returns: List of (lhm, lhe, rhm, rhe) tuples - 복합 키의 경우 여러 튜플 반환, 단일 키는 하나의 튜플만 포함
    """
    # 딕셔너리와 객체 둘 다 지원
    sm1_name = sm1.get("name") if isinstance(sm1, dict) else sm1.name
    sm2_name = sm2.get("name") if isinstance(sm2, dict) else sm2.name
    sm1_entities = (
        sm1.get("entities", [])
        if isinstance(sm1, dict)
        else getattr(sm1, "entities", [])
    )
    sm2_entities = (
        sm2.get("entities", [])
        if isinstance(sm2, dict)
        else getattr(sm2, "entities", [])
    )

    right_primary = _primary_lookup(sm2)
    left_primary = _primary_lookup(sm1)

    # 문제 3 해결: 복합 키 지원
    # sm1의 foreign key들이 sm2의 모든 primary key와 매칭되는지 확인
    sm1_foreign_keys = {}
    for e in sm1_entities:
        e_type = e.get("type") if isinstance(e, dict) else getattr(e, "type", None)
        if e_type == "foreign":
            e_name = e.get("name") if isinstance(e, dict) else e.name
            e_expr = e.get("expr") if isinstance(e, dict) else getattr(e, "expr", None)
            if e_name in right_primary:
                sm1_foreign_keys[e_name] = e_expr or e_name

    # sm2의 foreign key들이 sm1의 모든 primary key와 매칭되는지 확인
    sm2_foreign_keys = {}
    for e in sm2_entities:
        e_type = e.get("type") if isinstance(e, dict) else getattr(e, "type", None)
        if e_type == "foreign":
            e_name = e.get("name") if isinstance(e, dict) else e.name
            e_expr = e.get("expr") if isinstance(e, dict) else getattr(e, "expr", None)
            if e_name in left_primary:
                sm2_foreign_keys[e_name] = e_expr or e_name

    # sm1 -> sm2 방향 조인 (sm1의 foreign key 사용)
    if sm1_foreign_keys:
        join_keys = []
        for fk_name, fk_expr in sm1_foreign_keys.items():
            if fk_name in right_primary:
                join_keys.append((
                    sm1_name,
                    fk_expr,
                    sm2_name,
                    right_primary[fk_name],
                ))
        if join_keys:
            return join_keys

    # sm2 -> sm1 방향 조인 (sm2의 foreign key 사용)
    if sm2_foreign_keys:
        join_keys = []
        for fk_name, fk_expr in sm2_foreign_keys.items():
            if fk_name in left_primary:
                join_keys.append((
                    sm2_name,
                    fk_expr,
                    sm1_name,
                    left_primary[fk_name],
                ))
        if join_keys:
            return join_keys

    return None


# --- Graph (name-based) -------------------------------------------------------


def _build_join_graph_and_paths_by_name(
    sms: List,
) -> Tuple[Dict[frozenset, List[Tuple[str, str, str, str]]], Dict[str, Set[str]]]:
    """name 기반 그래프 생성 - 복합 키 지원"""
    name_of = {sm["name"]: sm for sm in sms}
    names = list(name_of.keys())

    edges: Dict[frozenset, List[Tuple[str, str, str, str]]] = {}
    adj: Dict[str, Set[str]] = {n: set() for n in names}

    n = len(names)
    for i in range(n):
        for j in range(i + 1, n):
            ni, nj = names[i], names[j]
            sm_i, sm_j = name_of[ni], name_of[nj]
            path = find_join_path(sm_i, sm_j)
            if path is not None:
                key = frozenset({ni, nj})
                edges[key] = path
                adj[ni].add(nj)
                adj[nj].add(ni)
    return edges, adj


def _connected_components_names(
    nodes: List[str], adj: Dict[str, Set[str]]
) -> List[List[str]]:
    seen: Set[str] = set()
    comps: List[List[str]] = []

    for start in nodes:
        if start in seen:
            continue
        stack = [start]
        seen.add(start)
        comp = [start]
        while stack:
            u = stack.pop()
            for v in adj.get(u, set()):
                if v not in seen:
                    seen.add(v)
                    stack.append(v)
                    comp.append(v)
        comps.append(comp)
    return comps


def _build_join_sequence_for_connected_component(
    comp: List[str],
    edges: Dict[frozenset, List[Tuple[str, str, str, str]]],
    adj: Dict[str, Set[str]],
) -> List[List[Tuple[str, str, str, str]]]:
    """BFS를 사용하여 조인 순서를 결정 - 복합 키 지원"""
    if len(comp) < 2:
        return []

    if len(comp) == 2:
        key = frozenset(comp)
        path = edges.get(key)
        return [path] if path else []

    join_sequence: List[List[Tuple[str, str, str, str]]] = []
    joined: Set[str] = {comp[0]}
    queue: List[str] = [comp[0]]

    while queue and len(joined) < len(comp):
        current = queue.pop(0)

        for neighbor in adj.get(current, set()):
            if neighbor not in joined and neighbor in comp:
                key = frozenset({current, neighbor})
                path = edges.get(key)

                if path:
                    # path는 이미 List[Tuple[str, str, str, str]] 형태
                    # current가 left가 되도록 조정
                    adjusted_path = []
                    for lhm, lhe, rhm, rhe in path:
                        if current == lhm and neighbor == rhm:
                            adjusted_path.append((lhm, lhe, rhm, rhe))
                        elif current == rhm and neighbor == lhm:
                            adjusted_path.append((rhm, rhe, lhm, lhe))
                        else:
                            adjusted_path.append((lhm, lhe, rhm, rhe))
                    
                    join_sequence.append(adjusted_path)
                    joined.add(neighbor)
                    queue.append(neighbor)

    if len(joined) < len(comp):
        unjoined = [m for m in comp if m not in joined]
        raise JoinError(
            f"Cannot join all models in component {comp}. Unjoined: {unjoined}",
            model_sets=[tuple(comp)],
        )

    return join_sequence


# --- Public API (assemble) ----------------------------------------------------


def generate_join_sql(semantic_manifest: list, models: list[str]) -> str:
    """
    모델 이름 리스트로부터 SQL JOIN 절 생성

    Args:
        semantic_models: 전체 semantic model 리스트
        models: 조인할 모델 이름들 (예: ["acct_installment_saving_src", "acct_installment_saving_daily"])

    Returns:
        SQL JOIN 절 문자열 (예: "FROM acct_installment_saving_src A LEFT JOIN acct_installment_saving_daily B ON A.계좌번호 = B.계좌번호")

    Raises:
        JoinError: 모델들을 조인할 수 없는 경우
    """

    semantic_models = semantic_manifest["semantic_models"]

    if not models:
        return ""

    if len(models) == 1:
        return f"FROM {models[0]}"

    # 모델 이름으로 semantic model 조회
    name_to_sm = {sm["name"]: sm for sm in semantic_models}
    sms = []
    for model_name in models:
        sm = name_to_sm.get(model_name)
        if not sm:
            raise ValueError(f"Model '{model_name}' not found in semantic models")
        sms.append(sm)

    # 조인 그래프 구성
    edges, adj = _build_join_graph_and_paths_by_name(sms)
    comps = _connected_components_names(models, adj)

    # 연결되지 않은 컴포넌트가 있으면 에러
    if len(comps) >= 2:
        model_sets = [tuple(comp) for comp in comps]
        raise JoinError(
            "Multiple disjoint model sets detected. Cannot generate JOIN clause.",
            model_sets=model_sets,
        )

    comp = comps[0]

    # 단일 모델인 경우 (이미 위에서 처리했지만 방어적 처리)
    if len(comp) == 1:
        if len(models) >= 2:
            raise JoinError(
                "No joinable pairs among provided models.",
                model_sets=[(m,) for m in models],
            )
        return f"FROM {comp[0]}"

    # 조인 순서 결정
    join_sequence = _build_join_sequence_for_connected_component(comp, edges, adj)

    if not join_sequence:
        raise JoinError(
            f"Cannot find join paths for models: {comp}",
            model_sets=[tuple(comp)],
        )

    # SQL 생성: 전체 테이블 이름을 alias로 사용
    # 첫 번째 모델로 FROM 시작
    first_join_keys = join_sequence[0]  # 첫 번째 조인의 키 리스트
    first_model = first_join_keys[0][0]  # lhm of first join
    sql_parts = [f"FROM {first_model}"]

    # 각 조인 추가 (모두 LEFT JOIN 사용)
    # 문제 3 해결: 복합 키 지원 - 모든 키를 AND로 연결
    for join_keys in join_sequence:
        # join_keys는 List[Tuple[str, str, str, str]] 형태
        if not join_keys:
            continue
        
        # 첫 번째 키에서 테이블 정보 추출
        lhm, _, rhm, _ = join_keys[0]
        
        # 모든 키를 AND로 연결
        on_conditions = []
        for lhm_key, lhe, rhm_key, rhe in join_keys:
            on_conditions.append(f"{lhm_key}.{lhe} = {rhm_key}.{rhe}")
        
        join_clause = f"LEFT JOIN {rhm} ON {' AND '.join(on_conditions)}"
        sql_parts.append(join_clause)

    return " ".join(sql_parts)
