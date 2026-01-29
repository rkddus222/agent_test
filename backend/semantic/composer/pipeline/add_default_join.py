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
                exp.Column(this=exp.Identifier(this=column_name, quoted=col.this.quoted)),
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


def find_join_path(
    sm1, sm2
) -> Optional[Tuple[str, str, List[Tuple[str, str]]]]:
    """
    (lhm, rhm, [(lhe1, rhe1), (lhe2, rhe2), ...]) or None
    - 매칭되는 모든 PK/FK 쌍을 반환하여 복합 키 조인 지원
    - 항상 LEFT JOIN 사용
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

    # sm1.foreign -> sm2.primary 매칭 수집
    right_primary = _primary_lookup(sm2)
    join_pairs: List[Tuple[str, str]] = []

    for e in sm1_entities:
        e_type = e.get("type") if isinstance(e, dict) else getattr(e, "type", None)
        e_name = e.get("name") if isinstance(e, dict) else e.name
        e_expr = e.get("expr") if isinstance(e, dict) else getattr(e, "expr", None)

        if e_type == "foreign" and e_name in right_primary:
            join_pairs.append((e_expr or e_name, right_primary[e_name]))

    if join_pairs:
        return (sm1_name, sm2_name, join_pairs)

    # sm2.foreign -> sm1.primary 매칭 수집
    left_primary = _primary_lookup(sm1)
    join_pairs = []

    for e in sm2_entities:
        e_type = e.get("type") if isinstance(e, dict) else getattr(e, "type", None)
        e_name = e.get("name") if isinstance(e, dict) else e.name
        e_expr = e.get("expr") if isinstance(e, dict) else getattr(e, "expr", None)

        if e_type == "foreign" and e_name in left_primary:
            join_pairs.append((e_expr or e_name, left_primary[e_name]))

    if join_pairs:
        return (sm2_name, sm1_name, join_pairs)

    return None


# --- Graph (name-based) -------------------------------------------------------


def _build_join_graph_and_paths_by_name(
    sms: List,
) -> Tuple[Dict[frozenset, Tuple[str, str, List[Tuple[str, str]]]], Dict[str, Set[str]]]:
    """name 기반 그래프 생성 - 복합 키 조인 지원"""
    name_of = {sm["name"]: sm for sm in sms}
    names = list(name_of.keys())

    edges: Dict[frozenset, Tuple[str, str, List[Tuple[str, str]]]] = {}
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
    edges: Dict[frozenset, Tuple[str, str, List[Tuple[str, str]]]],
    adj: Dict[str, Set[str]],
) -> List[Tuple[str, str, List[Tuple[str, str]]]]:
    """BFS를 사용하여 조인 순서를 결정 - 복합 키 조인 지원"""
    if len(comp) < 2:
        return []

    if len(comp) == 2:
        key = frozenset(comp)
        path = edges.get(key)
        return [path] if path else []

    join_sequence: List[Tuple[str, str, List[Tuple[str, str]]]] = []
    joined: Set[str] = {comp[0]}
    queue: List[str] = [comp[0]]

    while queue and len(joined) < len(comp):
        current = queue.pop(0)

        for neighbor in adj.get(current, set()):
            if neighbor not in joined and neighbor in comp:
                key = frozenset({current, neighbor})
                path = edges.get(key)

                if path:
                    lhm, rhm, join_pairs = path

                    # current가 left가 되도록 조정
                    if current == lhm and neighbor == rhm:
                        join_sequence.append((lhm, rhm, join_pairs))
                    elif current == rhm and neighbor == lhm:
                        # 조인 쌍의 좌우를 바꿔줌
                        swapped_pairs = [(rhe, lhe) for lhe, rhe in join_pairs]
                        join_sequence.append((rhm, lhm, swapped_pairs))
                    else:
                        join_sequence.append(path)
                    
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
    모델 이름 리스트로부터 SQL JOIN 절 생성 (복합 키 조인 지원)

    Args:
        semantic_models: 전체 semantic model 리스트
        models: 조인할 모델 이름들 (예: ["acct_installment_saving_src", "acct_installment_saving_daily"])

    Returns:
        SQL JOIN 절 문자열 (예: "FROM A LEFT JOIN B ON A.계좌번호 = B.계좌번호 AND A.기준일자 = B.기준일자")

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
    first_model = join_sequence[0][0]  # lhm of first join
    sql_parts = [f"FROM {first_model}"]

    # 각 조인 추가 (모두 LEFT JOIN 사용, 복합 키 지원)
    for lhm, rhm, join_pairs in join_sequence:
        # 모든 조인 조건을 AND로 연결
        on_conditions = [f"{lhm}.{lhe} = {rhm}.{rhe}" for lhe, rhe in join_pairs]
        on_clause = " AND ".join(on_conditions)
        join_clause = f"LEFT JOIN {rhm} ON {on_clause}"
        sql_parts.append(join_clause)

    return " ".join(sql_parts)
