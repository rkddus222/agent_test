import vendor_setup  # noqa: F401
from sqlglot import exp
from typing import Dict


def conver_cte_to_inline(root: exp.Expression) -> exp.Expression:
    """
    WITH 절에 정의된 CTE들을 전부 FROM/JOIN의 서브쿼리로 인라인하고,
    최종적으로 WITH 절을 제거한 새 AST를 반환한다.

    - 입력: sqlglot AST (Select, Union, etc. 최상위 Expression)
    - 출력: CTE가 인라인된 새로운 AST
    """
    root = root.copy()

    with_expr: exp.With = root.args.get("with")
    if not with_expr:
        return root

    # 1) CTE 정의를 순서대로 수집 (name -> body 매핑)
    cte_definitions: Dict[str, exp.Select] = {}
    cte_order: list[str] = []  # CTE 정의 순서 유지

    for cte in with_expr.expressions:
        name = cte.alias_or_name
        if not name:
            continue

        cte_body = cte.this
        if isinstance(cte_body, exp.Subquery):
            cte_body = cte_body.this

        cte_definitions[name] = cte_body.copy()
        cte_order.append(name)

    # 2) 각 CTE를 순서대로 inline 치환
    #    나중에 정의된 CTE가 이전 CTE를 참조하므로, 정의 순서대로 처리
    for cte_name in cte_order:
        cte_body = cte_definitions[cte_name]

        # 이 CTE body 내에서 다른 CTE 참조를 subquery로 치환
        def _replace_cte_refs(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Table):
                table_id = node.this
                if isinstance(table_id, exp.Identifier):
                    table_name = table_id.name
                else:
                    table_name = str(table_id)

                # 이 테이블이 이전에 정의된 CTE인지 확인
                if table_name in cte_definitions:
                    referenced_cte = cte_definitions[table_name].copy()

                    # Subquery로 감싸고 원래 CTE 이름을 alias로 유지
                    alias = exp.TableAlias(this=exp.to_identifier(table_name))
                    return exp.Subquery(this=referenced_cte, alias=alias)

            return node

        # CTE body를 transform하여 다른 CTE 참조를 inline으로 치환
        cte_definitions[cte_name] = cte_body.transform(_replace_cte_refs)

    # 3) 메인 쿼리 본문에서 CTE 참조를 inline subquery로 치환
    def _replace_main_cte_refs(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Table):
            table_id = node.this
            if isinstance(table_id, exp.Identifier):
                table_name = table_id.name
            else:
                table_name = str(table_id)

            # 메인 쿼리에서 CTE를 참조하면 최종 inlined된 버전으로 치환
            if table_name in cte_definitions:
                inlined_cte = cte_definitions[table_name].copy()
                alias = exp.TableAlias(this=exp.to_identifier(table_name))
                return exp.Subquery(this=inlined_cte, alias=alias)

        return node

    new_root = root.transform(_replace_main_cte_refs)

    # 4) WITH 절 제거
    new_root.set("with", None)

    return new_root
