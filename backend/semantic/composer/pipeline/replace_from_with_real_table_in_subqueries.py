import vendor_setup
from backend.semantic.utils import replace_from_with_real_table
from sqlglot import expressions as exp


def replace_from_with_real_table_in_subqueries(parsed_smq, semantic_manifest, dialect):
    # 현재는 filter에만 적용합니다
    for layer in parsed_smq:
        for key in parsed_smq[layer]:
            if key == "limit":
                continue
            for node in parsed_smq[layer][key]:
                for subquery in node.find_all(exp.Subquery):
                    select = subquery.this
                    # 1) subquery의 다른 칼럼 중에 앞에 from의 테이블 네임이 붙은 게 있으면 제거해 줍니다
                    from_clause = select.args.get("from")
                    if not from_clause:
                        continue
                    this_arg = from_clause.args.get("this")
                    if not this_arg:
                        pass
                    from_table_name = this_arg.name
                    for column in select.find_all(exp.Column):
                        table_name, column_name = (
                            column.name.split("__", 1)
                            if "__" in column.name
                            else (None, None)
                        )
                        if table_name == from_table_name:
                            column.replace(
                                exp.Column(this=exp.Identifier(this=column_name))
                            )

                    # 2) from을 실제 테이블 네임으로 교체해 줍니다
                    new_select = replace_from_with_real_table(
                        select, semantic_manifest, dialect
                    )
                    subquery.set("this", new_select)
    return parsed_smq
