from sqlglot import expressions as exp
from semantic.utils import ARITHMETIC_EXPRESSIONS


def add_alias_is_uppermost_select_is_statements_without_alias(parsed_smq):
    uppermost_layer = "deriv" if "deriv" in parsed_smq else "agg"
    select_statements = parsed_smq[uppermost_layer].get("metrics", [])
    for node in select_statements:
        if isinstance(node, exp.Alias) and node.alias:
            continue
        if isinstance(node, exp.Alias) and not node.alias:
            aliased_node = exp.Alias(
                this=node.this,
                alias=exp.Column(this=exp.Identifier(this=node.this.sql())),
            )
            parsed_smq[uppermost_layer]["metrics"].remove(node)
            parsed_smq[uppermost_layer]["metrics"].append(aliased_node)
        elif node.find(ARITHMETIC_EXPRESSIONS):
            aliased_node = exp.Alias(
                this=node,
                alias=exp.Column(this=exp.Identifier(this=node.sql())),
            )
            parsed_smq[uppermost_layer]["metrics"].remove(node)
            parsed_smq[uppermost_layer]["metrics"].append(aliased_node)

    return parsed_smq
