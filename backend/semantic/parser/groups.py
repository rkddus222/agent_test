import sqlglot
from sqlglot import expressions as exp
from backend.semantic.utils import append_node


def parse_groups(parsed_smq, values, semantic_manifest, dialect):
    for value in values:
        parsed_value = sqlglot.parse_one(value, read=dialect)
        # 1) 해당 value가 dimension인 경우
        if "__" in parsed_value.name:
            table_name, column_name = parsed_value.name.split("__")
            value_to_append = exp.Column(
                this=exp.Identifier(this=column_name),
                table=exp.Identifier(this=table_name)
            )
            parsed_smq = append_node(
                parsed_smq,
                "agg",
                "groups",
                value_to_append,
            )
        # 2) 해당 value가 metric인 경우
        else:
            parsed_smq = append_node(
                parsed_smq,
                "agg",
                "groups",
                parsed_value,
            )
    return parsed_smq
