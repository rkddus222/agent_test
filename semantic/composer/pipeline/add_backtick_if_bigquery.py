import vendor_setup
from sqlglot import expressions as exp


def add_backtick_if_bigquery(parsed_smq, dialect):
    """bigquery의 경우 모든 identifier에 backtick을 추가합니다."""
    if not dialect.lower() == "bigquery":
        return parsed_smq

    for layer in parsed_smq:
        for key in parsed_smq[layer]:
            if key == "limit":
                continue
            for node in parsed_smq[layer][key]:
                for ident in node.find_all(exp.Identifier):
                    ident.set("quoted", True)

    return parsed_smq
