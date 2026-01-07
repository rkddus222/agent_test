import re
from sqlglot import exp


def replace_special_char_for_bigquery(parsed_smq, dialect):
    if not dialect.lower() == "bigquery":
        return parsed_smq

    uppermost_layer = "deriv" if "deriv" in parsed_smq else "agg"

    for key in parsed_smq[uppermost_layer]:
        if key == "limit":
            continue
        for node in parsed_smq[uppermost_layer][key]:
            for ident in node.find_all(exp.Identifier):
                new_ident = _replace_special_chars(ident.this)
                ident.set("this", new_ident)

    return parsed_smq


def _replace_special_chars(text: str):
    # 대상 문자 리스트 (문자 그대로 또는 Unicode escape)
    special_chars = r"!\"\$\(\)\*,\./;\?@\[\]\\\^`\{\}~"

    # 1. 앞뒤 공백 제거 + "_" 치환
    #    예: "  !  " → "_" / "  $text" 는 건드리지 않음
    text = re.sub(rf"\s*([{special_chars}])\s*", r"_", text)

    return text
