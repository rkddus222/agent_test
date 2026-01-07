from sqlglot import expressions as exp

FUNCTION_MAP = {
    # 집계 함수들
    "AVG": exp.Avg,
    "AVERAGE": exp.Avg,  # 네 케이스
    "SUM": exp.Sum,
    "MAX": exp.Max,
    "MIN": exp.Min,
    "COUNT": exp.Count,
    # 필요하면 여기 계속 추가
    # "UPPER": exp.Upper,
    # "LOWER": exp.Lower,
    # ...
}


def transform_anonymous_node_into_legit_one(parsed_smq):
    for layer in parsed_smq.values():
        for dsl_nodes in layer.values():
            # key가 limit인 경우 int인데, 이런 경우 continue!
            if type(dsl_nodes) is not list:
                continue
            for node in dsl_nodes:
                anonymous_nodes = list(node.find_all(exp.Anonymous))
                if anonymous_nodes:
                    for anon_node in anonymous_nodes:
                        func_name = anon_node.this.upper()
                        if func_name in FUNCTION_MAP:
                            legit_class = FUNCTION_MAP[func_name]
                            new_node = legit_class(this=anon_node.expressions)
                            anon_node.replace(new_node)

    return parsed_smq
