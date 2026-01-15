from collections import defaultdict
from sqlglot import expressions as exp
import sqlglot
from backend.semantic.utils import find_metric_by_name


def distribute_smq_with_designated_models(smq, model_sets, semantic_manifest):

    distributed_smqs = {}
    model_sets_in_tuple = []
    for item in model_sets:
        item_tuple = tuple(sorted(item))
        model_sets_in_tuple.append(item_tuple)
        distributed_smqs[item_tuple] = defaultdict(list)

    for key, items in smq.items():
        if key == "limit":
            # 모든 model_set에 limit 적용
            for model_set in model_sets_in_tuple:
                distributed_smqs[model_set][key] = items
            continue

        # items가 None이면 건너뛰기
        if items is None:
            continue

        for item in items:
            item_tables = _extract_tables_from_smq_item(item, semantic_manifest)
            for model_set in model_sets_in_tuple:
                if item_tables.issubset(set(model_set)):
                    distributed_smqs[model_set][key].append(item)
                    break

    # 각 distributed_smq에 model_sets 정보 추가 및 검증
    validated_smqs = {}
    for model_set_tuple, smq_dict in distributed_smqs.items():
        # model_sets 정보 추가
        smq_dict["model_sets"] = list(model_set_tuple)
        # 필수 키들이 있는지 확인
        if "metrics" not in smq_dict or not smq_dict["metrics"]:
            # metrics가 없으면 이 model_set에 대한 SMQ는 건너뛰기
            continue
        validated_smqs[model_set_tuple] = smq_dict
    
    return validated_smqs


def _extract_tables_from_smq_item(item, semantic_manifest):
    tables = set()
    parsed_item = sqlglot.parse_one(item)

    for col in parsed_item.find_all(exp.Column):
        col_name = col.name
        # measure인 경우 -> 간단히 table_name을 붙임
        if "__" in col_name:
            table_name, _ = col_name.split("__", 1)
            tables.add(table_name)
        # metric인 경우 -> metric expr을 파싱해서 나오는 모든 칼럼의 table_name을 붙임
        else:
            metric = find_metric_by_name(col_name, semantic_manifest)
            if metric:
                parsed_metric_expr = sqlglot.parse_one(metric["expr"])
                for metric_col in parsed_metric_expr.find_all(exp.Column):
                    if "__" in metric_col.name:
                        table_name, _ = metric_col.name.split("__", 1)
                        tables.add(table_name)

    return tables
