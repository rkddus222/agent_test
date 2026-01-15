import vendor_setup
from typing import Dict, List

from sqlglot import exp

from backend.utils.logger import setup_logger

from backend.semantic.composer.pipeline import (
    move_groups_to_metrics,
    check_if_original_smq_included_and_complete_if_not,
    check_prerequisite_of_deriv_layer_and_complete,
    check_group_select_parity_and_complete,
    add_default_join,
    check_prerequisite_of_agg_layer_and_complete,
    add_backtick_if_bigquery,
    write_sql,
    replace_from_with_real_table_in_subqueries,
    add_alias_is_uppermost_select_is_statements_without_alias,
    replace_special_char_for_bigquery,
    push_down_agg_from_deriv_layer,
    transform_anonymous_node_into_legit_one,
    move_dimension_expr_to_deriv_layer_if_deriv_exists,
)


logger = setup_logger("SQLComposer")


class SQLComposer:
    """Parser를 통해 파싱된 smq를 sql로 변환하는 객체"""

    def __init__(self, semantic_manifest, dialect=None):
        self.dialect = dialect
        self.semantic_manifest = semantic_manifest

    def compose(self, parsed_smq, original_smq) -> exp.Select:

        # [임시] Dimension 식의 경우, 기본으로는 agg에 들어가지만 만약 deriv layer가 있는 경우 deriv로 옮겨 줍니다.
        parsed_smq = move_dimension_expr_to_deriv_layer_if_deriv_exists(
            parsed_smq, original_smq
        )

        # 5) [Deriv] Deriv Layer에 agg function이 있으면 agg layer로 push down 시킵니다.
        parsed_smq = push_down_agg_from_deriv_layer(
            parsed_smq, original_smq, self.semantic_manifest
        )

        # [임시] Groupby에 있는 항목을 모두 Metrics에도 넣어 줍니다.
        parsed_smq = move_groups_to_metrics(parsed_smq)

        # 1) [전체] SMQ 상의 모든 항목이 들어가 있는지 확인합니다.
        parsed_smq = check_if_original_smq_included_and_complete_if_not(
            parsed_smq, original_smq
        )

        # 2) anonymous node가 있는 경우 legit한 node로 바꿔 줍니다.
        parsed_smq = transform_anonymous_node_into_legit_one(
            parsed_smq,
        )

        # 3) [전체] subquery 안의 from이 deriv/agg가 아니면 real table로 바꿔줍니다.
        parsed_smq = replace_from_with_real_table_in_subqueries(
            parsed_smq, self.semantic_manifest, self.dialect
        )

        # 4) [DERIV] Deriv Layer에 필요한 하위 항목들이 다 들어 있는지 확인합니다.
        parsed_smq = check_prerequisite_of_deriv_layer_and_complete(
            parsed_smq, original_smq, self.semantic_manifest, self.dialect
        )

        # 6) [AGG] Agg layer에서 group과 select가 일치하는지 확인합니다. (aggregation function이 아닌 select는 다 group에 들어 있어야 합니다.)
        parsed_smq = check_group_select_parity_and_complete(parsed_smq)

        # 7) [AGG] Agg Layer에 필요한 하위 항목들이 다 들어 있는지 확인합니다.
        parsed_smq = check_prerequisite_of_agg_layer_and_complete(
            parsed_smq, original_smq, self.semantic_manifest, self.dialect
        )

        # 8) [AGG] 만약 proj layer가 2개 이상인데 agg layer에 join이 없으면 default join을 추가합니다.
        # -> key가 proj layer에 있는지도 확인! (4번을 지났기 때문에...)

        parsed_smq = add_default_join(
            parsed_smq, original_smq, self.semantic_manifest, self.dialect
        )

        # 9) [전체] 만약 uppermost layer의 metrics에 식이 있는데 그 식이 alias가 없으면, 그 식을 str으로 바꿔서 alias로 추가해 줍니다 (아니면 column 이름이 f0 이런 식으로 나옴)
        parsed_smq = add_alias_is_uppermost_select_is_statements_without_alias(
            parsed_smq
        )

        # 10) [전체] bigquery의 경우 모든 identifier에 backtick을 추가합니다.
        if self.dialect.lower() == "bigquery":
            parsed_smq = add_backtick_if_bigquery(parsed_smq, self.dialect)

            # 11) [전체] bigquery의 경우 모든 identifier에 special char를 _로 치환합니다.
            parsed_smq = replace_special_char_for_bigquery(parsed_smq, self.dialect)

        # 12) SQL을 조립합니다. (from절을 제대로 고치는 것도 포함 + from절 quote도 여기서!)
        sql = write_sql(parsed_smq, self.semantic_manifest, self.dialect)

        return sql
