from backend.semantic.types.smq_types import SMQ
from collections import defaultdict
from backend.utils.logger import setup_logger
from backend.semantic.parser.metrics import parse_metrics
from backend.semantic.parser.limit import parse_limit
from backend.semantic.parser.groups import parse_groups
from backend.semantic.parser.filters import parse_filters
from backend.semantic.parser.orders import parse_orders
from backend.semantic.parser.joins import parse_joins

logger = setup_logger("smq_parser")


class SMQParser:
    """SMQ를 파싱하고 테이블별로 그룹화된 결과를 반환하는 객체"""

    def __init__(
        self,
        semantic_manifest,
        dialect=None,
    ):
        self.semantic_manifest = semantic_manifest
        self.dialect = dialect

    def parse(self, smq: SMQ):
        """
        설계 원칙
        1. 각 parser는 해당 SMQ 키에 맞는 값만 추가하며, filter에 있는 값이 select에 없어서 생기는 문제 등은 composer에서 처리합니다.
        """
        logger.info("🔵 SMQParser.parse 시작")
        parsed_smq = defaultdict(lambda: defaultdict())

        # SMQ 키별 파서 함수 매핑
        parsers = {
            "metrics": parse_metrics,
            "filters": parse_filters,
            "groups": parse_groups,
            "orders": parse_orders,
            "limit": parse_limit,
            "joins": parse_joins,
        }

        logger.info("🔵 SMQ 키 순회 시작: %s", list(smq.keys()))
        for k, v in smq.items():
            if not v or (isinstance(v, list) and len(v) == 1 and v[0] == ""):
                logger.info("🔵 SMQ 키 '%s' 건너뜀 (비어있음)", k)
                continue
            parser = parsers.get(k)
            if parser:
                logger.info("🔵 파서 '%s' 실행 시작 (값: %s)", k, str(v)[:100])
                try:
                    parsed_smq = parser(parsed_smq, v, self.semantic_manifest, self.dialect)
                    logger.info("🔵 파서 '%s' 실행 완료", k)
                except Exception as e:
                    import traceback
                    logger.error("🔵 파서 '%s' 실행 중 오류: %s", k, str(e))
                    logger.error("🔵 Traceback: %s", traceback.format_exc())
                    raise
            else:
                raise ValueError(f"{k}는 SMQ에서 사용할 수 없는 키값입니다.")

        logger.info("🔵 SMQParser.parse 완료")
        return parsed_smq
