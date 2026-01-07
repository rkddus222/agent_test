from typing import List, Optional, TypedDict


class SMQ(TypedDict):
    """SMQ (Semantic Model Query) 쿼리 타입 정의"""

    metrics: List[str]
    group_by: List[str]
    filters: List[str]
    order_by: List[str]
    join: Optional[str]
    limit: Optional[int]
