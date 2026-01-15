import json
from typing import Union, Dict, Any

import vendor_setup  # vendor 경로 설정 및 벤더 패키지 로드를 위한 사이드 이펙트
import sqlglot
# from sqlglot import exp
# import pandas as pd

from backend.semantic.composer.pipeline.add_default_join import JoinError
from backend.semantic.utils.metadata import collect_metadata_from_sql
from backend.semantic.types.metric_type import load_metrics
from backend.semantic.parser import SMQParser
from backend.semantic.composer import SQLComposer
from backend.semantic.utils.inline_converter import conver_cte_to_inline
from backend.semantic.utils.distribute_smq import distribute_smq_with_designated_models

from backend.utils.logger import setup_logger


logger = setup_logger("smq2sql_service")


def prepare_smq_to_sql(
    smq: Dict, manifest_content: Union[str, dict], dialect: str, cte: bool = True
) -> Dict:
    """
    SMQ를 SQL로 변환하기 위한 준비 작업을 수행합니다.
    manifest를 파싱하고 semantic models/metrics를 로드합니다.
    """
    try:
        # manifest_content를 JSON으로 파싱
        if isinstance(manifest_content, dict):
            semantic_manifest = manifest_content
        else:
            try:
                semantic_manifest = json.loads(manifest_content)
            except json.JSONDecodeError as e:
                return {"success": False, "error": f"Invalid manifest JSON: {str(e)}"}
        # Manifest 구조 검증
        if "semantic_models" not in semantic_manifest:
            return {"success": False, "error": "Manifest missing 'semantic_models' key"}

        if "metrics" not in semantic_manifest:
            return {"success": False, "error": "Manifest missing 'metrics' key"}

        # Metrics 로드
        try:
            metrics = load_metrics(semantic_manifest["metrics"])
        except Exception as e:
            return {"success": False, "error": f"Failed to load metrics: {str(e)}"}

        logger.info(
            "  📊 Requested smq:\n%s", json.dumps(smq, indent=2, ensure_ascii=False)
        )

        try:
            logger.info("🔵 smq_to_sql 함수 호출 시작...")
            result = smq_to_sql(semantic_manifest, metrics, smq, dialect, cte)
            logger.info("🔵 smq_to_sql 함수 호출 완료, success: %s", result.get("success"))
        except JoinError as e:
            logger.error(f"Caught Join Error. You should process: {e.model_sets}")
            smqs = distribute_smq_with_designated_models(
                smq, e.model_sets, semantic_manifest
            )
            logger.info(f"🔧 distributed_smq: {smqs}")
            
            # 분배 결과 검증
            if not smqs or len(smqs) == 0:
                raise ValueError(
                    "joins 조건이 부족하거나, SMQ의 항목들이 지정된 model sets에 매핑되지 않습니다. "
                    f"요청된 model sets: {e.model_sets}, "
                    f"원본 SMQ: {smq}"
                )
            
            result = []
            for model_set_tuple, distributed_smq in smqs.items():
                # distributed_smq에 model_sets가 있는지 확인
                if "model_sets" not in distributed_smq:
                    distributed_smq["model_sets"] = list(model_set_tuple)
                
                logger.info(f"Processing SMQ for models: {distributed_smq.get('model_sets', model_set_tuple)}")
                
                # 필수 키 검증
                if "metrics" not in distributed_smq or not distributed_smq["metrics"]:
                    raise ValueError(
                        f"분배된 SMQ에 metrics가 없습니다. "
                        f"model sets: {distributed_smq.get('model_sets', model_set_tuple)}, "
                        "joins 조건이 부족하여 SMQ를 제대로 분배할 수 없습니다."
                    )
                
                partial_result = smq_to_sql(
                    semantic_manifest, metrics, distributed_smq, dialect, cte
                )
                if not partial_result.get("success", False):
                    raise ValueError(partial_result.get("error", "Unknown error"))
                result.append(partial_result)

        # result가 1개인 경우(list가 아닌 경우)
        if not isinstance(result, list):
            logger.info("🔧 Processing single query result...")
            logger.info(
                "  Result keys: %s",
                result.keys() if isinstance(result, dict) else "Not a dict",
            )
            logger.info("  Success: %s", result.get("success"))
            if result.get("success"):
                queries = [{"query": result["sql"], "metadata": result["metadata"]}]
                logger.info(
                    "  ✅ Created query with SQL length: %d", len(result["sql"])
                )
            else:
                # smq_to_sql에서 이미 상세한 에러 로그를 출력했으므로 여기서는 간단히만 로그
                error_msg = result.get("error", "Unknown error")
                logger.error(
                    "  ❌ Result marked as unsuccessful: %s", error_msg
                )
                raise ValueError(error_msg)

        # result가 n개인 경우(list인 경우)
        else:
            logger.info(
                "🔧 Processing multiple query results (count: %d)...", len(result)
            )
            # 여러 쿼리 결과를 표준 형식으로 변환: {"sql": ...} -> {"query": ...}
            queries = [{"query": q["sql"], "metadata": q["metadata"]} for q in result]

        logger.info(
            "✅ Successfully converted smq to sql, total queries: %d", len(queries)
        )
        # 단일 쿼리를 queries 배열로 래핑
        # TODO: SMQ 하나에서 여러 쿼리를 생성할 수 있도록 확장
        #       현재는 하나의 쿼리만 생성하지만, 향후 복잡한 SMQ에서
        #       여러 개의 쿼리를 생성하여 queries 배열에 추가할 수 있도록 구현 필요
        return {
            "success": True,
            "results": {"queries": queries},
            "source_engine": dialect,
        }

    except ValueError as e:
        # ValueError는 이미 smq_to_sql에서 상세 로그를 출력했으므로 여기서는 간단히만
        error_msg = str(e)
        logger.error("❌ Failed to process request: %s", error_msg)
        return {"success": False, "error": error_msg}
    except Exception as e:
        # 예상치 못한 다른 예외인 경우에만 상세 로그 출력
        import traceback
        logger.error("❌ Failed to process request: %s", str(e))
        logger.error(
            "🔍 Request details: metrics=%s, groupBy=%s, filters=%s",
            smq.get("metrics"),
            smq.get("groupBy"),
            smq.get("filters"),
        )
        logger.error("  📋 Traceback: %s", traceback.format_exc())
        error_msg = str(e)
        return {"success": False, "error": error_msg}


def smq_to_sql(
    semantic_manifest, metrics, smq: Dict, dialect: str, cte: bool = True
) -> Dict[str, Any]:
    """
    SMQ를 SQL로 변환합니다.
    semantic models와 metrics를 사용하여 실제 SQL 쿼리를 생성합니다.

    Returns:
        Dict with keys: 'sql', 'metadata', 'success'
    """
    try:
        logger.info("🟢 smq_to_sql 함수 시작")
        # SMQ 설정
        smq = {
            "limit": smq.get("limit"),
            "filters": smq.get("filters", []),
            "groups": smq.get("groupBy") or smq.get("group_by", []),
            "metrics": smq.get("metrics", []),
            "orders": smq.get("orderBy") or smq.get("order_by", []),
            "joins": smq.get("joins") or [],
        }
        logger.info("🟢 SMQ 설정 완료: %s", json.dumps(smq, ensure_ascii=False)[:200])

        # 입력 검증
        if not smq["metrics"]:
            raise ValueError("No metrics specified in request")

        logger.info("🟢 Metrics 검증 시작...")
        available_metric_names = [m.name for m in metrics]
        for metric in smq["metrics"]:
            if metric not in available_metric_names:
                try:
                    sqlglot.parse_one(metric)
                    continue  # 식인 경우 통과
                except Exception as e:
                    raise ValueError(
                        f"Metric '{metric}' not found. Available metrics: {available_metric_names}, Error: {str(e)}"
                    )
        logger.info("🟢 Metrics 검증 완료")

        logger.info("🟢 SMQParser 초기화 시작...")
        parser = SMQParser(semantic_manifest=semantic_manifest, dialect=dialect)
        logger.info("🟢 SQLComposer 초기화 시작...")
        composer = SQLComposer(dialect=dialect, semantic_manifest=semantic_manifest)
        logger.info("🟢 Parser/Composer 초기화 완료")

        logger.info("🟢 parser.parse 시작...")
        ast = parser.parse(smq)
        logger.info("🟢 parser.parse 완료")

        logger.info("🟢 composer.compose 시작...")
        sql = composer.compose(parsed_smq=ast, original_smq=smq)
        # sql이 Expression 객체일 수 있으므로 문자열로 변환 후 슬라이싱
        sql_str = sql.sql(dialect=dialect, pretty=True) if sql else None
        logger.info("✅ SqlComposer created sql: %s", sql_str[:200] if sql_str else "None")

        # CTE를 인라인 뷰로 변환
        if cte is False:
            sql = conver_cte_to_inline(sql)

        # 메타데이터 수집
        metadata = collect_metadata_from_sql(sql, ast, semantic_manifest)
        logger.info("metadata: %s", metadata)

        if not metadata:
            raise ValueError("No metadata found")

        return {
            "success": True,
            "sql": sql_str,  # 이미 위에서 변환된 문자열 사용
            "metadata": metadata,
        }

    except JoinError as e:
        raise e

    except RecursionError as e:
        return {
            "success": False,
            "error": f"Circular dependency detected in metrics {smq.get('metrics')}: {str(e)}",
            "sql": None,
            "metadata": [],
        }

    except Exception as e:
        import traceback

        logger.error("❌ SQL conversion failed: %s", str(e))
        logger.error("  🎯 Failed metrics: %s", smq.get("metrics"))
        logger.error("  📊 Failed group_by: %s", smq.get("group_by"))
        logger.error("  📋 Traceback: %s", traceback.format_exc())
        # 원본 에러 메시지를 그대로 전달 (중복 접두사 방지)
        # ValueError나 명확한 에러 메시지는 그대로 전달
        error_msg = str(e)
        return {
            "success": False,
            "error": error_msg,
            "sql": None,
            "metadata": [],
        }

