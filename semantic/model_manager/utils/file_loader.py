from __future__ import annotations

import os
from typing import Any, Dict, List, Tuple

import yaml

from utils.logger import setup_logger


logger = setup_logger("semantic_linter")


def _load_yaml(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_semantic_models_with_files(
    sem_dir: str,
) -> List[Tuple[Dict[str, Any], str, List[str]]]:
    """
    semantic_models 디렉토리의 각 파일에서 semantic_models 항목을 읽어,
    (semantic_model_dict, file_path, file_lines) 튜플 리스트를 반환합니다.
    """
    results: List[Tuple[Dict[str, Any], str, List[str]]] = []

    if not os.path.isdir(sem_dir):
        return results

    for fn in os.listdir(sem_dir):
        if not fn.endswith((".yml", ".yaml")):
            continue
        path = os.path.join(sem_dir, fn)
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
        try:
            data = yaml.safe_load(text) or {}
        except Exception as e:  # pragma: no cover - 방어용
            logger.error("Failed to parse semantic model yaml %s: %s", path, str(e))
            continue
        sms = data.get("semantic_models") or []
        lines = text.splitlines()
        for sm in sms:
            results.append((sm, path, lines))
    return results


def load_metrics_with_files(
    sem_dir: str,
) -> List[Tuple[Dict[str, Any], str, List[str]]]:
    """
    semantic_models 디렉토리의 각 파일에서 metrics 항목을 읽어,
    (metric_dict, file_path, file_lines) 튜플 리스트를 반환합니다.
    """
    results: List[Tuple[Dict[str, Any], str, List[str]]] = []

    if not os.path.isdir(sem_dir):
        return results

    for fn in os.listdir(sem_dir):
        if not fn.endswith((".yml", ".yaml")):
            continue
        path = os.path.join(sem_dir, fn)
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
        try:
            data = yaml.safe_load(text) or {}
        except Exception as e:  # pragma: no cover - 방어용
            logger.error("Failed to parse semantic model yaml %s: %s", path, str(e))
            continue
        mts = data.get("metrics") or []
        lines = text.splitlines()
        for mt in mts:
            results.append((mt, path, lines))
    return results
