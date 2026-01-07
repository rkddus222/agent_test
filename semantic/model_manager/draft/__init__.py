"""
Draft generation module for semantic models.
Generates sources.yml and semantic_models files from DDL SQL.
"""

from semantic.model_manager.draft.draft_generator import generate_draft
from semantic.model_manager.utils.ddl_parser import parse_ddl

__all__ = ["generate_draft", "parse_ddl"]
