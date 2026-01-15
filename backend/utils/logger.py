"""
간단한 로거 유틸리티
"""
import logging
import sys

def setup_logger(name: str) -> logging.Logger:
    """
    로거를 설정하고 반환합니다.
    
    Args:
        name: 로거 이름
        
    Returns:
        설정된 로거 인스턴스
    """
    logger = logging.getLogger(name)
    
    # 이미 핸들러가 있으면 중복 추가하지 않음
    if logger.handlers:
        return logger
    
    # smq_parser와 smq2sql_service 로거는 WARNING 레벨로 설정 (INFO 로그 제거)
    if name in ["smq_parser", "smq2sql_service"]:
        logger.setLevel(logging.WARNING)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
    
    # 포맷터
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    
    return logger


