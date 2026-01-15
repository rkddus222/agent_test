"""
이름 변환 유틸리티 모듈
snake_case를 camelCase, PascalCase로 변환하는 함수들을 제공합니다.
"""


def snake_to_camel(snake_str: str) -> str:
    """
    snake_case를 camelCase로 변환합니다.
    
    예:
        card_aply_sn -> cardAplySn
        co_ymd_info_m -> coYmdInfoM
    """
    components = snake_str.split('_')
    return components[0] + ''.join(word.capitalize() for word in components[1:])


def snake_to_pascal(snake_str: str) -> str:
    """
    snake_case를 PascalCase로 변환합니다.
    
    예:
        card_aply_sn -> CardAplySn
        co_ymd_info_m -> CoYmdInfoM
    """
    components = snake_str.split('_')
    return ''.join(word.capitalize() for word in components)


def to_model_filename(table_name: str) -> str:
    """
    테이블명을 모델 파일명으로 변환합니다.
    
    예:
        co_ymd_info_m -> CoYmdInfoMModel.yml
        tr_frcs_clsf_c -> TrFrcsClsfCModel.yml
    """
    pascal_name = snake_to_pascal(table_name)
    return f"{pascal_name}Model.yml"


def to_model_name(table_name: str) -> str:
    """
    테이블명을 semantic model의 name으로 변환합니다.
    
    예:
        co_ymd_info_m -> coYmdInfoM
        tr_frcs_clsf_c -> trFrcsClsfC
    """
    return snake_to_camel(table_name)
