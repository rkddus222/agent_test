"""
vendor 패키지 경로 설정
필요한 경우 여기에 vendor 패키지 경로를 추가할 수 있습니다.
"""
import sys
from pathlib import Path

# vendor 디렉토리가 있으면 경로에 추가
vendor_dir = Path(__file__).parent / 'vendor'
if vendor_dir.exists():
    sys.path.insert(0, str(vendor_dir))


