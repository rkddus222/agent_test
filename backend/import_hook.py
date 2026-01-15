"""
Import hook for mapping service.semantic -> backend.semantic
"""
import sys
import importlib.util
from importlib.machinery import ModuleSpec


class SemanticImportHook:
    """
    Import hook that maps 'service.semantic' imports to 'backend.semantic'
    """
    
    def find_spec(self, name, path, target=None):
        """
        If the module name starts with 'service.semantic', 
        replace it with 'backend.semantic'
        """
        if name.startswith('service.semantic'):
            # Replace 'service.semantic' with 'backend.semantic'
            new_name = name.replace('service.semantic', 'backend.semantic', 1)
            # Try to find the actual module
            try:
                spec = importlib.util.find_spec(new_name)
                if spec:
                    return spec
            except (ImportError, ValueError, AttributeError):
                pass
        return None


# 자동으로 sys.meta_path에 등록
if not any(isinstance(hook, SemanticImportHook) for hook in sys.meta_path):
    sys.meta_path.insert(0, SemanticImportHook())
