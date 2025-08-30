# loro_trader_core/loader.py
import importlib.util
import os
import sys

from watson.logger import get_logger

logger = get_logger(__name__)

def load_strategy_from_path(file_path: str, class_name: str):
    """Load a strategy class from an external .py file."""
    logger.info(f"Loading strategy from {file_path}")
    module_name = os.path.splitext(os.path.basename(file_path))[0]

    # spec = importlib.util.spec_from_file_location(module_name, file_path)
    # if spec is None or spec.loader is None:
    #     raise ImportError(f"Could not load module from {file_path}")
    
    # module = importlib.util.module_from_spec(spec)
    # sys.modules[module_name] = module
    # spec.loader.exec_module(module)

    # strategy_class = getattr(module, class_name)
    # return strategy_class()