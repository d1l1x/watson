# loro_trader_core/loader.py
import importlib.util
import os
import sys
import inspect
from abc import ABC

from watson.logger import get_logger

logger = get_logger(__name__)

def load_strategy_from_path(file_path: str, class_name: str = 'Strategy'):
    """Load a strategy class from an external .py file."""
    logger.info(f"Loading strategy from {file_path}")
    module_name = os.path.splitext(os.path.basename(file_path))[0]

    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load {file_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    # Find the concrete strategy class
    strategy_class = find_concrete_strategy_class(module, class_name)
    if strategy_class is None:
        raise ValueError(f"No concrete strategy class found in {file_path}")
    
    return strategy_class()

def find_concrete_strategy_class(module, base_class_name: str):
    """Find a concrete (non-abstract) strategy class in the module."""
    for name, obj in inspect.getmembers(module):
        if (inspect.isclass(obj) and 
            hasattr(obj, '__bases__') and
            any(base.__name__ == base_class_name for base in obj.__mro__) and
            not inspect.isabstract(obj)):
            logger.info(f"Found concrete strategy class: {name}")
            return obj
    
    # If no concrete class found, look for any class that's not abstract
    for name, obj in inspect.getmembers(module):
        if (inspect.isclass(obj) and 
            hasattr(obj, '__bases__') and
            not inspect.isabstract(obj) and
            not issubclass(obj, ABC)):
            logger.info(f"Found non-abstract class: {name}")
            return obj
    
    return None