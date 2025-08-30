import sys
import os
from pathlib import Path
from loguru import logger

from watson.settings import LOGGING_CONFIG


def setup_logger():
    """Setup logging configuration."""
    # Remove default handler
    logger.remove()
    
    # Add console handler
    logger.add(
        sys.stdout,
        format=LOGGING_CONFIG["format"],
        level=LOGGING_CONFIG["level"],
        colorize=True
    )
    
    # Add file handler
    log_file = Path(LOGGING_CONFIG["file_path"])
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    logger.add(
        log_file,
        format=LOGGING_CONFIG["format"],
        level=LOGGING_CONFIG["level"],
        rotation=LOGGING_CONFIG["max_size"],
        retention=LOGGING_CONFIG["retention"],
        compression="zip"
    )
    
def get_logger(name: str):
    """Get a logger instance for a module."""
    return logger.bind(name=name)


# Setup logging when module is imported
setup_logger() 