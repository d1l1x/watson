import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

LOGGING_CONFIG = {
    "level": os.getenv("LOG_LEVEL", "INFO"),
    "format": "{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
    "file_path": "logs/watson.log",
    "max_size": "10 MB",
    "retention": "30 days",
}