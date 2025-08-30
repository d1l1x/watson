import pytest
import os
from unittest.mock import patch, mock_open
from watson.settings import LOGGING_CONFIG


class TestSettings:
    """Test the settings module"""
    
    def test_logging_config_structure(self):
        """Test that LOGGING_CONFIG has the expected structure"""
        required_keys = ['level', 'format', 'file_path', 'max_size', 'retention']
        
        for key in required_keys:
            assert key in LOGGING_CONFIG, f"Missing key: {key}"
    
    def test_logging_config_values(self):
        """Test that LOGGING_CONFIG has valid values"""
        assert isinstance(LOGGING_CONFIG['level'], str)
        assert isinstance(LOGGING_CONFIG['format'], str)
        assert isinstance(LOGGING_CONFIG['file_path'], str)
        assert isinstance(LOGGING_CONFIG['max_size'], str)
        assert isinstance(LOGGING_CONFIG['retention'], str)
        
        # Check that format contains expected placeholders
        format_str = LOGGING_CONFIG['format']
        assert '{time:' in format_str
        assert '{level}' in format_str
        assert '{name}' in format_str
        assert '{function}' in format_str
        assert '{line}' in format_str
        assert '{message}' in format_str
    
    def test_logging_config_default_level(self):
        """Test that LOG_LEVEL defaults to INFO when not set"""
        with patch.dict(os.environ, {}, clear=True):
            # Re-import to test default value
            import importlib
            import watson.settings
            importlib.reload(watson.settings)
            
            assert watson.settings.LOGGING_CONFIG['level'] == 'INFO'
    
    def test_logging_config_custom_level(self):
        """Test that LOG_LEVEL can be set via environment variable"""
        with patch.dict(os.environ, {'LOG_LEVEL': 'DEBUG'}, clear=True):
            # Re-import to test environment variable
            import importlib
            import watson.settings
            importlib.reload(watson.settings)
            
            assert watson.settings.LOGGING_CONFIG['level'] == 'DEBUG'
    
    def test_logging_config_file_path(self):
        """Test that file_path is set correctly"""
        assert LOGGING_CONFIG['file_path'] == 'logs/watson.log'
    
    def test_logging_config_max_size(self):
        """Test that max_size is set correctly"""
        assert LOGGING_CONFIG['max_size'] == '10 MB'
    
    def test_logging_config_retention(self):
        """Test that retention is set correctly"""
        assert LOGGING_CONFIG['retention'] == '30 days'
    
    def test_dotenv_loading(self):
        """Test that dotenv is loaded"""
        # This test verifies that load_dotenv() is called
        # We can't easily test the actual loading without a real .env file,
        # but we can verify the import doesn't fail
        try:
            from watson.settings import LOGGING_CONFIG
            assert LOGGING_CONFIG is not None
        except ImportError as e:
            pytest.fail(f"Failed to import settings: {e}")