import pytest
import sys
from unittest.mock import patch
from unittest.mock import Mock
from pathlib import Path
from watson.logger import setup_logger, get_logger

@pytest.fixture
def mock_logger():
    """Mock logger for testing"""
    with patch('watson.logger.logger') as mock_logger:
        yield mock_logger


class TestLogger:
    """Test the logger module"""
    
    @patch('watson.logger.logger')
    def test_setup_logger(self, mock_logger: Mock):
        """Test logger setup"""
        # Mock the logger methods
        mock_logger.remove.return_value = None
        mock_logger.add.return_value = None
        
        setup_logger()
        
        # Verify logger was configured
        mock_logger.remove.assert_called_once()
        assert mock_logger.add.call_count == 2  # Console and file handlers
    
    @patch('watson.logger.logger')
    def test_get_logger(self, mock_logger: Mock):
        """Test getting a logger instance"""
        mock_logger.bind.return_value = mock_logger
        
        result = get_logger("test_module")
        
        mock_logger.bind.assert_called_once_with(name="test_module")
        assert result == mock_logger
    
    @patch('watson.logger.logger')
    def test_get_logger_different_modules(self, mock_logger: Mock):
        """Test getting loggers for different modules"""
        mock_logger.bind.return_value = mock_logger
        
        logger1 = get_logger("module1")
        logger2 = get_logger("module2")
        
        assert mock_logger.bind.call_count == 2
        mock_logger.bind.assert_any_call(name="module1")
        mock_logger.bind.assert_any_call(name="module2")
    
    @patch('watson.logger.logger')
    @patch('pathlib.Path.mkdir')
    def test_setup_logger_creates_log_directory(self, mock_mkdir: Mock, mock_logger: Mock):
        """Test that setup_logger creates log directory if it doesn't exist"""
        mock_logger.remove.return_value = None
        mock_logger.add.return_value = None
        
        setup_logger()
        
        # Verify mkdir was called to create log directory
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
    
    @patch('watson.logger.logger')
    def test_setup_logger_console_handler(self, mock_logger: Mock):
        """Test console handler configuration"""
        mock_logger.remove.return_value = None
        mock_logger.add.return_value = None
        
        setup_logger()
        
        # Get the first call to add (console handler)
        console_call = mock_logger.add.call_args_list[0]
        
        # Verify console handler parameters
        assert console_call[0][0] == sys.stdout  # First argument should be sys.stdout
        assert 'format' in console_call[1]  # Should have format parameter
        assert 'level' in console_call[1]   # Should have level parameter
        assert console_call[1]['colorize'] == True  # Should be colorized
    
    @patch('watson.logger.logger')
    def test_setup_logger_file_handler(self, mock_logger: Mock):
        """Test file handler configuration"""
        mock_logger.remove.return_value = None
        mock_logger.add.return_value = None
        
        setup_logger()
        
        # Get the second call to add (file handler)
        file_call = mock_logger.add.call_args_list[1]
        
        # Verify file handler parameters
        assert isinstance(file_call[0][0], Path)  # First argument should be a Path
        assert 'format' in file_call[1]  # Should have format parameter
        assert 'level' in file_call[1]   # Should have level parameter
        assert 'rotation' in file_call[1]  # Should have rotation parameter
        assert 'retention' in file_call[1]  # Should have retention parameter
        assert file_call[1]['compression'] == "zip"  # Should use zip compression
