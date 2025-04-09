import logging
import sys
from .config import LOG_LEVEL, LOG_FORMAT

def setup_logger(name):
    """
    Set up a logger with the given name.
    
    Args:
        name (str): The name of the logger
        
    Returns:
        logging.Logger: The configured logger
    """
    # Create logger
    logger = logging.getLogger(name)
    
    # Set level
    level = getattr(logging, LOG_LEVEL)
    logger.setLevel(level)
    
    # Create handlers
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    
    # Create formatters
    formatter = logging.Formatter(LOG_FORMAT)
    
    # Add formatters to handlers
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    
    return logger

# Global application logger
app_logger = setup_logger('retail_data_assistant')

def get_logger(name):
    """
    Get a logger with the given name.
    
    Args:
        name (str): The name of the logger
        
    Returns:
        logging.Logger: The configured logger
    """
    return setup_logger(f'retail_data_assistant.{name}') 