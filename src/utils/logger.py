import logging
import os
from datetime import datetime
from src.config.paths import LOG_PATH

def setup_logger(name='app'):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # 修改为DEBUG级别
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create and set file handler
    log_file = os.path.join(LOG_PATH, f'{name}.log')
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    
    # Create and set console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger
