import os
import logging
from logging.handlers import TimedRotatingFileHandler
import datetime 

def setup_logging(name = 'dask_scheduler_logger',log_dir='logs'):
    """
    Set up a custom logger for Dask operations with console and file output.

    Args:
        log_dir (str): Directory to store log files. Defaults to 'logs'.

    Returns:
        logging.Logger: Configured logger object.
    """
    # get current date 
    now = datetime.datetime.now().strftime('%Y-%m-%d')

    # Create the log directory if it doesn't exist
    if not os.path.exists(os.path.join(log_dir,now)):
        os.makedirs(os.path.join(log_dir,now))

    # Create a logger object with a custom name
    logger = logging.getLogger(name)

    # Set the logging level to INFO
    logger.setLevel(logging.INFO)

    # Set up Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    
    # Create a formatter for console output
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    # Set up File Handler with Time Rotation
    file_handler = TimedRotatingFileHandler(
        filename=os.path.join(os.path.join(log_dir,now), f'{name}_log.log'),  # Log file path
        when='midnight',  # Rotate at midnight
        interval=1,  # Rotate every day
        backupCount=7,  # Keep logs for 7 days
        encoding='utf-8'  # Use UTF-8 encoding
    )
    file_handler.setLevel(logging.INFO)

    # Create a formatter for file output
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    # Add both handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger