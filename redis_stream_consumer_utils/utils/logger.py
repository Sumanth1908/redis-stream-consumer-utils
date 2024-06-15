import logging
from pathlib import PurePath, Path


def setup_logger(logger_name, log_file_path=None):
    # Ensure the directory for the log file exists
    if log_file_path is not None:
        Path(PurePath(log_file_path).parent).mkdir(parents=True, exist_ok=True)

    # Set up the logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)  # Set the logging level to the most verbose

    handlers = []
    # Create file handler which logs even debug messages
    if log_file_path is not None:
        fh = logging.FileHandler(log_file_path)
        fh.setLevel(logging.DEBUG)
        handlers.append(fh)

    # Create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    handlers.append(ch)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Setting formatter and Add the handlers to the logger
    for handler in handlers:
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
